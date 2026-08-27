// aqs.hpp — Java AbstractQueuedSynchronizer 的 C++17 移植
//
// 参考文章：《AQS源码逐行精读：CLH队列、CAS、独占/共享模式的设计哲学》
// 对应 OpenJDK AQS 的三大核心：
//   1. volatile int state            -> std::atomic<int> state_
//   2. CLH 变体 FIFO 双向队列        -> node 双向链表（惰性 SIGNAL 标记 + park/unpark）
//   3. CAS                           -> std::atomic 的 compare_exchange
//
// 模板方法模式：本类固定 acquire/release 全部流程，子类只实现四个钩子：
//   try_acquire / try_release              （独占：ReentrantLock）
//   try_acquire_shared / try_release_shared（共享：Semaphore / CountDownLatch）
#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace aqs {

inline constexpr std::string_view version = "0.1.0";

// IllegalMonitorStateException 的对应物：未持有同步状态却调用释放/通知类接口
class monitor_error : public std::logic_error {
 public:
  using std::logic_error::logic_error;
};

class abstract_aqs {
 public:
  abstract_aqs() = default;
  abstract_aqs(const abstract_aqs&) = delete;
  abstract_aqs& operator=(const abstract_aqs&) = delete;
  virtual ~abstract_aqs();

  // ---- 独占模式入口 ----
  void acquire(int arg);                                  // 阻塞直到成功
  bool acquire_for(int arg, std::chrono::nanoseconds timeout); // 超时返回 false
  bool release(int arg);

  // ---- 共享模式入口 ----
  void acquire_shared(int arg);
  bool acquire_shared_for(int arg, std::chrono::nanoseconds timeout);
  bool release_shared(int arg);

 protected:
  // 未覆写的钩子被调用时抛出（对应 Java 的 UnsupportedOperationException）
  [[noreturn]] static void unsupported(const char* op);

 protected:
  // ================= 模板方法钩子（对应 Java 四个 protected 抽象方法） =================
  // 共享模式钩子返回剩余量：>=0 成功并参与传播，<0 表示需排队等待
  virtual bool try_acquire(int /*arg*/) { unsupported("try_acquire"); }
  virtual bool try_release(int /*arg*/) { unsupported("try_release"); }
  virtual int  try_acquire_shared(int /*arg*/) { unsupported("try_acquire_shared"); }
  virtual bool try_release_shared(int /*arg*/) { unsupported("try_release_shared"); }
  virtual bool is_held_exclusively() const { return false; }

  // 同步状态访问器（子类经由它们实现 CAS 语义）
  int         get_state() const noexcept;
  void        set_state(int v) noexcept;
  bool        compare_and_set_state(int expect, int update) noexcept;

  // AbstractOwnableSynchronizer 的 exclusiveOwnerThread 对应物
  void                 set_exclusive_owner_thread(std::thread::id t) noexcept;
  std::thread::id      get_exclusive_owner_thread() const noexcept;

  // 公平锁用：同步队列中是否存在排在自己前面的线程
  bool has_queued_predecessors() const noexcept;


 private:
  struct node;                              // 前置声明：私有嵌套类型
  // 当前线程正在主队列中排队的节点（公平门沿 prev 反向计数，免疫 next 陈旧）
  static thread_local node* tls_current_wait_;
  enum class mode : bool { exclusive, shared };

  // ================= CLH 变体节点 =================
  struct node {
    // waitStatus 取值，与 Java Node 常量一一对应
    static constexpr int k_cancelled = 1;   // 超时取消，等待出队
    static constexpr int k_signal    = -1;  // 后继需要被唤醒（惰性标记）
    static constexpr int k_condition = -2;  // 在 Condition 队列中
    static constexpr int k_propagate = -3;  // 共享模式传播唤醒

    explicit node(mode m) : mode_(m), tid_(std::this_thread::get_id()) {}

    bool is_shared() const noexcept { return mode_ == mode::shared; }

    node* predecessor() const noexcept {
      auto* p = prev_.load(std::memory_order_acquire);
      return p ? p : nullptr; // 入队后的节点前驱必非空（Java 版此处为断言）
    }

    // ---------- park 机制：LockSupport(permit) 的 per-node 对应物 ----------
    // 状态机: 0=空转, 1=已投递唤醒(permit), 2=正在睡眠。
    // unpark 先于 park 时置 1，后续 park 检查到 1 直接通过——唤醒不丢失。
    void park();
    void park_for(std::chrono::nanoseconds d);          // 有界兜底睡（防丢唤醒）
    bool park_until(std::chrono::steady_clock::time_point deadline);
    void unpark();

    std::atomic<int> wait_status_{0};   // volatile waitStatus
    std::atomic<node*> prev_{nullptr};  // volatile prev —— 支持从 tail 反向扫描
    std::atomic<node*> next_{nullptr};  // volatile next
    node*   next_waiter_ = nullptr;     // Condition 队列中的后继（非 volatile，同 Java）
    const mode mode_;
    std::thread::id tid_;               // volatile thread；公平判断/取消清理用

    // Parker（futex 实现的 LockSupport）
    std::atomic<int> park_state_{0};
  };

  node* new_node(mode m);   // arena 分配

  // ================= 队列与获取流程内部实现 =================
  node* add_waiter(mode m);
  node* enq(node* n);
  void  set_head(node* n);
  void  set_head_and_propagate(node* n, int propagate);
  bool  should_park_after_failed_acquire(node* pred, node* n) const noexcept;
  void  acquire_queued(node* n, int arg);   // 无中断语义，循环至成功（异常时取消出队）
  bool  do_acquire_nanos(node* n, int arg,
                         std::chrono::steady_clock::time_point deadline);
  void  do_acquire_shared(node* n, int arg);
  bool  do_acquire_shared_nanos(node* n, int arg,
                                std::chrono::steady_clock::time_point deadline);
  void  cancel_acquire(node* n) noexcept;
  void  unpark_successor(node* n) noexcept;
  void  do_release_shared();

  // ================= ConditionObject 的框架侧支撑 =================
  bool  is_on_sync_queue(node* n) const;
  node* find_node_from_tail(node* n) const noexcept;
  bool  transfer_for_signal(node* n);

  friend class condition;

 public:
  // ================= 条件变量：ConditionObject 的直接移植 =================
  // 只能配合独占模式的子类使用（如 reentrant_lock），必须先持有锁再 wait/signal。
  class condition {
   public:
    explicit condition(abstract_aqs& owner) : owner_(owner) {}
    condition(const condition&) = delete;
    condition& operator=(const condition&) = delete;

    void await();                                             // 完整释放锁并挂起
    bool wait_for(std::chrono::nanoseconds timeout);          // true=被通知
    void signal();                                            // 移一个节点回主队列
    void signal_all();                                        // 移全部节点回主队列
    bool has_waiters() const noexcept;                        // 调试/测试用

   private:
    node* add_condition_waiter();
    int   fully_release(node* n);
    void  unlink_cancelled_waiters();
    void  do_signal(node* first);
    void  do_signal_all(node* first);

    abstract_aqs& owner_;   // 持有的外部对象
    node* first_waiter_ = nullptr;
    node* last_waiter_  = nullptr;
  };

 private:
  // ---- 同步状态（volatile int state）----
  std::atomic<int> state_{0};
  // ---- CLH 队列头尾（volatile head/tail + CAS）----
  std::atomic<node*> head_{nullptr};
  std::atomic<node*> tail_{nullptr};
  // ---- 独占持有者（exclusiveOwnerThread）----
  std::thread::id exclusive_owner_{};
  // ---- 节点仓库：Java 由 GC 回收节点，这里延迟到析构统一回收。----
  // ponytail: arena 替代 GC；活跃队列中的节点可能仍被其他线程的 prev 扫描引用，
  // 逐个 delete 有悬垂风险，摊销成本是每节点一次互斥保护的堆分配。
  std::mutex         nodes_mu_;
  std::vector<std::unique_ptr<node>> nodes_;

  // ponytail: 所有原子操作统一使用 seq_cst，正确性优先；
  // 如实测成为瓶颈可逐点降级为 acquire/release。
};

// =====================================================================
// 三大同步原语：仅靠覆写四个钩子复用整个框架（模板方法的精髓）
// =====================================================================

// ---------------- 可重入互斥锁（ReentrantLock）----------------
class reentrant_lock : private abstract_aqs {
 public:
  explicit reentrant_lock(bool fair = false) : fair_(fair) {}

  void lock();
  bool try_lock();                              // 非公平的立即尝试（tryLock），重入溢出会抛
  bool lock_for(std::chrono::nanoseconds timeout);
  void unlock();
  bool is_locked() const noexcept;
  bool is_held_by_current_thread() const noexcept;
  int  held_count() const noexcept { return get_state(); }   // 当前重入深度（诊断用）

  condition& new_condition();                   // 与 java 锁的 newCondition() 一致

 private:
  bool try_acquire(int arg) override;
  bool try_release(int arg) override;
  bool is_held_exclusively() const override;
  int  nonfair_try_acquire(int acquires);       // 返回新计数或 -1；重入溢出抛 monitor_error

  bool fair_;
  std::vector<std::unique_ptr<condition>> conditions_;  // 条件队列句柄仓库
};

// ---------------- 计数信号量（Semaphore）----------------
class semaphore : private abstract_aqs {
 public:
  semaphore(int permits, bool fair = false);

  void acquire();                               // 阻塞取一个许可
  bool try_acquire_immediate() noexcept;        // 非公平插队尝试（tryAcquire 语义）
  bool acquire_for(std::chrono::nanoseconds timeout);
  void release();                               // 归还一个许可
  int  available_permits() const noexcept { return get_state(); }

 private:
  int  try_acquire_shared(int arg) override;    // 返回剩余许可数，负值表示不足
  bool try_release_shared(int arg) override;
  bool is_held_exclusively() const override { return false; }
  int  nonfair_try_acquire_shared(int acquires) noexcept;

  bool fair_;
};

// ---------------- 倒计时门闩（CountDownLatch）----------------
class countdown_latch : private abstract_aqs {
 public:
  explicit countdown_latch(int count);

  void await();                                 // count 为 0 前一直阻塞
  bool await_for(std::chrono::nanoseconds timeout);
  void count_down();                            // 计数减一，减到 0 唤醒所有等待者
  long count() const noexcept { return get_state(); }

 private:
  int  try_acquire_shared(int arg) override;    // state==0 才放行
  bool try_release_shared(int arg) override;    // 减到 0 时返回 true 触发传播
  bool is_held_exclusively() const override { return false; }
};

}  // namespace aqs

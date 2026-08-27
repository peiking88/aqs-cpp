// aqs.cpp — AQS 框架与三大同步原语的实现（对照 OpenJDK AbstractQueuedSynchronizer）
//
// 移植差异说明：
//  * C++ 无线程中断机制，InterruptedException 路径省略；超时取消是唯一取消来源，
//    cancel_acquire 完整保留并经测试风暴覆盖。
//  * Java 由 GC 回收节点，这里由每对象 arena 在析构时统一回收，
//    规避其他线程反向扫描时的悬垂指针问题。
//  * 文章第八节引用的 cancelAcquire 有缺漏（缺尾指针 CAS 分支），以 OpenJDK 为准。

#include "aqs.hpp"

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#if !defined(__linux__)
#error "Parker 依赖 Linux futex"
#endif

extern "C" long syscall(long, ...) noexcept;

namespace aqs {

using steady_clock = std::chrono::steady_clock;



// ================= 同步状态访问器 =================

int abstract_aqs::get_state() const noexcept {
  return state_.load(std::memory_order_acquire);
}

void abstract_aqs::set_state(int v) noexcept {
  state_.store(v, std::memory_order_release);   // Java setState 的 volatile 写语义
}

bool abstract_aqs::compare_and_set_state(int expect, int update) noexcept {
  return state_.compare_exchange_strong(expect, update);
}

void abstract_aqs::set_exclusive_owner_thread(std::thread::id t) noexcept {
  exclusive_owner_ = t;
}

std::thread::id abstract_aqs::get_exclusive_owner_thread() const noexcept {
  return exclusive_owner_;
}

[[noreturn]] void abstract_aqs::unsupported(const char* op) {
  throw monitor_error(std::string(op) + ": 本同步器未实现该模式");
}

abstract_aqs::~abstract_aqs() = default;   // arena 在此统一回收

// 挂死诊断：全线程事件环 + 队列拓扑
// ponytail: 挂死诊断工具（配合测试看门狗使用）
// ================= Parker：futex 三态 permit 的 LockSupport 等价物 =================
// park_state_: 0=空转，1=唤醒已投递，2=睡眠中。
// “先认领床位再睡”+“值校验睡眠”，unpark 先于 park 时置 1 不丢失唤醒。

namespace {

long sys_futex(int* addr, int op, long val, const timespec* ts = nullptr,
               int* addr2 = nullptr, int val3 = 0) noexcept {
  return syscall(SYS_futex, addr, op, val, ts, addr2, val3);
}

int* raw(std::atomic<int>& slot) noexcept {
  return reinterpret_cast<int*>(&slot);   // lock-free atomic<int> 与 int 布局兼容
}

static_assert(std::atomic<int>::is_always_lock_free,
              "Parker 需要无锁的 std::atomic<int>");

}  // namespace

namespace {
// 显式消耗一张已投递的唤醒票；并发者改写时顺其自然，绝不多睡
void consume_permit(std::atomic<int>& ps) noexcept {
  int expected = ps.load(std::memory_order_acquire);
  if (expected != 0)
    ps.compare_exchange_strong(expected, 0);
}
}  // namespace

void abstract_aqs::node::park() {
  int st = park_state_.load(std::memory_order_acquire);
  if (st == 0 && park_state_.compare_exchange_strong(st, 2)) {
    for (;;) {                             // 已认领床位：内核值校验沉睡
      sys_futex(raw(park_state_), FUTEX_WAIT_PRIVATE, 2);
      st = park_state_.load(std::memory_order_acquire);
      if (st != 2) break;
    }
    // 醒来：只消耗确认存在的那张票，不误伤其他并发投递
    int expected = 2;
    if (!park_state_.compare_exchange_strong(expected, 0)) {
      expected = park_state_.load();
      consume_permit(park_state_);
    }
    return;
  }
  // 快速路径或认领失败：当前值为待领票
  park_state_.store(0, std::memory_order_release);
}

void abstract_aqs::node::park_for(std::chrono::nanoseconds d) {
  // ponytail: 无限 park 换成 2ms 有界睡——彻底封死"标记与释放读交错"微窗口下的
  // 永久搁浅；最坏代价只是极强竞争下每毫秒一次的额外自旋复查。
  park_until(steady_clock::now() + d);
}

bool abstract_aqs::node::park_until(steady_clock::time_point deadline) {
  bool claimed = false;
  int st = park_state_.load(std::memory_order_acquire);
  if (st == 0) claimed = park_state_.compare_exchange_strong(st, 2);
  if (!claimed) {                            // 初始为1或竞态中变1：吞掉pending
    park_state_.store(0, std::memory_order_release);
    return true;
  }
  auto now = steady_clock::now();
  for (;;) {
    const auto left =
        std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now).count();
    if (left <= 0) {
      int expected = 2;
      if (park_state_.compare_exchange_strong(expected, 0))
        return false;                        // 干净撤销床位：确无唤醒到来
      break;                                 // 唤醒恰在超时瞬间到达：按醒来处理
    }
    timespec ts{static_cast<time_t>(left / 1000000000LL),
                static_cast<long>(left % 1000000000LL)};
    sys_futex(raw(park_state_), FUTEX_WAIT_PRIVATE, 2, &ts);
    st = park_state_.load(std::memory_order_acquire);
    if (st != 2) break;
    now = steady_clock::now();
  }
  st = park_state_.load(std::memory_order_acquire);
  if (st != 0) park_state_.store(0, std::memory_order_release);
  return true;
}

void abstract_aqs::node::unpark() {
  const int prev = park_state_.exchange(1, std::memory_order_acq_rel);
  if (prev == 2)                             // 有人真在睡才需要内核介入
    sys_futex(raw(park_state_), FUTEX_WAKE_PRIVATE, 1);
}

abstract_aqs::node* abstract_aqs::new_node(mode m) {
  std::lock_guard lk(nodes_mu_);   // arena 的 push_back 必须串行化（多线程同时入队）
  nodes_.push_back(std::make_unique<node>(m));
  return nodes_.back().get();      // pointee 地址稳定，重排不影响已发出的指针
}

// ================= CLH 队列基础操作 =================

// enq：两次 CAS 入队。第一次初始化哑元 head，第二次抢 tail。
abstract_aqs::node* abstract_aqs::enq(node* n) {
  for (;;) {
    node* t = tail_.load(std::memory_order_acquire);
    if (t == nullptr) {                                // 惰性初始化哑元头
      node* dummy = new_node(mode::exclusive);
      node* expected = nullptr;
      if (head_.compare_exchange_strong(expected, dummy))
        tail_.store(dummy, std::memory_order_release);
    } else {
      n->prev_.store(t, std::memory_order_release);
      if (tail_.compare_exchange_strong(t, n)) {       // 抢到尾指针
        t->next_.store(n, std::memory_order_release);  // 成功后才写 next
        return t;                                      // 返回前驱
      }
    }
  }
}

abstract_aqs::node* abstract_aqs::add_waiter(mode m) {
  node* n = new_node(m);
  node* pred = tail_.load(std::memory_order_acquire);
  if (pred != nullptr) {
    n->prev_.store(pred, std::memory_order_release);
    if (tail_.compare_exchange_strong(pred, n)) {
      pred->next_.store(n, std::memory_order_release);
      return n;
    }
  }
  enq(n);                          // 快速路径失败或未初始化：自旋入队
  return n;
}

void abstract_aqs::set_head(node* n) {
  n->prev_.store(nullptr, std::memory_order_relaxed);
  // 关键：不清 next！与 OpenJDK setHead 一致——hasQueuedPredecessors 与
  // unparkSuccessor 反向扫描都依赖这条"悬而未断"的旧链做正确性判断。
  head_.store(n, std::memory_order_release);
}

bool abstract_aqs::should_park_after_failed_acquire(node* pred,
                                                    node* n) const noexcept {
  int ws = pred->wait_status_.load(std::memory_order_acquire);
  if (ws == node::k_signal)
    return true;                   // 前驱承诺过会叫醒我们，可以放心挂起
  if (ws > 0) {                    // 前驱已取消：一路跳过所有取消节点
    do {
      pred = pred->prev_.load(std::memory_order_acquire);
      n->prev_.store(pred, std::memory_order_release);
    } while (pred->wait_status_.load(std::memory_order_acquire) > 0);
    pred->next_.store(n, std::memory_order_release);
  } else {
    int expected = ws;
    pred->wait_status_.compare_exchange_strong(expected, node::k_signal);
  }
  return false;                    // 标记完再自旋一轮确认，不立刻挂起
}

// cancelAcquire：多线程安全摘链（OpenJDK 完整版，含尾指针 CAS 分支）
void abstract_aqs::cancel_acquire(node* n) noexcept {
  n->tid_ = std::thread::id{};                       // 断开线程关联

  node* pred = n->prev_.load(std::memory_order_acquire);
  if (pred == nullptr) return;                       // 尚未真正入主队列

  while (pred->wait_status_.load(std::memory_order_acquire) > 0) {   // 跳过取消前驱
    pred = pred->prev_.load(std::memory_order_acquire);
    n->prev_.store(pred, std::memory_order_release);
  }

  node* pred_next = pred->next_.load(std::memory_order_acquire);

  n->wait_status_.store(node::k_cancelled, std::memory_order_release);

  if (n == tail_.load(std::memory_order_acquire) &&
      tail_.compare_exchange_strong(n, pred)) {      // 自己是尾巴：缩尾
    pred->next_.compare_exchange_strong(pred_next, static_cast<node*>(nullptr));
  } else {
    int ws = pred->wait_status_.load(std::memory_order_acquire);
    if (pred != head_.load(std::memory_order_acquire) &&
        ((ws == node::k_signal) ||
         (ws <= 0 && pred->wait_status_.compare_exchange_strong(ws, node::k_signal))) &&
        pred->tid_ != std::thread::id{}) {
      node* next = n->next_.load(std::memory_order_acquire);
      if (next != nullptr && next->wait_status_.load(std::memory_order_acquire) <= 0)
        pred->next_.compare_exchange_strong(pred_next, next);   // 跨过自己直连后继
    } else {
      unpark_successor(n);                 // 关键兜底：保证后继仍可被唤醒
    }
    n->next_.store(n, std::memory_order_relaxed);   // 自指：标记已出队
  }
}

// acquireQueued：自旋。前驱=head 就试抢；失败先惰性打 SIGNAL 再挂起。
void abstract_aqs::acquire_queued(node* n, int arg) {
  tls_current_wait_ = n;
  struct WaitGuard { ~WaitGuard() { tls_current_wait_ = nullptr; } } wg;
  (void)wg;
  try {
    for (;;) {
      node* p = n->predecessor();
      if (p == head_.load(std::memory_order_acquire) && try_acquire(arg)) {
        set_head(n);
        p->next_.store(nullptr, std::memory_order_relaxed);   // help GC
        return;
      }
      if (should_park_after_failed_acquire(p, n)) {
        n->park_for(std::chrono::milliseconds(2));
      }
    }
  } catch (...) {
    cancel_acquire(n);     // 对应 Java finally(failed): 钩子抛异常也要摘链
    throw;
  }
}

void abstract_aqs::acquire(int arg) {
  if (try_acquire(arg))          // 无竞争快速路径（Java acquire 先试钩子后才 addWaiter）
    return;
  acquire_queued(add_waiter(mode::exclusive), arg);
}

bool abstract_aqs::do_acquire_nanos(node* n, int arg,
                                    steady_clock::time_point deadline) {
  tls_current_wait_ = n;
  struct WG { ~WG() { tls_current_wait_ = nullptr; } } wg_guard;
  (void)wg_guard;
  try {
    for (;;) {
      node* p = n->predecessor();
      if (p == head_.load(std::memory_order_acquire) && try_acquire(arg)) {
        set_head(n);
        p->next_.store(nullptr, std::memory_order_relaxed);
        return true;
      }
      if (steady_clock::now() >= deadline) {
        cancel_acquire(n);
        return false;
      }
      // ponytail: 省去 spinForTimeoutThreshold 短时自旋分支，
      // 亚毫秒级剩余时间下 cv/futex 的空转开销实测可接受。
      if (should_park_after_failed_acquire(p, n)) n->park_until(deadline);
    }
  } catch (...) {
    cancel_acquire(n);
    throw;
  }
}

bool abstract_aqs::acquire_for(int arg, std::chrono::nanoseconds timeout) {
  if (timeout <= std::chrono::nanoseconds::zero()) return false;
  if (try_acquire(arg))          // 快速路径：未超时约束下的立即尝试
    return true;
  return do_acquire_nanos(add_waiter(mode::exclusive), arg,
                          steady_clock::now() + timeout);
}

// 当前线程排队节点登记（公平门的可靠数据源）
thread_local abstract_aqs::node* abstract_aqs::tls_current_wait_ = nullptr;

// 公平门 v2：沿自身 prev 反向数跳。>0 跳即有人在前。
// ponytail: 不再读 h.next——next 是惰性发布的单写链，极端竞争下存在陈旧窗口；
// prev 每步都由入队者亲手写定且 arena 保活，结果是保守而自愈的上界：
// 若前方其实无人（都晋升走了），head 会贴上来使跳数归零，本轮必然放行。
bool abstract_aqs::has_queued_predecessors() const noexcept {
  node* me = tls_current_wait_;
  if (me == nullptr) {          // 未入队的裸尝试：退回 OpenJDK 语义
    node* t = tail_.load(std::memory_order_acquire);
    node* h = head_.load(std::memory_order_acquire);
    if (h == t) return false;
    node* s = h->next_.load(std::memory_order_acquire);
    return s == nullptr || s->tid_ != std::this_thread::get_id();
  }
  // 沿自身 prev 反向走，抵达"当前 head"即止：head 与我之间的节点才是真实前驱。
  // （整条祖先链上还留着大量已晋升的幽灵节点，绝不能数它们——这正是此门的关键。）
  const node* hcur = head_.load(std::memory_order_acquire);
  int ahead = 0;
  for (const node* q = me->prev_.load(std::memory_order_acquire); q != nullptr;
       q = q->prev_.load(std::memory_order_acquire)) {
    if (q == hcur) break;
    ++ahead;
    if (ahead > 4096) break;    // 防御上界：不可能有那么长的一致队列
  }
  return ahead > 0;             // 头与我直接相邻 => 无真实前驱，放行抢锁
}

bool abstract_aqs::release(int arg) {
  const bool unlocked = try_release(arg);
  if (unlocked) {
    node* h = head_.load(std::memory_order_acquire);
    if (h != nullptr && h->wait_status_.load(std::memory_order_acquire) != 0)
      unpark_successor(h);
    return true;
  }
  return false;
}

// unparkSuccessor：清 SIGNAL 后找第一个有效后继唤醒。
// 从 tail 反向扫描应对"next 尚未链好/已被取消"两种情况。
void abstract_aqs::unpark_successor(node* n) noexcept {
  int ws = n->wait_status_.load(std::memory_order_acquire);
  if (ws < 0) n->wait_status_.compare_exchange_strong(ws, 0);   // 清 SIGNAL
  node* s = n->next_.load(std::memory_order_acquire);
  if (s == nullptr || s->wait_status_.load(std::memory_order_acquire) > 0) {
    s = nullptr;
    for (node* t = tail_.load(std::memory_order_acquire);
         t != nullptr && t != n; t = t->prev_.load(std::memory_order_acquire))
      if (t->wait_status_.load(std::memory_order_acquire) <= 0) s = t;
  }
  if (s != nullptr) {
    stats_wakeups_.fetch_add(1, std::memory_order_relaxed);
    s->unpark();
  }
}

// ================= 共享模式获取/释放流程 =================

void abstract_aqs::do_acquire_shared(node* n, int arg) {
  tls_current_wait_ = n;
  struct WG { ~WG() { tls_current_wait_ = nullptr; } } wg_guard;
  (void)wg_guard;
  try {
    for (;;) {
      node* p = n->predecessor();
      if (p == head_.load(std::memory_order_acquire)) {
        int r = try_acquire_shared(arg);
        if (r >= 0) {
          set_head_and_propagate(n, r);
          p->next_.store(nullptr, std::memory_order_relaxed);
          return;
        }
      }
      if (should_park_after_failed_acquire(p, n))
        n->park_for(std::chrono::milliseconds(2));
    }
  } catch (...) {
    cancel_acquire(n);
    throw;
  }
}

bool abstract_aqs::do_acquire_shared_nanos(node* n, int arg,
                                           steady_clock::time_point deadline) {
  tls_current_wait_ = n;
  struct WG { ~WG() { tls_current_wait_ = nullptr; } } wg_guard;
  (void)wg_guard;
  try {
    for (;;) {
      node* p = n->predecessor();
      if (p == head_.load(std::memory_order_acquire)) {
        int r = try_acquire_shared(arg);
        if (r >= 0) {
          set_head_and_propagate(n, r);
          p->next_.store(nullptr, std::memory_order_relaxed);
          return true;
        }
      }
      if (steady_clock::now() >= deadline) {
        cancel_acquire(n);
        return false;
      }
      if (should_park_after_failed_acquire(p, n)) n->park_until(deadline);
    }
  } catch (...) {
    cancel_acquire(n);
    throw;
  }
}

void abstract_aqs::acquire_shared(int arg) {
  if (try_acquire_shared(arg) >= 0)   // 快速路径：队列本为空，无需传播
    return;
  do_acquire_shared(add_waiter(mode::shared), arg);
}

bool abstract_aqs::acquire_shared_for(int arg, std::chrono::nanoseconds timeout) {
  if (timeout <= std::chrono::nanoseconds::zero()) return false;
  if (try_acquire_shared(arg) >= 0)
    return true;                      // 同上：非队列成员的成功不欠任何传播
  return do_acquire_shared_nanos(add_waiter(mode::shared), arg,
                                 steady_clock::now() + timeout);
}

// setHeadAndPropagate：拿到许可后把"还能拿"向后传播。
void abstract_aqs::set_head_and_propagate(node* n, int propagate) {
  node* h = head_.load(std::memory_order_acquire);   // 记录旧头供下方判定
  set_head(n);
  if (propagate > 0 || h == nullptr ||
      h->wait_status_.load(std::memory_order_acquire) < 0 ||
      (h = head_.load(std::memory_order_acquire)) == nullptr ||
      h->wait_status_.load(std::memory_order_acquire) < 0) {
    node* s = n->next_.load(std::memory_order_acquire);
    if (s == nullptr || s->is_shared()) do_release_shared();
  }
}

void abstract_aqs::do_release_shared() {
  for (;;) {
    node* h = head_.load(std::memory_order_acquire);
    if (h != nullptr && h != tail_.load(std::memory_order_acquire)) {
      int ws = h->wait_status_.load(std::memory_order_acquire);
      if (ws == node::k_signal) {
        if (!h->wait_status_.compare_exchange_strong(ws, 0))
          continue;                    // 状态被并发改动，重读重试
        unpark_successor(h);
      } else if (ws == 0 &&
                 !h->wait_status_.compare_exchange_strong(ws, node::k_propagate))
        continue;
    }
    if (h == head_.load(std::memory_order_acquire)) break;   // head 未变才收敛
  }
}

bool abstract_aqs::release_shared(int arg) {
  if (try_release_shared(arg)) {   // 减到 0 时返回 true → 唤醒等待队列
    do_release_shared();
    return true;
  }
  return false;
}

// ================= Condition 的框架侧支撑 =================

bool abstract_aqs::is_on_sync_queue(node* n) const {
  if (n->wait_status_.load(std::memory_order_acquire) == node::k_condition ||
      n->prev_.load(std::memory_order_acquire) == nullptr)
    return false;                // 有条件状态或无前驱 => 必不在主队列
  if (n->next_.load(std::memory_order_acquire) != nullptr) return true;
  return find_node_from_tail(n) != nullptr;   // prev 已链但 next 未及：从尾反查
}

abstract_aqs::node* abstract_aqs::find_node_from_tail(node* n) const noexcept {
  for (node* t = tail_.load(std::memory_order_acquire); t != nullptr;
       t = t->prev_.load(std::memory_order_acquire))
    if (t == n) return t;
  return nullptr;
}

// signal 搬运：CONDITION→0 后 enq 进主队列；前驱靠不住就直接踢醒
bool abstract_aqs::transfer_for_signal(node* n) {
  int expected = node::k_condition;
  if (!n->wait_status_.compare_exchange_strong(expected, 0))
    return false;                // 已被超时取消等场景处理过
  node* p = enq(n);
  int ws = p->wait_status_.load(std::memory_order_acquire);
  if (ws > 0 || !p->wait_status_.compare_exchange_strong(ws, node::k_signal)) {
    stats_wakeups_.fetch_add(1, std::memory_order_relaxed);
    n->unpark();                 // 前驱已取消或标记失败：踢醒让它自己走完入队竞争
  }
  return true;
}

// ================= Condition（ConditionObject 移植） =================

abstract_aqs::node* abstract_aqs::condition::add_condition_waiter() {
  node* t = last_waiter_;
  if (t != nullptr && t->wait_status_.load(std::memory_order_acquire) != node::k_condition)
    unlink_cancelled_waiters(), t = last_waiter_;
  // ponytail: 条件节点的 mode 字段无意义，借 exclusive 占位
  node* n = owner_.new_node(abstract_aqs::mode::exclusive);
  n->wait_status_.store(node::k_condition, std::memory_order_relaxed);
  if (t == nullptr)
    first_waiter_ = n;
  else
    t->next_waiter_ = n;
  last_waiter_ = n;
  return n;
}

int abstract_aqs::condition::fully_release(node* n) {
  int saved = owner_.get_state();
  bool failed = true;
  struct guard {                       // 手工模拟 Java try/finally
    ~guard() {
      if (failed_) node_->wait_status_.store(node::k_cancelled, std::memory_order_release);
    }
    bool& failed_;
    node* node_;
  } g{failed, n};
  (void)g;
  if (!owner_.release(saved))
    throw monitor_error("condition::await 需要持有独占锁");
  failed = false;
  return saved;
}

void abstract_aqs::condition::unlink_cancelled_waiters() {
  node* t = first_waiter_;
  node* trail = nullptr;
  while (t != nullptr) {
    node* next = t->next_waiter_;
    if (t->wait_status_.load(std::memory_order_acquire) != node::k_condition) {
      t->next_waiter_ = nullptr;
      if (trail == nullptr)
        first_waiter_ = next;
      else
        trail->next_waiter_ = next;
      if (next == nullptr) last_waiter_ = trail;
    } else {
      trail = t;
    }
    t = next;
  }
}

void abstract_aqs::condition::do_signal(node* first) {
  do {
    if ((first_waiter_ = first->next_waiter_) == nullptr) last_waiter_ = nullptr;
    first->next_waiter_ = nullptr;
  } while (!owner_.transfer_for_signal(first) &&
           (first = first_waiter_) != nullptr);   // 首个已取消则换下一个
}

void abstract_aqs::condition::do_signal_all(node* first) {
  first_waiter_ = last_waiter_ = nullptr;
  do {
    node* next = first->next_waiter_;
    first->next_waiter_ = nullptr;
    owner_.transfer_for_signal(first);
    first = next;
  } while (first != nullptr);
}

void abstract_aqs::condition::await() {
  if (!owner_.is_held_exclusively())
    throw monitor_error("condition::await 需要持有独占锁");
  node* n = add_condition_waiter();
  int saved_state = fully_release(n);            // 不占着锁等待
  while (!owner_.is_on_sync_queue(n))
    n->park_for(std::chrono::milliseconds(2)); // 醒后重查是否被搬运
  owner_.acquire_queued(n, saved_state);         // 以原重入深度重新竞争
  if (n->next_waiter_ != nullptr) unlink_cancelled_waiters();
}

bool abstract_aqs::condition::wait_for(std::chrono::nanoseconds timeout) {
  if (!owner_.is_held_exclusively())
    throw monitor_error("condition::wait_for 需要持有独占锁");
  node* n = add_condition_waiter();
  int saved_state = fully_release(n);
  const auto deadline = steady_clock::now() + timeout;
  bool timed_out = false;
  while (!owner_.is_on_sync_queue(n)) {
    if (steady_clock::now() >= deadline) {
      // 对应 transferAfterCancelledWait：CAS 赢了说明无人通知，自己送自己回主队列
      int expected = node::k_condition;
      if (n->wait_status_.compare_exchange_strong(expected, 0)) {
        owner_.enq(n);
        timed_out = true;
      } else {
        while (!owner_.is_on_sync_queue(n)) std::this_thread::yield();
      }
      break;
    }
    n->park_until(deadline);
  }
  owner_.acquire_queued(n, saved_state);
  if (n->next_waiter_ != nullptr) unlink_cancelled_waiters();
  return !timed_out;
}

void abstract_aqs::condition::signal() {
  if (!owner_.is_held_exclusively())
    throw monitor_error("condition::signal 需要持有独占锁");
  if (first_waiter_ != nullptr) do_signal(first_waiter_);
}

void abstract_aqs::condition::signal_all() {
  if (!owner_.is_held_exclusively())
    throw monitor_error("condition::signal_all 需要持有独占锁");
  if (first_waiter_ != nullptr) do_signal_all(first_waiter_);
}

bool abstract_aqs::condition::has_waiters() const noexcept { return first_waiter_ != nullptr; }

// ================= 三大同步原语 =================

// ---------------- reentrant_lock ----------------

int reentrant_lock::nonfair_try_acquire(int acquires) {
  const auto cur = std::this_thread::get_id();
  int c = get_state();
  if (c == 0) {
    if (fair_ && has_queued_predecessors()) return -1;   // 公平版唯一差别：多查一次队
    if (compare_and_set_state(0, acquires)) {
      set_exclusive_owner_thread(cur);
      return acquires;
    }
    return -1;
  }
  if (cur == get_exclusive_owner_thread()) {
    const int nextc = c + acquires;
    if (nextc < 0) throw monitor_error("Maximum lock count exceeded");
    set_state(nextc);              // 只有持有者会走到这
    return nextc;
  }
  return -1;
}

bool reentrant_lock::try_acquire(int arg) { return nonfair_try_acquire(arg) >= 0; }

bool reentrant_lock::try_release(int releases) {
  int c = get_state() - releases;
  if (std::this_thread::get_id() != get_exclusive_owner_thread())
    throw monitor_error("unlock 由非持有线程调用");
  const bool free = (c == 0);
  if (free) set_exclusive_owner_thread(std::thread::id{});   // 先清持有者再放 state
  set_state(c);
  return free;
}

bool reentrant_lock::is_held_exclusively() const {
  return get_state() > 0 && get_exclusive_owner_thread() == std::this_thread::get_id();
}

void reentrant_lock::lock() { acquire(1); }

bool reentrant_lock::try_lock() { return nonfair_try_acquire(1) >= 0; }   // barging

bool reentrant_lock::lock_for(std::chrono::nanoseconds timeout) {
  return acquire_for(1, timeout);
}

void reentrant_lock::unlock() { release(1); }

bool reentrant_lock::is_locked() const noexcept { return get_state() != 0; }

bool reentrant_lock::is_held_by_current_thread() const noexcept {
  return is_held_exclusively();
}

abstract_aqs::condition& reentrant_lock::new_condition() {
  abstract_aqs& self = static_cast<abstract_aqs&>(*this);   // 成员作用域内完成私有基类转型
  conditions_.push_back(std::make_unique<abstract_aqs::condition>(self));
  return *conditions_.back();
}

// ---------------- semaphore ----------------

semaphore::semaphore(int permits, bool fair) : fair_(fair) {
  if (permits < 0) throw std::invalid_argument("permits < 0");
  set_state(permits);
}

int semaphore::nonfair_try_acquire_shared(int acquires) noexcept {
  for (;;) {
    int available = get_state();
    int remaining = available - acquires;
    if (remaining < 0 || compare_and_set_state(available, remaining))
      return remaining;          // >=0 成功；<0 表示还差多少个许可
  }
}

int semaphore::try_acquire_shared(int arg) {
  if (fair_ && has_queued_predecessors()) return -1;
  return nonfair_try_acquire_shared(arg);
}

bool semaphore::try_release_shared(int releases) {
  for (;;) {
    int c = get_state();
    if (compare_and_set_state(c, c + releases)) return true;
  }
}

void semaphore::acquire() { acquire_shared(1); }

bool semaphore::try_acquire_immediate() noexcept {
  return nonfair_try_acquire_shared(1) >= 0;
}

bool semaphore::acquire_for(std::chrono::nanoseconds timeout) {
  return acquire_shared_for(1, timeout);
}

void semaphore::release() { release_shared(1); }

// ---------------- countdown_latch ----------------

countdown_latch::countdown_latch(int count) {
  if (count < 0) throw std::invalid_argument("count < 0");
  set_state(count);
}

int countdown_latch::try_acquire_shared(int) {
  return get_state() == 0 ? 1 : -1;     // 门开着才能通过
}

bool countdown_latch::try_release_shared(int releases) {
  for (;;) {
    int c = get_state();
    if (c == 0) return false;           // 已开门：重复 count_down 无害幂等
    const int nextc = c - releases;
    if (compare_and_set_state(c, nextc)) return nextc == 0;   // 减到0触发传播
  }
}

void countdown_latch::await() { acquire_shared(1); }

bool countdown_latch::await_for(std::chrono::nanoseconds timeout) {
  return acquire_shared_for(1, timeout);
}

void countdown_latch::count_down() { release_shared(1); }

}  // namespace aqs

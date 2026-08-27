// test_aqs.cpp — AQS 移植的并发压力测试（无 mock，全部真实线程）
//
// 覆盖点：
//   1. 独占/共享模式核心互斥不变量（临界区同时进入数恒为 1 / 许可上限恒成立）
//   2. 重入语义与误用抛错（对应 IllegalMonitorStateException）
//   3. 公平锁防饥饿
//   4. Condition await/signal 全流程 + 超时路径
//   5. 超时取消风暴：高强度覆盖 cancel_acquire 的摘链兜底逻辑
#include "aqs.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <deque>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
namespace sc = std::chrono;
using aqs::abstract_aqs;
using aqs::countdown_latch;
using aqs::monitor_error;
using aqs::reentrant_lock;
using aqs::semaphore;

static std::atomic<int> g_failures{0};

#define CHECK(cond)                                                            \
  do {                                                                         \
    if (!(cond)) {                                                             \
      g_failures.fetch_add(1);                                                 \
      std::printf("FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond);              \
    }                                                                          \
  } while (0)

#define CHECK_THROWS(Expr, ExType)                                             \
  do {                                                                         \
    bool thrown = false;                                                       \
    try {                                                                      \
      Expr;                                                                    \
    } catch (const ExType&) {                                                  \
      thrown = true;                                                           \
    } catch (...) {                                                            \
    }                                                                          \
    if (!thrown) {                                                             \
      g_failures.fetch_add(1);                                                 \
      std::printf("FAIL %s:%d  未抛出预期异常: %s\n", __FILE__, __LINE__,       \
                  #ExType);                                                    \
    }                                                                          \
  } while (0)

static int hardware_threads_capped() {
  int n = static_cast<int>(std::thread::hardware_concurrency());
  return n > 8 ? 8 : (n > 1 ? n : 4);          // 控制 CI 时长
}

// ---------------------------------------------------------------- 独占模式

static void test_reentrant_basic() {
  reentrant_lock lk;
  CHECK(!lk.is_locked());
  CHECK(!lk.is_held_by_current_thread());

  lk.lock();
  CHECK(lk.is_locked());
  CHECK(lk.is_held_by_current_thread());

  lk.lock();                                   // 重入一次
  CHECK(lk.held_count() == 2);                 // state==2

  std::thread other([&] {
    CHECK(!lk.try_lock());                     // 他人不能抢占重入中的锁
    CHECK_THROWS(lk.unlock(), monitor_error);  // 非持有者解锁必须抛错
  });
  other.join();

  lk.unlock();
  CHECK(lk.held_count() == 1);
  lk.unlock();
  CHECK(!lk.is_locked());
  CHECK_THROWS(lk.unlock(), monitor_error);    // 超额解锁同样抛错

  // try_lock 成功路径与 lock_for 成功路径
  CHECK(lk.try_lock());
  lk.unlock();
  CHECK(lk.lock_for(1s));
  lk.unlock();
}

static void mutex_stress(bool fair, const char* tag) {
  reentrant_lock lk(fair);
  int inside = 0;                              // 仅持锁线程触碰；本身即被测不变量载体
  std::atomic<int> violations{0};

  const int threads_n = hardware_threads_capped();
  const int rounds = fair ? 1500 : 4000;

  std::atomic<long> total{0};
  std::vector<std::thread> pool;
  std::vector<long> per_thread(static_cast<size_t>(threads_n), 0);

  for (int t = 0; t < threads_n; ++t) {
    pool.emplace_back([&, t] {
      for (int i = 0; i < rounds; ++i) {
        lk.lock();
        if (++inside != 1) violations.fetch_add(1);   // 同一时刻至多一人
        --inside;
        ++per_thread[static_cast<size_t>(t)];
        total.fetch_add(1);
        lk.unlock();
      }
    });
  }
  for (auto& th : pool) th.join();

  CHECK(violations.load() == 0);
  CHECK(total.load() == static_cast<long>(threads_n) * rounds);
  if (fair) {                                  // 公平性烟雾检查：无人被饿死
    long min_take = *std::min_element(per_thread.begin(), per_thread.end());
    const long expect_avg = total.load() / threads_n;
    CHECK(min_take * 2 >= expect_avg);
  }
  std::printf("  [%s] 线程=%d 每线程轮次=%d 违规=%d\n", tag, threads_n, rounds,
              violations.load());
}

// ---------------------------------------------------------------- 共享模式

static void test_semaphore_stress() {
  semaphore sem(3);
  std::atomic<int> inside{0};
  std::atomic<bool> violated{false};
  std::atomic<int> max_seen{0};

  const int workers = 12, rounds = 300;
  std::vector<std::thread> pool;
  for (int w = 0; w < workers; ++w) {
    pool.emplace_back([&] {
      for (int i = 0; i < rounds; ++i) {
        sem.acquire();
        const int now = inside.fetch_add(1) + 1;
        int prev_max = max_seen.load();
        while (now > prev_max && !max_seen.compare_exchange_weak(prev_max, now)) {}
        if (now > 3) violated.store(true);     // 许可上限不变量
        inside.fetch_sub(1);
        sem.release();
      }
    });
  }
  for (auto& th : pool) th.join();

  CHECK(!violated.load());
  CHECK(max_seen.load() <= 3);
  CHECK(sem.available_permits() == 3);         // 归还守恒
}

static void test_semaphore_try_and_timeout() {
  semaphore sem(2);
  CHECK(sem.try_acquire_immediate());
  CHECK(sem.try_acquire_immediate());
  CHECK(!sem.try_acquire_immediate());         // 许可用尽
  const auto t0 = sc::steady_clock::now();
  CHECK(!sem.acquire_for(80ms));               // 必须整段超时失败
  const auto waited =
      sc::duration_cast<sc::milliseconds>(sc::steady_clock::now() - t0).count();
  CHECK(waited >= 70);
  sem.release();
  CHECK(sem.acquire_for(50ms));                // 释放后立即可得（0→占用）
  sem.release();                               // 归还 → 1
  CHECK(sem.available_permits() == 1);
  sem.release();                               // 再归还最初两个中的另一个 → 2
  CHECK(sem.available_permits() == 2);
  CHECK_THROWS(semaphore(-1), std::invalid_argument);
}

static void test_countdown_latch() {
  countdown_latch latch(16);
  countdown_latch done(16);
  std::atomic<int> passed{0};

  std::vector<std::thread> pool;
  for (int i = 0; i < 16; ++i) {
    pool.emplace_back([&, i] {
      if (i % 2 == 0)
        latch.await();                         // 阻塞式等待
      else
        CHECK(latch.await_for(10s));           // 定时等待：门开前一直阻塞
      passed.fetch_add(1);
      done.count_down();
    });
  }
  CHECK(!countdown_latch(1).await_for(60ms));  // 门未开时定时等待应失败

  for (int i = 0; i < 16; ++i) latch.count_down();
  CHECK(done.await_for(10s));
  CHECK(passed.load() == 16);
  CHECK(latch.count() == 0);
  latch.count_down();                          // 幂等
  CHECK(latch.count() == 0);
  CHECK(latch.await_for(1ms));                 // 已开的门立即通过
  CHECK_THROWS(countdown_latch(-1), std::invalid_argument);
  for (auto& th : pool) th.join();
}

// ---------------------------------------------------------------- Condition

static void test_bounded_queue_with_condition() {
  constexpr int k_capacity = 8, k_per_producer = 1000, k_producers = 6,
                k_consumers = 6;
  reentrant_lock lk;
  auto& not_full = lk.new_condition();
  auto& not_empty = lk.new_condition();

  std::deque<int> q;
  std::atomic<long> sum_pushed{0}, sum_popped{0};
  std::atomic<int> produced{0}, consumed{0};

  std::vector<std::thread> pool;
  for (int p = 0; p < k_producers; ++p) {
    pool.emplace_back([&, p] {
      for (int i = 0; i < k_per_producer; ++i) {
        const int v = p * k_per_producer + i + 1;   // 全局唯一值便于总量校验
        lk.lock();
        while (static_cast<int>(q.size()) == k_capacity) not_full.await();
        q.push_back(v);
        not_empty.signal();
        lk.unlock();
        sum_pushed.fetch_add(v);
        produced.fetch_add(1);
      }
    });
  }
  for (int c = 0; c < k_consumers; ++c) {
    pool.emplace_back([&] {
      for (;;) {
        lk.lock();
        while (q.empty()) {
          if (consumed.load() >= produced.load()) {
            not_empty.signal_all();            // 唤醒同伴消费者一起退出
            lk.unlock();
            return;
          }
          not_empty.await();
        }
        const int v = q.front();
        q.pop_front();
        not_full.signal();
        lk.unlock();
        sum_popped.fetch_add(v);
        consumed.fetch_add(1);
      }
    });
  }
  for (auto& th : pool) th.join();

  CHECK(produced.load() == k_producers * k_per_producer);
  CHECK(consumed.load() == produced.load());
  CHECK(sum_pushed.load() == sum_popped.load());   // 无丢失无重复
  CHECK(q.empty());
  CHECK(!not_empty.has_waiters());
}

static void test_condition_timeout_and_errors() {
  reentrant_lock lk;
  auto& cond = lk.new_condition();

  CHECK_THROWS(cond.signal(), monitor_error);  // 不持锁通知 → IllegalMonitorState 同款
  CHECK_THROWS(cond.await(), monitor_error);

  // 场景一：signal_all 确定性唤醒（用轮询代替固定 sleep 消除竞态）
  {
    std::atomic<int> arrived{0};
    std::vector<std::thread> waiters;
    for (int i = 0; i < 2; ++i) {
      waiters.emplace_back([&] {
        lk.lock();
        arrived.fetch_add(1);                  // 仍持锁时登记已就位
        CHECK(cond.wait_for(3s));              // 应被 signal_all 唤醒而非超时
        arrived.fetch_sub(1);
        lk.unlock();
      });
    }
    const auto deadline = sc::steady_clock::now() + 2s;
    while (arrived.load() < 2 && sc::steady_clock::now() < deadline)
      std::this_thread::sleep_for(500us);
    lk.lock();                                 // 主线程此刻必然能拿到锁（等待者已全放锁）
    const bool saw_waiting = cond.has_waiters();
    cond.signal_all();
    lk.unlock();
    for (auto& th : waiters) th.join();
    CHECK(saw_waiting);                        // 轮询成功后条件队列应非空
  }

  // 场景二：无人通知，等待按期超时并恢复持有状态
  {
    std::atomic<long> waited_ms{0};
    std::thread waiter([&] {
      lk.lock();
      const auto t0 = sc::steady_clock::now();
      const bool got = cond.wait_for(80ms);
      waited_ms = sc::duration_cast<sc::milliseconds>(sc::steady_clock::now() - t0).count();
      CHECK(got == false);
      CHECK(lk.is_held_by_current_thread());
      CHECK(lk.held_count() == 1);             // 完整释放一层后只需抢回一层
      lk.unlock();
    });
    waiter.join();
    CHECK(waited_ms.load() >= 70);
    CHECK(!lk.is_locked());
  }
}

// ---------------------------------------------- 超时取消风暴（cancel_acquire）

static void test_timed_cancel_storm() {
  reentrant_lock lk;
  std::atomic<bool> stop_holder{false};
  int inside = 0;                              // 锁保护的互斥探针
  std::atomic<int> violations{0}, granted{0}, denied{0};

  std::thread holder([&] {                     // 周期持锁制造激烈竞争
    while (!stop_holder.load()) {
      lk.lock();
      ++inside;
      if (inside != 1) violations.fetch_add(1);
      std::this_thread::sleep_for(3ms);
      --inside;
      lk.unlock();
      std::this_thread::sleep_for(600us);      // 给竞争者留出排队窗口
    }
  });

  std::vector<std::thread> racers;
  for (int r = 0; r < 8; ++r) {
    racers.emplace_back([&, r] {
      // 各不相同且极短的期限：持锁者单轮 3ms，逼迫绝大多数尝试走超时取消路径
      const auto slice = std::chrono::microseconds(300 + (r % 8) * 250);
      for (int i = 0; i < 40 && !stop_holder.load(); ++i) {
        if (lk.lock_for(slice)) {              // 超时反复走进 cancel_acquire
          if (++inside != 1) violations.fetch_add(1);
          --inside;
          granted.fetch_add(1);
          lk.unlock();
        } else {
          denied.fetch_add(1);
        }
      }
    });
  }

  std::this_thread::sleep_for(600ms);          // 风暴时长
  stop_holder.store(true);
  holder.join();
  for (auto& th : racers) th.join();

  CHECK(violations.load() == 0);               // 全程互斥未被破坏
  CHECK(granted.load() + denied.load() > 50);  // 取消路径确实被大量执行
  std::printf("  [取消风暴] 成功=%d 超时取消=%d\n", granted.load(), denied.load());

  CHECK(lk.try_lock());                        // 终局：锁必定回到空闲态
  lk.unlock();
}

// ---------------------------------------------------------------- 自定义子类冒烟

namespace {
class exclusive_only final : public abstract_aqs {  // 只覆写独占钩子的自定义子类
 protected:
  bool try_acquire(int arg) override { return compare_and_set_state(0, arg); }
  bool try_release(int) override {
    set_state(0);
    return true;
  }

 public:
  using abstract_aqs::acquire_shared;          // 故意暴露未实现的共享入口以验证默认行为
};
}  // namespace

static void test_unsupported_hooks() {
  exclusive_only s;
  s.acquire(1);
  s.release(1);
  CHECK_THROWS(s.acquire_shared(1), monitor_error);  // UnsupportedOperationException 对应物
}

// ---------------------------------------------------------------- 主入口

int main() {
  struct case_t {
    const char* name;
    void (*fn)();
  };
  const case_t cases[] = {
      {"reentrant_basic", test_reentrant_basic},
      {"mutex_stress_nonfair", [] { mutex_stress(false, "非公平"); }},
      {"mutex_stress_fair", [] { mutex_stress(true, "公平"); }},
      {"semaphore_stress", test_semaphore_stress},
      {"semaphore_timeout", test_semaphore_try_and_timeout},
      {"countdown_latch", test_countdown_latch},
      {"bounded_queue", test_bounded_queue_with_condition},
      {"condition_timeout", test_condition_timeout_and_errors},
      {"cancel_storm", test_timed_cancel_storm},
      {"unsupported_hooks", test_unsupported_hooks},
  };
  for (const auto& c : cases) {
    std::printf("== %s\n", c.name);
    c.fn();
  }
  const int failed = g_failures.load();
  if (failed)
    std::printf("\n结果：%d 个断言失败\n", failed);
  else
    std::printf("\n结果：全部通过\n");
  return failed == 0 ? 0 : 1;
}

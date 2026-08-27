// bench_aqs.cpp — aqs 性能基准（手工运行，不注册 ctest）
//
//   cmake --build build -j$(nproc) && ./build/bench_aqs
//
// 维度：
//   1. 无竞争单线程成本（ns/op）：aqs 锁 vs std::mutex
//   2. 有竞争吞吐（ops/s，T=1/2/4/8）：量化 arena 互斥与 seq_cst 的代价
//   3. 高竞争交接延迟（ns/次抢锁周期）：检验 futex 唤醒路径质量
//   4. 共享模式：aqs::semaphore(1)/(3) vs POSIX sem_t
//
// 方法：轻量配置取 3 次采样最小值；重度竞争只采单样（噪声远大于采样偏差），
// 并行段用 go 标志对齐起跑线。所有对比在同一进程同一负载下进行。
#include "aqs.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <semaphore.h>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

static inline void cpu_relax() {
#if defined(__x86_64__)
  __builtin_ia32_pause();
#endif
}

// 临界区内的确定性伪工作：若临界区为空，持锁线程会在自己的时间片内
// 批量排空循环，测得的是"时间片独占"而非真实竞争。
static inline void cs_work() {
  for (int i = 0; i < 12; ++i) cpu_relax();
}

static long ns_between(Clock::time_point a, Clock::time_point b) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count();
}

// 先热身一次再采样；samples<0 时只做一次正式采样（重度竞争场景）
template <class Fn>
static long timed_run(Fn&& fn, int samples) {
  fn();                                          // 热身：触页/预热缓存
  if (samples <= 0) {
    const auto t0 = Clock::now();
    fn();
    return ns_between(t0, Clock::now());
  }
  long best = -1;
  for (int s = 0; s < samples; ++s) {
    const auto t0 = Clock::now();
    fn();
    const long d = ns_between(t0, Clock::now());
    if (best < 0 || d < best) best = d;
  }
  return best;
}

// 起 threads 个线程在 go 标志处对齐后同时开跑，返回总耗时 ns
template <class Body>
static long run_parallel(int threads, Body&& body) {
  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::vector<std::thread> pool;
  pool.reserve(static_cast<size_t>(threads));
  for (int t = 0; t < threads; ++t)
    pool.emplace_back([&, t] {
      ready.fetch_add(1);
      while (!go.load(std::memory_order_acquire))
        std::this_thread::yield();
      body(t);
    });
  while (ready.load() < threads) std::this_thread::yield();
  const auto t0 = Clock::now();
  go.store(true, std::memory_order_release);
  for (auto& th : pool) th.join();
  return ns_between(t0, Clock::now());
}

// ---------- 统一 lock/unlock 接口的适配器 ----------
struct aqs_lock_wrap {
  aqs::reentrant_lock m;
  void lock() { m.lock(); }
  void unlock() { m.unlock(); }
};

struct posix_sem_wrap {                 // POSIX 信号量的机械对应物
  sem_t s;
  explicit posix_sem_wrap(int c = 1) { sem_init(&s, 0, static_cast<unsigned>(c)); }
  ~posix_sem_wrap() { sem_destroy(&s); }
  void lock() { sem_wait(&s); }
  void unlock() { sem_post(&s); }
};

struct aqs_sem_wrap {
  aqs::semaphore s;
  explicit aqs_sem_wrap(int c = 1) : s(c) {}
  void lock() { s.acquire(); }
  void unlock() { s.release(); }
};

template <typename LK>
static long bench_uncontended(long iters) {
  LK lk;
  return timed_run([&] {
    for (long i = 0; i < iters; ++i) {
      lk.lock();
      asm volatile("" ::: "memory");             // 防编译器消除空临界区
      lk.unlock();
    }
  }, 3);
}

template <typename LK>
static long bench_contended(int threads, long per_thread) {
  LK lk;
  return timed_run([&] {
    run_parallel(threads, [&](int) {
      for (long i = 0; i < per_thread; ++i) {
        lk.lock();
        cs_work();                       // 逼出真实交接而非时间片批量排空
        lk.unlock();
      }
    });
  }, 0);
}

// 严格交替的交接延迟：锁保护的 baton 轮转，杜绝时间片批量独占。
// 每次持锁必须显式把 baton 递给对方，故 switches 次 acquire 全部属于
// "唤醒对端→完成交接"路径，其均值即真实交接成本。
template <typename LK>
static long bench_handoff(long switches) {
  LK lk;
  auto one_round = [&] {
    std::atomic<int> baton{0};
    std::atomic<long> done{0};
    std::atomic<bool> finished{false};
    const auto t0 = Clock::now();
    run_parallel(2, [&](int id) {
      const int mine = id;               // 各自唯一的座位号；baton 指向当前持有者
      for (;;) {
        while (mine != baton.load(std::memory_order_relaxed) &&
               !finished.load(std::memory_order_relaxed))
          cpu_relax();                   // 等待自旋；两实现同样对待
        if (finished.load(std::memory_order_relaxed)) break;
        lk.lock();
        if (finished.load(std::memory_order_acquire)) {   // 双检：拿到锁再核销
          lk.unlock();
          break;
        }
        if (done.fetch_add(1) + 1 >= switches)
          finished.store(true, std::memory_order_release);  // 终局广播
        else
          baton.store(id ^ 1, std::memory_order_relaxed);   // 移交棒子
        lk.unlock();
      }
    });
    return ns_between(t0, Clock::now());
  };
  one_round();                           // 热身
  return one_round();
}

int main() {
  std::printf("== aqs 性能基准 ==\n");
  const int hw = static_cast<int>(std::thread::hardware_concurrency());

  // ---------- 1. 无竞争单线程成本 ----------
  constexpr long k_uncontended_iters = 3'000'000;
  struct row_t { const char* name; long ns; };
  row_t rows[] = {
      {"aqs 非公平锁", bench_uncontended<aqs_lock_wrap>(k_uncontended_iters)},
      {"aqs 公平锁  ", bench_uncontended<aqs::reentrant_lock>(k_uncontended_iters)},
      {"std::mutex   ", bench_uncontended<std::mutex>(k_uncontended_iters)},
  };
  const double base = rows[2].ns;                // std::mutex 为基线
  std::printf("\n[无竞争] %ld 次 lock+unlock\n", k_uncontended_iters);
  std::printf("  %-14s %10s %12s\n", "实现", "ns/op", "占mutex");
  for (auto& r : rows) {
    const long per_op = r.ns / k_uncontended_iters;
    const int pct = static_cast<int>(r.ns * 100.0 / base + 0.5);
    std::printf("  %-14s %10ld   %3d%%\n", r.name, per_op, pct);
  }

  // ---------- 2. 有竞争吞吐 ----------
  std::printf("\n[有竞争] 吞吐 ops/s（每次进入极短临界区）\n");
  std::printf("  %-6s %14s %14s %14s\n", "线程", "aqs非公平", "aqs公平",
              "std::mutex");
  const int max_t = hw > 8 ? 8 : (hw > 0 ? hw : 4);
  const int thread_counts[] = {1, 2, 4, max_t};
  for (int t : thread_counts) {
    const long per_thread = t == 1 ? 2'000'000 : 200'000;
    const long n_aqsf = bench_contended<aqs_lock_wrap>(t, per_thread);
    const long n_aqsfair = bench_contended<aqs::reentrant_lock>(t, per_thread);
    const long n_std = bench_contended<std::mutex>(t, per_thread);
    auto fmt = [](long ns, long rounds) {
      return static_cast<long>(static_cast<double>(rounds) / (ns * 1e-9));
    };
    const long total_ops = static_cast<long>(t) * per_thread;
    std::printf("  T=%-4d %12ld %13ld %13ld\n", t, fmt(n_aqsf, total_ops),
                fmt(n_aqsfair, total_ops), fmt(n_std, total_ops));
  }

  // ---------- 3. 高竞争交接延迟（严格交替） ----------
  // 双线程经 baton 强制逐次移交：每个持锁周期都对应一次真实的
  // "释放→唤醒对端→交接完成"，均值即唤醒路径的端到端成本。
  constexpr long k_switches = 100'000;
  std::printf("\n[交接延迟] 严格交替 %ld 次移交\n", k_switches);
  {
    const long h_aqs = bench_handoff<aqs_lock_wrap>(k_switches) / k_switches;
    const long h_std = bench_handoff<std::mutex>(k_switches) / k_switches;
    std::printf("  aqs 非公平锁   %5ld ns/次\n", h_aqs);
    std::printf("  std::mutex     %5ld ns/次\n", h_std);
  }

  // ---------- 4. 共享模式：信号量 vs POSIX sem_t ----------
  std::printf("\n[共享模式] T=8 抢信号量，ops/s\n");
  {
    const int workers = max_t;
    const long per_thread = 60'000;

    const long n_aqs1 = bench_contended<aqs_sem_wrap>(workers, per_thread);
    const long n_posix = bench_contended<posix_sem_wrap>(workers, per_thread);

    // 多许可并发通过：验证共享传播级联的有效性
    aqs::semaphore sem3(3);
    const long batch = per_thread / 3;
    const long n_aqs3 = timed_run([&] {
      run_parallel(workers, [&](int) {
        for (long i = 0; i < batch; ++i) {
          sem3.acquire();
          asm volatile("" ::: "memory");
          sem3.release();
        }
      });
    }, 0);

    auto fmt = [](long ns, long ops) {
      return static_cast<long>(static_cast<double>(ops) / (ns * 1e-9));
    };
    std::printf("  aqs::semaphore(1)   %12ld\n",
                fmt(n_aqs1, static_cast<long>(workers) * per_thread));
    std::printf("  POSIX sem_t(1)      %12ld\n",
                fmt(n_posix, static_cast<long>(workers) * per_thread));
    std::printf("  aqs::semaphore(3)   %12ld （多许可并行通过）\n",
                fmt(n_aqs3, static_cast<long>(workers) * batch));
  }

  std::printf("\n完成。\n");
  return 0;
}

# aqs-cpp — Java AQS 的 C++17 移植

参考《AQS源码逐行精读：CLH队列、CAS、独占/共享模式的设计哲学》实现的
AbstractQueuedSynchronizer C++17 版本：以 `std::atomic<int> state` + CLH 变体
FIFO 双向队列 + CAS 复刻 OpenJDK 的模板方法框架，并给出仅覆写钩子即得
三大同步原语的最佳佐证。

## 目录

```
src/aqs.hpp    全部类型声明（框架 + 三大原语）
src/aqs.cpp    框架与原语实现
tests/         真实线程并发压测（无 mock）
docs/          设计对照文档
```

## 构建

```bash
cmake -B build -G Ninja
cmake --build build -j$(nproc)
cd build && ctest --output-on-failure     # 或直接 ./test_aqs
```

依赖：Linux、g++ ≥ 8（C++17）、CMake ≥ 3.14、pthread。

## 一览

| 类 | 对应 Java | 关键语义 |
|---|---|---|
| `aqs::abstract_aqs` | `AbstractQueuedSynchronizer` | 模板方法骨架；子类只实现 4 个钩子 |
| `aqs::reentrant_lock` | `ReentrantLock` | 公平/非公平、可重入计数、Condition |
| `aqs::semaphore` | `Semaphore` | 许可上限不变量、公平可选 |
| `aqs::countdown_latch` | `CountDownLatch` | 计数到 0 唤醒全部等待者 |

waitStatus 常量与 Java 一致：`CANCELLED=1 / SIGNAL=-1 / CONDITION=-2 / PROPAGATE=-3`。

## 快速上手

### 可重入锁 + 条件变量（有界阻塞队列）

```cpp
#include "aqs.hpp"

aqs::reentrant_lock lk;                 // 构造传 true 即公平锁
auto& not_full  = lk.new_condition();
auto& not_empty = lk.new_condition();

// 生产者
lk.lock();
while (q.size() == cap) not_full.await();
q.push_back(v);
not_empty.signal();
lk.unlock();

// 非阻塞尝试 / 超时获取
if (lk.try_lock()) { /* ... */ lk.unlock(); }
if (lk.lock_for(std::chrono::milliseconds(100))) { /* ... */ lk.unlock(); }
```

### 信号量

```cpp
aqs::semaphore sem(3);        // 3 个许可，传 true 开启公平模式
sem.acquire();                // 阻塞取一个
/* 临界区 */
sem.release();                // 归还
```

### 倒计时门闩

```cpp
aqs::countdown_latch latch(4);
// 工作线程：latch.await() 或 latch.await_for(1s)
// 完成方：  latch.count_down();   // 减到 0 唤醒全部等待者
```

## 钩子契约（对应 Java 四个 protected 抽象方法）

```
try_acquire(arg)            -> bool   独占：state 是否允许持有
try_release(arg)            -> bool   独占：是否完全释放（true 触发唤醒后继）
try_acquire_shared(arg)     -> int    共享：<0 排队；>=0 成功并作为传播依据
try_release_shared(arg)     -> bool   共享：true 触发 doReleaseShared 传播
```

自定义同步器示例见 `tests/test_aqs.cpp` 中 `exclusive_only`：
只覆写独占钩子的类调用共享入口会抛 `aqs::monitor_error`
（即 `IllegalMonitorStateException` 的对应物，`UnsupportedOperationException` 同源）。

## 测试

全部真实线程压测，核心断言为**运行时不变量**而非结果比对：

- 互斥不变量：临界区同时进入数恒为 1（公平/非公平各数万次抢锁）
- 许可上限不变量：12 线程争 3 许可，进入数永不超 3 且归还守恒
- 有界队列：6×6 生产消费者合计 6000 元素求和守恒（Condition 全路径）
- 取消风暴：毫秒级以下超时高频触发 cancelAcquire 摘链，全程互斥不被破坏
- 公平性烟雾检查：长压测中每个线程获锁次数不低于均值的一半

## 与 OpenJDK 的差异

| 差异 | 原因 | 说明 |
|---|---|---|
| 无中断语义 | std::thread 不可中断 | InterruptedException 相关路径省略；超时是唯一取消来源，cancelAcquire 完整保留 |
| 节点 arena 回收 | Java 靠 GC | 节点延迟到 synchronizer 析构统一释放，规避反向扫描悬垂指针 |
| Parker 用 futex | 平台原生挂起原语 | 三态 permit（0 空/1 已投递/2 睡眠），保持 LockSupport"先 unpark 后 park 不丢失"语义 |
| 等待一律 2ms 有界睡 | 见 docs/design.md §5 | 封死 SIGNAL 标记与 release 读状态交错的微窗口，最坏代价是竞争时的周期复查 |
| 公平门改走 prev 反向计数 | 见 docs/design.md §6 | next 是惰性发布的单写链，极端竞争下存在陈旧窗口；prev 由入队者亲手写定且永久存活 |

其余（enq/addWaiter 两次 CAS、acquireQueued 自旋 + 惰性 SIGNAL、unparkSuccessor
尾向扫描、setHeadAndPropagate/doReleaseShared 传播、ConditionObject 双队列搬运、
cancelAcquire 含尾指针 CAS 分支）均逐行对照 OpenJDK 源码移植，
参考文章中引用的 cancelAcquire 缺漏处以 JDK 为准。

## 版本

0.1.0（`aqs::version`）

License：MIT

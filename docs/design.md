# aqs-cpp 设计对照与工程决策

对应 OpenJDK `AbstractQueuedSynchronizer`（jdk8u 逐行比对）。文章中引用的
cancelAcquire 缺少尾指针 CAS 分支与"前驱是否可靠"判定，本实现以 JDK 真实源码为准。

## 1. 三大核心的映射

| Java                            | C++                       | 备注                                      |
| ------------------------------- | ------------------------- | ----------------------------------------- |
| `volatile int state`            | `std::atomic<int> state_` | 读 acquire 写 release，CAS 用 seq_cst     |
| `volatile Node head/tail`       | `std::atomic<node*>`      | enq 两次 CAS：惰性哑元头 + 尾插           |
| `volatile waitStatus/prev/next` | 对应原子成员              | next_waiter / tid 与 Java 同为非 volatile |
| `Unsafe.compareAndSwapXxx`      | `compare_exchange_strong` |                                           |
| `LockSupport.park/unpark`       | 每节点 Parker（§4）       |                                           |

## 2. 队列协议（独占路径逐行对照）

- **addWaiter/enq**：快速路径 CAS 尾插，失败自旋入队；先写 `prev` 再抢 tail，
  抢到后才发布 `pred->next`——顺序保证其他线程看到的链没有半成品。
- **acquireQueued**：前驱 == head 才尝试钩子；失败先给前驱惰性打 SIGNAL，
  打完必须再自旋一轮确认，才允许 park。
- **release**：`tryRelease` 成功且 head 带非零状态 → unparkSuccessor。
- **unparkSuccessor**：清 SIGNAL 后优先走 next；next 为空或已取消则
  从 tail 反向扫到最近的有效后继。反向遍历是 AQS 应对取消的核心手段。
- **setHead**：只清 `prev`，**不清 `next`、不改 waitStatus**。
  hasQueuedPredecessors 与唤醒扫描都依赖这条悬而未断的旧链；
  help GC 断链发生在前任节点上（`p.next = null`），不是新 head 上。

## 3. 共享模式

`setHeadAndPropagate` 的双条件传播（propagate>0 或新旧 head 带负状态）与
`doReleaseShared` 的 SIGNAL→0→unpark / 0→PROPAGATE 接力循环均按 JDK 移植；
CountDownLatch 只需把两个共享钩子写成"state 是否为 0"即得。

## 4. Parker：三态 futex permit

```
park_state_: 0=空转  1=唤醒已投递（再 park 直接过）  2=认领床位睡眠中
park():     CAS(0→2) 成功后 FUTEX_WAIT(值校验)；醒来显式消耗观测到的票
unpark():   exchange(1)；旧值==2 才发 FUTEX_WAKE
```

保 LockSupport 关键语义：**unpark 先于 park 不丢失**（先置 1 后睡），
值校验从内核侧杜绝"唤醒先于入队"类竞态。曾用 mutex+condition_variable 版本，
在极限竞争下复现永久搁浅后替换为 futex。

## 5. 有界睡眠（对 Java 无限 park 的偏差）

死锁取证确认了一个 Java 同样存在的窗口——却因 JVM 调度特性几乎不可触发：

1. 释放方读 `head.waitStatus` 得 0，按协议跳过唤醒；
2. 紧接着等待方完成 `CAS(head: 0→SIGNAL)` 并睡死；
3. 此刻锁已空闲，永远不会再有 release 来兑现这张 SIGNAL。

Java 里该窗口依赖调度侥幸收敛；C++ 直接移植会在极限竞争下以十万分之一量级
稳定触发。工程决策：**所有阻塞等待一律 2ms 有界睡**（`node::park_for`），
醒来无条件重查队列状态，任何丢唤醒在 2ms 内自愈；代价仅是竞争时的周期复查，
换取构造层面的活性保证。

## 6. 公平门：prev 反向计数（替代 h.next 比对）

OpenJDK 公平门读 `h.next` 判断第一等待者是否自己。next 由入队者单方面惰性
发布、又会被 setHead 时代的对手撕裂，极端竞争下存在陈旧读窗口（实测可在
公平锁高压下饿死真正队首）。本实现改为：

```cpp
// 当前线程排队的节点登记在 thread_local 中
node* me = tls_current_wait_;
const node* hcur = head_;               // 原子读当前头
int ahead = 0;
for (node* q = me->prev_; q; q = q->prev_) {   // 反向走向历史
    if (q == hcur) break;                // 到达当前头：其之前皆已晋升的历史
    ++ahead;
}
return ahead > 0;                        // 我和头之间还有人 => 让位
```

要点：prev 由每个入队者亲手一次性写定、arena 保证节点永久存活，
因此这条链是全系统唯一"绝对可信"的结构；沿它走到**当前 head 即止**，
自动把早已晋升的祖先排除在计数外（这是最初朴素计数的 bug：
整条家谱都会被算进去，导致所有线程被幽灵拒绝）。

未入队的裸尝试（如 tryLock barge）退回 JDK 的 t/h/h.next 语义。

## 7. 条件变量 ConditionObject

双队列搬运完全照搬 JDK：await 侧 addConditionWaiter→fullyRelease→
isOnSyncQueue 轮询→acquireQueued(savedState)；signal 侧 transferForSignal
(CAS CONDITION→0 + enq + 可靠性存疑时直接 unpark)。定时版补齐了
transferAfterCancelledWait 的语义（超时者 CAS 赢了自己 enq 回主队列，
输给 signal 则 yield 等对方完成搬运）。

## 8. 内存回收：arena

活跃节点的 prev/next 可能仍被其他线程的反向扫描引用，逐个 delete 有 UAF
风险且无法在不引入 hazard pointer 的前提下安全判定。因此节点由 synchronizer
持有的 arena 统一拥有、析构时释放——每对象生命周期内不归还任何节点内存。

## 9. 测试策略

并发正确性靠**不变量断言**而非时序假设：临界区进入计数恒等式、许可上限、
生产消费总量守恒、公平饥饿下界、超时取消风暴。调试期使用过三层取证工具
（事件环形缓冲、搁浅探针、全线程验尸 dump），已随根因修复移除，
仅保留测试侧看门狗思路的痕迹于 cancel_storm 用例设计。

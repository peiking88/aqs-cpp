# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目

Java AbstractQueuedSynchronizer 的 C++17 逐行移植。单库结构，无第三方依赖（仅 pthread/标准库）。

## 常用命令

```bash
cmake -B build -G Ninja                # 配置（默认 RelWithDebInfo）
cmake --build build -j$(nproc)         # 构建
./build/test_aqs                       # 运行全部测试（唯一测试入口）
cd build && ctest --output-on-failure  # 等价方式
```

无 lint/format 工具链；编译即检查（`-Wall -Wextra`）。测试是普通可执行文件（自带断言的 main），不支持按用例过滤——新增用例加进 `tests/test_aqs.cpp` 的 `cases` 表即可。

## 架构

三个文件构成全部核心：

- `src/aqs.hpp` / `src/aqs.cpp` —— 框架 + 三大原语，同一个 `aqs` 命名空间
- `tests/test_aqs.cpp` —— 真实线程压测，断言运行时不变量（互斥、许可上限、求和守恒），不用 mock

**模板方法模式是整个设计的骨架**：`abstract_aqs` 固定 acquire/release 全部流程（排队、park/unpark、取消、共享传播），子类只覆写 4 个钩子即可得到一个同步原语：

- 独占：`try_acquire` / `try_release` → `reentrant_lock`
- 共享：`try_acquire_shared` / `try_release_shared` → `semaphore`、`countdown_latch`

未覆写的钩子被调用时抛 `aqs::monitor_error`。`condition` 是 `abstract_aqs` 的公有嵌套类，只能配独占子类使用。

**关键工程决策（改动前必读 `docs/design.md` 对应章节）：**

| 决策                                        | 位置       | 一句话原因                                          |
| ------------------------------------------- | ---------- | --------------------------------------------------- |
| 阻塞等待一律 2ms 有界睡而非无限 park        | design §5  | 封死 SIGNAL 标记与 release 读状态交错的丢唤醒微窗口 |
| 公平门沿 prev 反向计数，不比对 h.next       | design §6  | next 是惰性发布链，极限竞争下有陈旧窗口             |
| 节点走 arena（析构统一回收），不逐个 delete | hpp 内注释 | 规避反向扫描悬垂指针                                |
| Parker 用三态 futex permit（0/1/2）         | design §4  | 保住 LockSupport"先 unpark 后 park 不丢失"语义      |

这些偏差是对 Java 移植过程中真实踩坑后的修复（详见 `summary.md` 排障历程），不要轻易"优化"回 JDK 直译版本。

## 约定

- 版本号两处同步：`CMakeLists.txt` 的 `project(... VERSION ...)` 与 `src/aqs.hpp` 的 `aqs::version`。
- 注释、文档、提交信息统一中文。
- waitStatus 常量与 Java 对齐（`CANCELLED=1/SIGNAL=-1/CONDITION=-2/PROPAGATE=-3`），改值会破坏与 OpenJDK 源码的可对照性。

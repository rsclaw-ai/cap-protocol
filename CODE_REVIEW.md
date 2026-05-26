# CAP Protocol — 全面代码审查报告

**审查日期:** 2026-05-23  
**审查范围:** 全部 Rust 源码 (`crates/`)、规范文档 (`docs/`)、网站 (`website/`)、CI 配置、项目配置

---

## 目录

1. [关键问题 (Critical)](#1-关键问题-critical)
2. [高风险问题 (High)](#2-高风险问题-high)
3. [中等风险问题 (Medium)](#3-中等风险问题-medium)
4. [低风险问题 (Low)](#4-低风险问题-low)
5. [代码质量与风格](#5-代码质量与风格)
6. [文档与规范问题](#6-文档与规范问题)
7. [CI/CD 与工程化问题](#7-cicd-与工程化问题)
8. [安全审查](#8-安全审查)
9. [优化建议](#9-优化建议)
10. [总结](#10-总结)

---

## 1. 关键问题 (Critical)

### C1. `acp` feature 独立编译失败

**文件:** `crates/cap-rs/src/driver.rs:34,126-127`  
**描述:** `common` 模块 (定义 `Driver`, `DriverError`, `DriverExitStatus`) 的 cfg gate 为 `#[cfg(any(feature = "stream-json", feature = "pty", feature = "codex"))]`，但未包含 `feature = "acp"`。当用户仅启用 `acp` feature 时，`common` 模块不会被编译，导致 `acp.rs` 中引用 `crate::driver::{Driver, DriverError, DriverExitStatus}` 编译失败。  
**修复:** 将 `feature = "acp"` 加入两个 cfg gate。

### C2. `config.rs` 中 `unwrap()` 可能 panic

**文件:** `crates/cap-rs-orchestrator/src/config.rs:191`  
**描述:** `Action::Collect(self.collect.unwrap())` 在 if-else 链的最后分支中使用了 `unwrap()`。如果未来重构破坏了"仅一个 action 被设置"的不变量，将在运行时 panic。  
**修复:** 使用 `ok_or_else` 返回 `Config` 错误，或改用 `match` 结构。

### C3. `pty:` URI 的命令注入风险

**文件:** `crates/cap-rs-orchestrator/src/config.rs:66-68`  
**描述:** `pty:codex --some-arg` 或 `pty:codex; rm -rf /` 会直接解析为 `DriverKind::Pty("codex --some-arg")`，仅过滤了空字符串。尽管底层 `Command::new()` 本身较安全，但命令字符串中嵌入的空白字符可能导致参数拆分混淆。  
**修复:** 对 `cmd` 部分施加 `valid_session_id` 级别的限制（仅字母数字、`_`、`-`、`/`、`.`）或拒绝任何非简单二进制名的值。

### C4. executor 中 `Mutex::lock().unwrap()` 在互斥锁中毒时 panic

**文件:** `crates/cap-rs-orchestrator/src/executor.rs:34,365`  
**描述:** `self.audit.lock().unwrap()` 在另一个线程 panic 导致 audit mutex 中毒后，所有后续访问都会 panic。  
**修复:** 使用 `.lock().unwrap_or_else(|e| e.into_inner())` 恢复，或改用 `tokio::sync::Mutex`。

---

## 2. 高风险问题 (High)

### H1. `CodexAppServerDriver` 未传递 model 到 `thread/start`

**文件:** `crates/cap-rs/src/driver/codex_app_server.rs:436-458,467-473`  
**描述:** 创建新 thread 时，params 包含 `cwd`、`approvalPolicy`、`sandbox`、`baseInstructions`，但**不包含 `model`**。model 仅在握手后合成的 `Ready` 事件中使用（第471行），呈纯装饰性效果。`CodexExecDriver` 和 `CodexMcpDriver` 正确传递了 model（`-m` 标志/参数），此不一致会无声地误导用户。  
**修复:** 将 `model` 加入 `thread/start` 参数。

### H2. `CodexMcpDriver` 合成 `Ready` 带空的 `session_id`

**文件:** `crates/cap-rs/src/driver/codex_mcp.rs:349-354`  
**描述:** 合成 `AgentEvent::Ready` 的 `session_id: String::new()` 为空字符串。真实的 `thread_id` 仅在后续 `session_configured` 通知到达后才会填充，但该通知在 `Ready` **之后**才到达（在第一个 `tools/call` 时）。依赖 `session_id` 的调用者会收到空字符串。  
**修复:** 延迟发送合成 `Ready`，直到观察到 `session_configured`。

### H3. Oneshot sender 泄露

**文件:** 
- `crates/cap-rs/src/driver/codex_app_server.rs:496-507`
- `crates/cap-rs/src/driver/codex_mcp.rs:375-386`
- `crates/cap-rs/src/driver/acp.rs:350-362`

**描述:** 在 `send_and_await` 中，oneshot sender 被插入 `pending` HashMap，然后发送请求。如果 `writer_tx.send()` 失败（如 agent 已退出），函数返回 `Err(DriverError::AgentExited)`，但 oneshot sender 仍留在 `pending` 中。虽然每个泄露实例有限，但长时间运行的会话中重复失败会积累无界条目。  
**修复:** 在错误路径上移除 pending 条目。

### H4. `executor.rs` 中 HashMap 直接索引可能 panic

**文件:** `crates/cap-rs-orchestrator/src/executor.rs:165-167`  
**描述:** `self.spec.fleet.sessions[id]` 直接使用 `[]` 索引，如果 `id` 不在 sessions 映射中（绕过验证或由于 bug），将直接 panic。  
**修复:** 改用 `.get(id).ok_or_else(|| ...)?` 返回正确错误。

### H5. `main.rs` 同步 I/O 阻塞异步运行时

**文件:** `crates/cap-cli/src/main.rs:44`  
**描述:** `std::fs::read_to_string(&path)?` 在异步运行时中执行同步 I/O。对于 CLI 工具尚可接受，但如果 `path` 指向慢速文件系统（NFS、FUSE）会阻塞事件循环。  
**修复:** 使用 `tokio::fs::read_to_string(&path).await?`。

### H6. Session spawn 失败导致 worktree 泄露

**文件:** `crates/cap-rs-orchestrator/src/registry.rs:43-46`  
**描述:** `worktree.create()` 成功后若 `factory.build()` 失败，worktree 文件已存在于磁盘但无 session 注册。该 worktree 永远不会被清理。  
**修复:** 使用 `scopeguard` 延迟清理：worktree 创建后注册 deferred 清理，session 注册成功后取消清理。

### H7. `bus.send()` 在关闭时可能导致死锁

**文件:** `crates/cap-rs-orchestrator/src/session.rs:54-81`  
**描述:** 如果 `bus` 接收端已丢弃（executor 关闭），`bus.send()` 会阻塞直到 channel 被清空。`cancel` token 仅在顶层的 `select!` 中检查，actor 可能在一个已满 channel 的 `bus.send()` 上卡住任意长时间。  
**修复:** 在 `bus.send()` 周围添加独立的 `tokio::select!` 和 `cancel` 分支，或使用 `bus.try_send()`。

### H8. 未使用的 `futures` 可选依赖

**文件:** `crates/cap-rs/Cargo.toml:58`  
**描述:** `futures` crate 声明为 `stream-json` feature 的可选依赖，但从未在任何源码中导入或使用。仅增加了构建时间和依赖树大小。  
**修复:** 移除 `dep:futures` 及对应 feature 依赖。

---

## 3. 中等风险问题 (Medium)

### M1. PTY 驱动使用固定 150ms 延迟等待提示提交

**文件:** `crates/cap-rs/src/driver/pty.rs:886`  
**描述:** `tokio::time::sleep(Duration::from_millis(150))` 是在写入提示文本和发送 Enter 之间的固定延迟。在不同的 agent、系统负载和终端模拟器速度下都很脆弱。慢系统上可能不足150ms，导致 Enter 在文本被摄取前到达。  
**修复:** 采用轮询方式：写入文本后，在发送 Enter 前轮询 parser 的 prompt gate 检查输入是否就绪，附带可配置的最大等待时间。

### M2. ACP 权限响应无匹配 optionId

**文件:** `crates/cap-rs/src/driver/acp.rs:162-165`  
**描述:** `select_option` 返回 `None`（未找到与权限决策匹配的 `optionId`）时，代码发送 `{"outcome": "cancelled"}` 不包含 `optionId`。按 ACP 规范，某些 agent 可能要求 `optionId` 存在，缺省时可能导致 agent 忽略响应，权限请求未被解决，从而死锁会话。  
**修复:** 回退时发送第一个选项的 ID 和 `"cancelled"`。

### M3. `detect_bracketed_paste` 静默忽略 mutex 中毒

**文件:** `crates/cap-rs/src/driver/pty.rs:603`  
**描述:** 使用 `if let Ok(mut g) = self.gate.lock()` 静默忽略中毒的 mutex，而代码库中其他所有 mutex 访问都使用 `.expect("... poisoned")` panic。不一致的失败模式。  
**修复:** 统一使用 `expect()` 模式。

### M4. `on_bytes` 中 `emit_cursor` 越界可能 panic

**文件:** `crates/cap-rs/src/driver/pty.rs:392`  
**描述:** `let region = &self.buffer[self.emit_cursor..]` 在 `emit_cursor > self.buffer.len()` 时 panic。  
**修复:** 添加防御性守卫：`if self.emit_cursor > self.buffer.len() { self.emit_cursor = self.buffer.len(); }`。

### M5. `is_error` 使用黑名单标记未来未知状态为错误

**文件:** `crates/cap-rs/src/driver/codex.rs:481`  
**描述:** `let is_error = !matches!(status, "completed" | "in_progress" | "")`——任何这三个之外的 status 都被视为错误。如果 codex 引入新的非错误状态（如 `"passed"`、`"skipped"`）将被错误标记。  
**修复:** 改用白名单：仅将已知错误状态视为错误 `matches!(status, "failed" | "error" | "cancelled")`。

### M6. 路由图无循环检测

**文件:** `crates/cap-rs-orchestrator/src/config.rs:222-290`  
**描述:** `validate()` 不检测路由循环。`a.done -> b, b.done -> a` 的配置会验证通过，然后在运行时陷入无限循环。  
**修复:** 在 route 图上添加循环检测。

### M7. `AuditLog` 时间戳回退到 0

**文件:** `crates/cap-rs-orchestrator/src/audit.rs:30-33`  
**描述:** `SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(0)`——如果系统时钟早于 Unix 纪元，静默返回 `0`。  
**修复:** 添加 `warn!` 日志或改用单调时钟。

### M8. `worktree.create()` 不重新验证 session 名称

**文件:** `crates/cap-rs-orchestrator/src/worktree.rs:51-56`  
**描述:** `create()` 直接将 `session` 字符串嵌入 git branch 名：`format!("cap/{session}")`。如果绕过 `validate()` 或 session ID 含恶意字符，可能产生危险的 git 命令。  
**修复:** 在 `create()` 内部重新验证 session 名。

### M9. `real_factory.rs` 对特定 agent 硬编码

**文件:** `crates/cap-rs-orchestrator/src/real_factory.rs:71-74`  
**描述:** 通过 `cmd.as_str() == "opencode"` 字符串匹配区分 `AcpDriver::opencode` 和 `AcpDriver::builder`。新的 agent 需要同步更新此文件和 `config.rs::DriverKind`。  
**修复:** 采用注册表模式（agent-name -> builder 函数映射）。

### M10. 重复 event channel 导致不必要的克隆

**文件:** `crates/cap-rs-orchestrator/src/executor.rs:89-90,251`  
**描述:** 两路独立 channel（`bus` 和 `out`），bus 中的每个 event 在转发到 `out` 前都要 `clone()`（第251行）。高频流式事件中开销大。  
**修复:** 考虑合并为单路 channel + broadcast，或使用 `match &ev` 引用匹配减少克隆。

### M11. `main.rs` 主循环在 executor 崩溃后永远挂起

**文件:** `crates/cap-cli/src/main.rs:81-122`  
**描述:** 只有 `FleetComplete` 会使循环退出。如果 executor 未发送 `FleetComplete` 就崩溃（如 panic），循环永远挂起。  
**修复:** 添加超时，或 `CancellationToken` 由 Ctrl-C 处理器触发，用 `tokio::select!` 退出。

### M12. `registry.shutdown()` 不处理取消竞争

**文件:** `crates/cap-rs-orchestrator/src/registry.rs:63-68`  
**描述:** 丢弃 inbox 并不能解除 session 在 `bus.send()` 上的阻塞（只有丢弃 bus sender 才能）。session 可能无限等待。  
**修复:** 调用 `registry.shutdown()` 前先丢弃 `bus_tx`。

---

## 4. 低风险问题 (Low)

### L1. `Usage` 结构体缺少 `#[non_exhaustive]`

**文件:** `crates/cap-rs/src/core.rs:483`  
**描述:** 所有公开枚举都标记了 `#[non_exhaustive]`，但 `Usage` 结构体没有。新增字段对使用结构体字面量的下游用户是 semver-breaking 变更。

### L2. `stderr_drain` 静默丢弃读取错误

**文件:** 多处 (`stream_json.rs:412`, `codex.rs:331`, `codex_app_server.rs:629`, `codex_mcp.rs:465`, `acp.rs:498`)  
**描述:** `while let Ok(Some(line)) = lines.next_line().await`——如果发生读取错误，stderr 被静默丢弃，可能丢失重要诊断信息。  
**修复:** 添加 `warn!` 日志或捕获 error 结果。

### L3. `codex_mcp.rs` 中 Cancel request ID 可能错误

**文件:** `crates/cap-rs/src/driver/codex_mcp.rs:162`  
**描述:** `Cancel` 发送 `requestId` 为 `self.next_id.load(...).saturating_sub(1)`，假设最新发送的请求就是需要取消的。但如果没有先前的请求（next_id=1），结果为 `0`，不匹配任何实际请求 ID。  
**修复:** 在 driver 结构体中显式跟踪最后一次 prompt 请求的 ID。

### L4. `seen_first_prompt` 在 `ReplParser` 中永不重置

**文件:** `crates/cap-rs/src/driver/pty.rs:254`  
**描述:** `seen_first_prompt` 设为 `true` 后永不重置。如果复用同一个 parser 实例进行会话重启，首次提示会被误认为 `Done` 边界而非 `Ready`。  
**修复:** 为 `ReplParser` 添加 `reset()` 方法。

### L5. `ReplParser` 的 `scan_for_boundary` 双 trim 不够稳健

**文件:** `crates/cap-rs/src/driver/pty.rs:326`  
**描述:** `line.trim_end_matches(['\n', '\r'])` 对 `\r\n` 一次只移除末尾 `\n`。实践中因 `vt100` 输出特性而正确，但逻辑脆弱。

### L6. `is_some_and` 需要 Rust 1.70+

**文件:** `crates/cap-rs/src/driver/pty.rs:711`  
**描述:** `Option::is_some_and` 在 Rust 1.70 中稳定。如果 workspace 设定的 `rust-version` 低于 1.70 会编译失败。当前 `rust-version = "1.85"` 没有问题，但值得注意兼容性边界。

### L7. `testing.rs` 中 `last_decision` 字段从未读取

**文件:** `crates/cap-rs-orchestrator/src/testing.rs:28,102-104`  
**描述:** 该字段在 `send()` 方法中被写入，但无测试或生产代码读取，疑似死代码/调试遗留。

### L8. `OrchestratorEvent` 和 `OrchestratorControl` 缺少 `PartialEq`

**文件:** `crates/cap-rs-orchestrator/src/event.rs:11-61`  
**描述:** 缺少 `PartialEq` 推导，导致基于断言的测试编写困难。

### L9. 函数体内 import 语句

**文件:** `crates/cap-rs/src/driver/pty.rs:1171`  
**描述:** `use std::sync::mpsc::RecvTimeoutError;` 在函数体内。代码库惯例是将 import 放在文件顶部。

### L10. `let _ = streamed_this_turn;` 消除警告模式

**文件:** `crates/cap-rs/src/driver/codex_mcp.rs:316`  
**描述:** 使用 `let _ = var;` 消除未使用变量警告，不如显式 `drop(var)` 语义清晰。

### L11. `try_clone_reader`/`take_writer` 嵌套错误丢失原始类型

**文件:** `crates/cap-rs/src/driver/pty.rs:1070-1075`  
**描述:** `std::io::Error::other(e.to_string())` 若 `e` 已经是 `std::io::Error`，重复包装丢失原始错误类型（如 `NotFound`、`PermissionDenied`）。

---

## 5. 代码质量与风格

| # | 文件 | 行 | 问题 |
|---|------|-----|------|
| Q1 | `config.rs` | 148 | `raw_tokens()` 为 `fn`（非 `pub`）但被公开 API 依赖 |
| Q2 | `config.rs` | 207-213 | `valid_git_ref` 允许 `.` 可能与路径遍历检查混淆 |
| Q3 | `executor.rs` | 337-339 | `by_subtask` 可能读取错误的 buffer |
| Q4 | `executor.rs` | 304 | `_stop: StopReason` 被接收但丢弃 |
| Q5 | `executor.rs` | 342-343 | subtask 超过 workers 数量时静默丢弃 |
| Q6 | `executor.rs` | 238 | `bus_rx.recv()` 返回 `None` 时无条件 break，可能过早关闭 |
| Q7 | `session.rs` | 180-187 | 嵌套 `select!` 可能在 `Ask` 策略下使 cancel 饿死 |
| Q8 | `session.rs` | 29 | inbox 容量 32 可能在高负载下阻塞 sender |
| Q9 | `registry.rs` | 55-59 | 关闭的 inbox 错误被归类为 `Config` 而非运行时错误 |
| Q10 | `lib.rs` (orchestrator) | 19-30 | `OrchestratorError` 仅3个变体，缺少 `Io`、`Spawn` 等 |
| Q11 | `lib.rs` (orchestrator) | 5 | `missing_debug_implementations` 警告因 `Run` 结构体未实现 Debug |
| Q12 | `Cargo.toml` (orchestrator) | 13 | 库 crate 拉入 `rt-multi-thread` |
| Q13 | `codex.rs` | 356 | `Ready` 中 model 总是 `None` 即使已配置 |

---

## 6. 文档与规范问题

| # | 严重度 | 文件 | 描述 |
|---|--------|------|------|
| D1 | 高 | `docs/cap-v1.md` §7 | `cap.session.ready` 事件有引用但未正式定义，无 JSON schema、无 `kind` 值、无字段说明 |
| D2 | 高 | `README.md` | 里程碑表格与 `docs/STATUS.md` 矛盾——README 说 orchestrator 是 "planned" (2026-07)，STATUS.md 说 "已实现并落地" |
| D3 | 高 | `README.md:81-86` | 引用的 `examples/claude-code.toml` 等 5 个 Manifest 示例文件全部缺失 |
| D4 | 中 | `docs/cap-v1.md:728-734` | `_meta.cap.message_to` 跨 agent 通信字段 schema 未定义：类型（单 URN/数组/对象？）、不存在目标的处理、传递保证均未说明 |
| D5 | 中 | `docs/cap-v1.md:925` | 附录 A4 指出部分 agent 在 SIGWINCH 时可能 "corrupt their TUI"，但无检测或恢复指导 |
| D6 | 中 | `docs/cap-v1.md:876` | JSON-RPC 错误码分配范围 `-32099~-32000`，已有分配仅到 -32024，`-32000` 自身含义、Profile 可用槽位均未说明 |
| D7 | 中 | `docs/cap-v1.md:124` | Manifest schema URI `https://cap-protocol.org/schema/manifest/v1.json` 是占位符，未上线 |
| D8 | 中 | `docs/STATUS.md:3` | STATUS.md 说 "overwrite on each work session"，但包含架构决策等有价值信息，不可靠 |
| D9 | 中 | `docs/cap-v1.md:176` | `a2a_serve_at` 字段类型歧义：示例中用 `false`（布尔），注释说"设置为 URL"，严格 schema 会拒绝其中之一 |
| D10 | 中 | `docs/cap-v1.md` 各节 | `ready_when` 已要求但无默认值；`stop_reason` 枚举非穷举定义；预算聚合模型未指定跨 session 还是累计；`UserInputInject` content schema 宽松 |

---

## 7. CI/CD 与工程化问题

| # | 严重度 | 文件 | 描述 |
|---|--------|------|------|
| C1 | 高 | `.github/workflows/ci.yml` | 无 Windows 测试。CAP 旨在作为通用协议，Windows 是重大遗漏（`portable-pty` 支持 Windows） |
| C2 | 高 | `.github/workflows/ci.yml` | 无 `cargo-deny`/`cargo-audit` 等供应链安全扫描 |
| C3 | 中 | `.github/workflows/ci.yml` | CI 仅在 `main` 分支推送时触发，特性分支无 CI 覆盖 |
| C4 | 中 | `.github/workflows/ci.yml` | `RUSTFLAGS: "-D warnings"` 全局应用于 all jobs，依赖的废弃警告会使 test 失败 |
| C5 | 中 | `.github/workflows/ci.yml` | MSRV 检查仅运行 `cargo check`，未运行 `test`/`clippy` |
| C6 | 中 | 根目录 | 缺少 `rust-toolchain.toml`，贡献者之间可能有工具链版本差异 |
| C7 | 中 | 根目录 | 缺少 `CONTRIBUTING.md` 和 `CHANGELOG.md` |
| C8 | 中 | `.github/` | 缺少 `dependabot.yml` |
| C9 | 低 | 根目录 | 缺少 `.editorconfig` 和 pre-commit 配置 |
| C10 | 低 | `.github/workflows/ci.yml` | 无交叉编译检查（`aarch64-unknown-linux-gnu` 等） |

---

## 8. 安全审查

| # | 严重度 | 文件 | 问题 |
|---|--------|------|------|
| S1 | 关键 | `config.rs:66-68` | `pty:` URI 中的命令注入（见 C3） |
| S2 | 高 | `worktree.rs:51-56` | session ID 注入 git 命令（见 M8） |
| S3 | 中 | `real_factory.rs:84` | `--dangerously-bypass-approvals-and-sandbox` 标志在 unsafe cmd 下可能被误用 |
| S4 | 中 | `Cargo.toml` | 无可信编译或依赖验证 |
| S5 | 低 | `website/index.html` | 外部链接无 `rel="noopener noreferrer"`，无 CSP |
| S6 | 安全 | 全部 | **未发现 `unsafe` 代码**——代码库使用纯安全 Rust |

---

## 9. 优化建议

### 9.1 性能优化

| # | 描述 | 预期收益 | 工作量 |
|---|------|---------|--------|
| P1 | `executor.rs:251` 避免 event 克隆（使用引用匹配或 broadcast） | 减少高吞吐场景下的分配 | 小 |
| P2 | 将 orchestrator 的 feature flags 改为镜像 `cap-rs` 的 feature gates | 减少不必要的编译 | 中 |
| P3 | `session.rs:29` 增加 inbox 容量 (32→256) | 减少高并发下的发送端阻塞 | 小 |
| P4 | 移除未使用的 `futures` 依赖 | 减少构建时间 | 小 |

### 9.2 安全加固

| # | 描述 | 优先级 |
|---|------|--------|
| S1 | `config.rs:66-68`——限制 `pty:` 命令部分为简单二进制名 | 高 |
| S2 | `worktree.rs:51-56`——重新验证 session ID | 高 |
| S3 | 添加 `cargo deny` 供应链安全检查 CI 步骤 | 中 |
| S4 | 添加 `dependabot.yml` | 中 |
| S5 | 添加 CSP 和外链 `rel` 属性到网站 | 低 |

### 9.3 弹性与正确性

| # | 描述 | 优先级 |
|---|------|--------|
| F1 | `registry.rs:43-46`——使用 `scopeguard` 防止 worktree 泄露 | 高 |
| F2 | `codex_app_server.rs:496-507`——在错误路径清理 oneshot sender | 高 |
| F3 | `executor.rs:165-167`——HashMap 索引改用 `get().ok_or()` | 高 |
| F4 | `config.rs:222-290`——添加路由循环检测 | 中 |
| F5 | `session.rs:54-81`——`bus.send()` 添加取消感知 | 中 |
| F6 | `pty.rs:886`——固定 sleep 改轮询 | 中 |
| F7 | `acp.rs:162-165`——权限响应回退发送 optionId | 中 |

### 9.4 工程化改进

| # | 描述 | 优先级 |
|---|------|--------|
| E1 | 添加 `rust-toolchain.toml` | 中 |
| E2 | 添加 `CONTRIBUTING.md`、`CHANGELOG.md` | 中 |
| E3 | 添加 Windows CI | 中 |
| E4 | 添加 `dependabot.yml` | 中 |
| E5 | 修复 README 里程碑与 STATUS.md 的矛盾 | 高 |
| E6 | 创建缺失的 5 个示例 Manifest 文件 | 高 |
| E7 | 在 `cap-v1.md` 中正式定义 `cap.session.ready` | 高 |

---

## 10. 总结

### 严重度分布

| 严重度 | 源代码 | 文档/工程化 | 合计 |
|--------|--------|-------------|------|
| 关键 | 4 | 0 | **4** |
| 高 | 8 | 3 | **11** |
| 中 | 12 | 6 | **18** |
| 低 | 13 | 5 | **18** |
| **合计** | **37** | **14** | **51** |

### 最优先修复项（立即执行）

1. **C1** — `acp` feature 独立编译失败（阻塞下游使用）
2. **C3/S1** — `pty:` 命令注入（安全风险）
3. **H1/H2** — `CodexAppServerDriver`/`CodexMcpDriver` 模型传递和 session_id 问题（功能正确性）
4. **H3** — Oneshot sender 泄露（资源安全）
5. **H6/F1** — Worktree 泄露（资源安全）
6. **D2** — README 里程碑需更新（误导新用户）
7. **D3** — 缺失示例 Manifest（开发者入门需要）
8. **D1** — `cap.session.ready` 定义缺失（规范完整性）

### 总体评价

该项目代码质量**较高**：纯安全 Rust、清晰的架构分层、完备的 feature gate 体系、良好的模块文档、统一的 `Driver` trait 设计。主要问题集中在新实现的 `codex_app_server`/`codex_mcp` 驱动器的**正确性边界**（模型传递、session_id 同步、资源清理）以及**工程化成熟度**方面（CI 覆盖、文档更新、示例完整性）。

状态：**Alpha 阶段**，适合参与贡献和实验性使用，建议在上生产前修复所有关键/高风险问题。

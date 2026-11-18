# freshnessgate — 监控采集源的新鲜度、schema 完整性、空值/重复/离群，异常告警隔离与对账。

运行栈：Python 3.14 + 标准库 `sqlite3` + `httpx` + `pytest`（离线，无需联网）

## 运行方式
读 `fixtures/input.json` → `loader` → `schema_check`（质量入口）→ `normalizer` →
其余处理阶段 → `keyer` → 幂等落 `sqlite`；坏记录在质量入口被隔离。
```text
make run     # 运行离线流程，输出摄取/落库/隔离数量
make test    # 运行项目测试，包括质量门、隔离与幂等性检查
```

命令行（`PYTHONPATH=src python3 -m freshnessgate.cli <命令>`）：

| 命令 | 作用 |
| --- | --- |
| `run [--fixtures PATH]` | 跑流水线，输出统计 |
| `policy` | 导出规则版本与全部决策依据 |
| `quarantine` | 列出隔离样本 |
| `replay` | 用当前规则回放隔离样本（校验摘要 + 是否仍被拦截） |
| `recover` | 从只追加日志重建隔离库 |

环境变量：`FRESHNESSGATE_DB` / `FRESHNESSGATE_QUARANTINE_DB` /
`FRESHNESSGATE_QUARANTINE_JOURNAL` / `FRESHNESSGATE_FIXTURES` /
`FRESHNESSGATE_UNKNOWN_FIELD_POLICY=allow|strip|block`。

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段与失败路由，不在编排层加入业务判断。
业务模块：`anomaly`, `schema_check`, `reconcile`, `quarantine`, `freshness`, `alert`。
通用模块：`loader / normalizer / keyer / store / pipeline / cli`。

## 质量入口：schema_check

采集接口升级后，关键字段可能改名或改变类型。质量入口在 **写 SQLite 之前** 判定
每条记录，判定结果是一份可序列化的契约（`report`），随隔离样本和正式记录一起持久化：

- `rule_version`：本次校验使用的规则版本；
- `raw_digest`：原始样本规范 JSON 的 sha256，可复算、用于回放防篡改；
- `fields`：逐字段的 `status / action / expected / actual_type / policy`；
- `decision` / `blocked` / `route`：`pass` / `repair` / `block`，`block` 一律走隔离；
- `reasons` / `repairs` / `unknown_fields` / `compatibility`：决策依据与修复明细。

### 版本与兼容

| 输入 | 处理 | 依据 |
| --- | --- | --- |
| 缺省 `schema_version` | 按 v1（legacy）校验，不注入新字段 | 旧格式兼容路径最短，旧消费方零改动 |
| `2` / `"2"` / `"2.0"` / `"v2"` / `schemaVersion` | 归一化为主版本 2 | 只做无歧义的写法归一 |
| 未知主版本（如 `9`） | `block` | 新主版本可能改变字段语义，旧规则不能解释 |
| 两个版本字段声明不一致 | `block` | 版本不可判定 |

### 放行 / 修复 / 阻断的依据

| 变化 | 决策 | 依据 |
| --- | --- | --- |
| 新增未声明字段 | **放行**（默认） | 不改变已知字段语义；前向兼容。可配置为 `strip`（修复）或 `block` |
| 注册表声明的无损重命名（`name→title`、`ts→observed_at`） | **修复** | 1:1 映射、语义不变，registry 显式声明 |
| 新旧字段并存且值相同 | **修复** | 删除冗余别名不丢信息 |
| 新旧字段并存但值不同 | **阻断** | 无法判断权威值 |
| 旧格式整数写成十进制字符串 / 整值浮点 | **修复**（仅 v1） | 无损整数化，记入 `repairs` |
| 其它类型变化（如数量变成文本/布尔） | **阻断** | 自动修复会掩盖上游升级 |
| 必填缺失 / 非可空收到 null | **阻断** | 采集契约未满足 |
| 未知枚举值 | **阻断** | 可能是新状态或脏数据，不能安全映射 |
| 非法时间戳 | **阻断** | 声明为时间戳却不可解析 |

完整依据可通过 `schema_check.describe_policy()` / `cli policy` 导出，策略表 `POLICY`
为每个决策码保存 `action` 与 `rationale`。

### 落库契约

正式表仍是 `records(key, data)`（旧读取路径 `store.all_records()` 形状不变），
另追加列 `schema_version / rule_version / raw_digest / schema_report / decision /
source_format`；旧库通过 `ALTER TABLE` 平滑迁移。`store.contract_of(key)` 读取单条
记录的质量门契约。

## 隔离与回放

- 坏记录在质量入口直接进入隔离区，**不会** 经过后续阶段、也不会写入正式库。
- 隔离采用双层持久化：先写只追加 JSONL 日志并 `fsync`（第一落点），再写独立
  SQLite 隔离库。SQLite 失败时日志仍保留诊断样本，运行结束可用 `recover` 重建。
- 两条日志/库落点都失败时抛 `IsolationError` 并回滚正式库事务——宁可不落库，
  也不允许未验证数据进入正式结果。
- 每次运行开始会从日志幂等补写隔离库，覆盖“隔离保存失败后重启”的场景。
- `replay` 用当前规则重放样本，返回 `digest_ok`（样本是否被篡改）与
  `still_blocked`（当前规则是否仍拦截）。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，
`tests/test_schema_gate.py` 覆盖半批旧格式 + 半批新格式、未知枚举、缺失必填、
重命名冲突、未知字段策略、隔离库失败后重启与诊断回放。运行方式以 `Makefile` 为准。

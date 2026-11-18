# freshnessgate — 监控采集源的新鲜度、schema 完整性、空值/重复/离群，异常告警隔离与对账。

运行栈：Python 3.14 + 标准库 `sqlite3` + `httpx` + `pytest`（离线，无需联网）

## 运行方式
读 `fixtures/input.json` → `loader` → `normalizer` → 其余处理阶段 → `keyer` → 幂等落 `sqlite`。
```text
make run     # 运行离线流程，输出摄取数量和落库行数
make test    # 运行项目测试，包括流水线和幂等性检查
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。
业务模块：`anomaly`, `schema_check`, `reconcile`, `quarantine`, `freshness`, `alert`。
通用模块：`loader / normalizer / keyer / store / pipeline / cli`。

## 模块说明
业务模块按独立阶段组织，通用模块负责加载、规范化、主键、存储和编排。
流水线按配置顺序调用处理阶段，输入输出均遵循项目定义的记录结构。

## 新鲜度阶段（`freshness.py`）
按来源配置（`src/freshnessgate/sources.json`）的发布周期、事件时间、接收时间与时区，
为每条记录输出 `_stage_freshness` 可解释状态，并把每源“最后有效更新”水位持久化到
SQLite 表 `freshness_watermark`。

- **事件时间 vs 接收时间**：过期按事件时间相对评估时钟推导；接收时间只用于传输延迟、
  迟到与时钟漂移判断，二者不可互换。
- **按周期定阈值**：过期阈值 = 发布周期 + 容忍（`tolerance`）。日更源（如 `1d + 2h`）
  与分钟级源（如 `1m + 30s`）阈值独立，绝不共用。
- **跨时区发布**：`timezone` + `expected_at` 把事件归位到源本地计划槽位；朴素时间戳按
  源时区本地化，带偏移时间戳按偏移解析，DST 切换日槽位自动随本地时区平移。
- **时钟漂移容忍**：事件时间小幅超前接收时间且在 `clock_skew` 带内，延迟按 0 处理并留痕；
  超出容忍标记 `clock_skew` 且拒绝推进水位。
- **补采/重复不冒充新数据**：事件时间不新于水位时标记 `backfill`/`duplicate`，
  **绝不刷新**最后更新时间；仅严格更新的事件时间推进水位，并记录 `updater_id` 证据。
- **迟到与过期可并存**：接收延迟超阈值置 `late` 标志（仍推进水位）；越过“所属槽位 +
  阈值”的具体时间边界才置 `stale`，证据含 `boundary_iso`/`overdue_seconds`，
  来源快照 `source_snapshot` 锚定首个缺失槽位定位过期边界。
- **稳定判定**：状态写入即提交（崩溃恢复后水位不回退）；重复运行、源配置变更
  （`rule_version`/配置签名留痕，水位不重置）、跨批次补采均产生可复现判定。
- **旧输入兼容**：无 `received_at` 时按事件时间兜底并留痕；未配置来源为 `unconfigured`，
  坏时间戳为 `invalid`，均不中断流水线。

时序样例见 `fixtures/timeseries.json`（跨时区、漂移、迟到、补采、重复批次）。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
环境缺少 pytest 时 `make test` 自动回退到测试文件内置的标准库 mini runner。

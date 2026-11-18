# freshnessgate — 监控采集源的新鲜度、schema 完整性、空值/重复/离群，异常告警隔离与对账。

运行栈：Python 3.11+ / 标准库 `sqlite3` + `pytest`（离线，无需联网）

## 运行方式
旧流水线（happy path，入口与输出保持不变）：读 `fixtures/input.json` →
`loader` → `normalizer` → 其余处理阶段 → `keyer` → 幂等落 `sqlite`。
```text
make run     # 运行离线流程，输出摄取数量和落库行数（{"fed": 4, "rows": 3}）
make test    # 运行项目测试，包括流水线、幂等性与端到端分步对账
```

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。
业务模块：`anomaly`, `schema_check`, `reconcile`, `quarantine`, `freshness`, `alert`。
通用模块：`loader / normalizer / keyer / store / pipeline / cli`，
对账扩展新增 `source`（分页源抽象）与 `ingest`（分页摄取与台账落账）。

## 对账模型（reconcile）
对账把四类账放在同一范围里算，报告恒等式必须平衡：

```text
源端声明的活 id == 范围内正式入库 + 隔离中(held) + 未采到(not_fetched)
                + 延迟(late) + 真正丢失(missing)
```

- 源批次/分页边界：`source_pages` 记录每页 `pending / fetched / failed`，
  页可见时间（`visible_at`）与批次宽限截止（`grace_deadline`）决定迟到状态。
- 正式记录：`records`；隔离记录：`quarantine`（`held`，释放后 `released` 并入正式表）。
- 差异五分类，原因码即 `alert` 消费的输入：

| reason | 含义 | 是否告警 |
| --- | --- | --- |
| `not_fetched` | 页未翻到（pending）或分页重试耗尽（failed），尚不能判丢失 | 否（已解释） |
| `quarantined` | 采到但在隔离区；释放入库后差异归档 `resolved` | 否（已解释） |
| `duplicate` | 同一 key 的额外 occurrence | 否（已解释） |
| `late` | 越过批次宽限仍未采到的迟到批次 | 否（已解释） |
| `missing` | 声明页已 fetched、台账却无该 key 的真正丢失 | 是（仅首次） |

### 两条归属约定
- **删除标记（tombstone）计源端**：删除是源端声明的事实，`declared_total`
  含该 id（状态 deleted，不计入 `expected_live`）；落库侧移除正式行、单独记
  `tombstones` 账，不会被误判成 `missing`。
- **跨批次重复计落库侧**：源端按声明 id 只算一次；重复是摄取侧的额外
  occurrence，记 `ingest_ledger`，正式表行数不变，差异归 `duplicate`。

### 幂等与不反复升级
- 摄取台账以 `batch|page|seq` 为确定性主键，进程退出重开后重放同一页不会重复计数；
  所有表写入均为 upsert/`INSERT OR IGNORE`。
- 每条差异有确定性指纹（reason+key+定位字段的 sha256，不含 run_id/时间），
  重跑只刷新 `last_seen`；历史差异消失则归档 `resolved`。
- `alert_log` 与指纹上的 `escalated` 标记保证同一故障只在首次出现时告警一次，
  已解释差异（未采到/隔离/重复/延迟）重跑永不升级。

## 持久化与测试
SQLite 表：`records / quarantine / ingest_ledger / tombstones /
source_pages / recon_runs / recon_findings / alert_log`。
`tests/test_reconcile.py` 按"未采到 → 隔离 → 释放 → 最终入库"分步执行，
穿插分页重试、进程退出再恢复、迟到批次、删除标记与跨批次重复，
逐步核对计数、恒等式与差异指纹。测试使用临时库，不污染工作区。

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

### anomaly — 采集质量异常检测（窗口级）
对单个数据源按窗口（默认按天）聚合五类质量指标，对照同源历史基线做稳健判定，
并把窗口指标、基线、摄入台账与告警指纹通过 `models.Record` + `store.upsert` 落库：

| 检查 | 指标 | 判定方式 |
| --- | --- | --- |
| `null_ratio` | 空值比例（配置字段集合） | 中位数 + MAD 稳健 z |
| `duplicate` | 重复记录率（内容指纹） | 中位数 + MAD 稳健 z |
| `outlier` | 数值离群占比（窗口内稳健 z） | 中位数 + MAD |
| `count` | 记录数突变 | **同星期几季节性基线**，周末低量不误报 |
| `delay` | 事件时间→观测时间延迟 | 中位数 + MAD 稳健 z |

四类判定：`no_baseline`（冷启动只积累不告警）、`seasonal`（季节性带内变化）、
`transient_fault`（异常窗口补采恢复后回改，告警解析）、`anomaly`（真实异常，指纹告警）。

明确的策略选择：

* **冷启动**：通用检查需 ≥3 个历史窗口；记录数检查需 ≥2 个同星期几样本，
  在此之前判 `no_baseline`——首个周末低量绝不告警；
* **异常样本不污染基线**：`anomaly` / `transient_fault` 窗口不进入下一窗口基线，
  仅正常与季节性窗口参与基线积累（摄入台账独立保存，重放仍幂等）；
* **不重复告警**：告警键为 `(src, window, check, 方向)` 的 sha256 指纹，
  同一异常重跑/重启只更新同一条告警；超过宽限窗口未恢复则置 `confirmed`；
* **分批/补采**：同一窗口多批摄入按记录指纹合并后重新聚合，跨分批判定一致；
  补采为权威重发，替换故障期残数据并触发恢复判定。

入口：`anomaly.evaluate(src, window, records, con=...)` 或 `AnomalyDetector(con)`；
`tests/test_anomaly.py` 离线构造季节变化、突发空值、恢复补采、分批、重启与重复运行场景。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。

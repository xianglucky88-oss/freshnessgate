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

## alert：质量异常 -> 可恢复告警
`alert.py` 把 quarantine/reconcile 上报的质量异常转成一条持续演变的告警，
三类数据在 SQLite 中分开保存，互不混淆：

| 表 | 保存内容 |
| --- | --- |
| `findings` | 异常指纹：每次来源上报（含 `source/source_ref/batch`），幂等去重 |
| `alerts` | 告警状态：`active/suppressed/acked/recovered`、级别、升级次数、恢复时间 |
| `notifications` | 通知确认：每次发送尝试、失败原因、退避重试、送达时间 |
| `alert_audit` | 状态迁移审计（open/repeat/escalate/acknowledge/suppress/recover…） |

- 去重指纹 `fp = sha256(src | rule | event_key)`：同一故障跨 quarantine/reconcile、
  跨重跑只对应一条告警；重放的旧批次只留痕不重开，新批次再次出现才重开。
- 生命周期：聚合窗口、抑制、升级、确认、恢复、通知失败指数退避重试（超限置 `exhausted`）。
- 优先级（高 -> 低）：**恢复 > 抑制 > 确认 > 升级 > 聚合窗口**。
- 双向追溯：`traceback(fp)` 从告警回查全部来源上报与通知；
  `find_alert_by_finding(source, source_ref, batch)` 从任一条上报反查告警。
- 阶段入口 `alert.process(record)` 仍是纯记录变换，无异常的正常链路原样透传；
  有状态能力由 `AlertManager` 提供，quarantine/reconcile 的结果通过记录上的
  `_quality_findings` 字段（`alert.extract_findings` 读取）交给 `ingest()` 接收。

## 持久化与测试
数据通过项目内 SQLite 存储，使用稳定键执行幂等写入；测试夹具位于项目测试目录，运行方式以 `Makefile` 为准。
离线环境若未安装 pytest，可用标准库 runner 运行同一份 alert 用例：
`python3 -I _stdlib_run_tests.py`。

"""采集质量异常检测（窗口级、单数据源）。

职责边界
========
本模块只负责"判定"与"证据留存"：把一批采集记录按窗口（默认按天）聚合为质量指标，
对照历史窗口基线做稳健判定，并把三类工件以项目现有模型（``models.Record`` +
``store.upsert``，稳定键幂等）落库：

1. 窗口工件 ``anomaly:window:{src}:{window}`` —— 窗口指标值、各检查判定与可解释证据；
2. 基线工件 ``anomaly:baseline:{src}``         —— 冷启动后逐步积累的同源历史基线；
3. 台账工件 ``anomaly:ingested:{src}``         —— 各窗口已摄入记录指纹（独立于基线，
   异常窗口被排除出基线也不丢重放幂等能力）；
4. 告警工件 ``anomaly:alert:{fingerprint}``    —— 异常指纹驱动，同一异常重跑/重启不重复告警。

五类检查
========
* ``null_ratio``   空值比例（配置字段集合内的空值占比）
* ``duplicate``    重复率（去重后重复记录占比，按记录内容指纹）
* ``outlier``      数值离群（窗口内 ``value`` 的稳健 z-score，中位数 + MAD）
* ``count``        记录数突变（带"同星期几"季节性基线，避免把周末低量误判为故障）
* ``delay``        时间延迟（事件时间到观测时间的延迟中位数/最大值，单位秒）

四类判定
========
* ``no_baseline``      首次/冷启动，历史不足，不告警，仅积累基线
* ``seasonal``         落在同星期几季节基线容忍带内的周期变化，不告警
* ``transient_fault``  来源短暂故障：异常窗口在补采恢复后被回改，告警解析
* ``anomaly``          真实异常：偏离基线且超过容忍带，按指纹告警

冷启动策略（明确选择）
======================
* 历史窗口数 < ``min_history`` 时，所有检查判 ``no_baseline``；
* 记录数检查需要至少 ``min_seasonal`` 个"同星期几"历史样本，否则同样判
  ``no_baseline``——周末低量在没有同类周末基线前，绝不告警；
* 判为 ``anomaly`` / ``transient_fault`` 的窗口 **不进入** 下一窗口基线（避免污染），
  只有正常窗口与首次冷启动窗口参与基线积累。

重跑 / 分批 / 重启
==================
* 摄入台账按"窗口 + 记录内容指纹"在 **窗口内** 去重：同一窗口分多批摄入会正确合并，
  而同一批重复运行/重启重放不会放大计数与重复率；不同窗口的相同内容互不影响；
* 所有工件键稳定，``upsert`` 覆盖，重跑结果幂等；
* 告警以 (src, window, check, metric, 异常方向) 的 sha256 摘要为指纹，
  同一异常重复评估只更新同一条告警，绝不产生第二条。
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Any, Iterable

from . import store as _store
from .models import Record

# ---------------------------------------------------------------- 参数（可在测试中覆盖）
MIN_HISTORY = 3            # 通用检查所需的最少历史窗口数
MIN_SEASONAL = 2           # 记录数检查所需的最少"同星期几"历史样本
Z_LIMIT = 3.5              # 稳健 z-score 离群阈值
DELTA_LIMIT = 3.5          # 基线偏离阈值（单位：稳健 MAD 标准差）
BACKFILL_GRACE = 1         # 短暂故障宽限窗口数：异常后 N 个窗口内恢复可回改为 transient_fault
DEFAULT_NULL_FIELDS = ("name", "value", "status", "ts")

NO_BASELINE = "no_baseline"
SEASONAL = "seasonal"
TRANSIENT_FAULT = "transient_fault"
ANOMALY = "anomaly"
OK = "ok"

_CHECK_LABELS = {
    "null_ratio": "空值比例",
    "duplicate": "重复率",
    "outlier": "数值离群",
    "count": "记录数突变",
    "delay": "时间延迟",
}


# ---------------------------------------------------------------- 纯函数：小工具
def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _record_fingerprint(rec: dict) -> str:
    """记录内容指纹（稳定序列化，忽略内部阶段字段）。"""
    payload = {k: v for k, v in rec.items() if not k.startswith("_")}
    return _sha(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def parse_ts(value: Any) -> datetime | None:
    """解析事件时间；支持 'Z' 结尾的 ISO8601，失败返回 None。"""
    if not value:
        return None
    try:
        s = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def day_window(ts: Any) -> str | None:
    """从事件时间派生按天窗口标识（UTC YYYY-MM-DD）。"""
    dt = parse_ts(ts)
    return dt.date().isoformat() if dt else None


def _median(xs: list[float]) -> float:
    s = sorted(xs)
    n = len(s)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2:
        return float(s[mid])
    return (s[mid - 1] + s[mid]) / 2.0


def _mad(xs: list[float], med: float) -> float:
    """中位绝对偏差；MAD=0（历史完全平稳）时退化为对中位数的平均绝对偏差。"""
    if not xs:
        return 0.0
    raw = _median([abs(x - med) for x in xs])
    if raw > 0:
        return raw
    return sum(abs(x - med) for x in xs) / len(xs)


def _robust_z(value: float, med: float, mad: float) -> float:
    if mad <= 0:
        return 0.0 if value == med else math.copysign(math.inf, value - med)
    return (value - med) / mad


def _is_null(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


# ---------------------------------------------------------------- 纯函数：窗口指标
def compute_metrics(records: list[dict], *, null_fields: Iterable[str] = DEFAULT_NULL_FIELDS,
                    observed_at: datetime | None = None,
                    prior_seen: set[str] | None = None) -> tuple[dict, list[str]]:
    """把一个窗口的记录聚合为质量指标。

    去重分两层，避免把"来源里的重复行"和"重放/重跑"混为一谈：

    * ``prior_seen`` 是该窗口历次运行已摄入的记录内容指纹台账——重放时这些记录
      整体跳过，保证重复运行/重启幂等（计数与重复率不被放大）；
    * 本次载荷内部的重复行 **照常计数**（total 计两次、unique 计一次），
      重复率本身就是要检测的质量指标。

    返回 ``(指标字典, 本次载荷的记录指纹列表)``，后者由调用方并入窗口台账。
    """
    observed_at = observed_at or datetime.now(timezone.utc)
    null_fields = tuple(null_fields)
    prior_seen = prior_seen or set()
    total = 0
    local_hashes: set[str] = set()
    null_cells = 0
    values: list[float] = []
    delays: list[float] = []
    payload_hashes: list[str] = []

    for rec in records:
        h = _record_fingerprint(rec)
        payload_hashes.append(h)
        if h in prior_seen:
            continue  # 该窗口已在此前运行中摄入：重放跳过
        total += 1
        local_hashes.add(h)
        for f in null_fields:
            if _is_null(rec.get(f)):
                null_cells += 1
        v = rec.get("value")
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            try:
                v = float(v)
            except (TypeError, ValueError):
                v = None
        if v is not None and not (isinstance(v, float) and math.isnan(v)):
            values.append(float(v))
        dt = parse_ts(rec.get("ts"))
        if dt is not None:
            delays.append(max(0.0, (observed_at - dt).total_seconds()))

    unique = len(local_hashes)
    cell_total = total * len(null_fields)
    med_v = _median(values) if values else 0.0
    mad_v = _mad(values, med_v) if values else 0.0
    outliers = [v for v in values if abs(_robust_z(v, med_v, mad_v)) > Z_LIMIT] if len(values) >= 4 else []
    metrics = {
        "total": total,
        "unique": unique,
        "duplicate_rate": round((total - unique) / total, 6) if total else 0.0,
        "null_ratio": round(null_cells / cell_total, 6) if cell_total else 0.0,
        "null_cells": null_cells,
        "value_median": round(med_v, 6),
        "outlier_count": len(outliers),
        "outlier_ratio": round(len(outliers) / total, 6) if total else 0.0,
        "outlier_values": [round(v, 6) for v in outliers][:10],
        "delay_median_s": round(_median(delays), 3) if delays else 0.0,
        "delay_max_s": round(max(delays), 3) if delays else 0.0,
    }
    return metrics, payload_hashes


# ---------------------------------------------------------------- 判定器
class AnomalyDetector:
    """有状态判定器：状态保存在 sqlite（生产/重启测试）或内存字典（单元测试）。"""

    def __init__(self, con=None, *, null_fields=DEFAULT_NULL_FIELDS, min_history=MIN_HISTORY,
                 min_seasonal=MIN_SEASONAL, delta_limit=DELTA_LIMIT,
                 backfill_grace=BACKFILL_GRACE):
        self.con = con
        self.mem: dict[str, dict] = {} if con is None else None
        self.null_fields = tuple(null_fields)
        self.min_history = min_history
        self.min_seasonal = min_seasonal
        self.delta_limit = delta_limit
        self.backfill_grace = backfill_grace

    # ---- 工件读写（经现有模型 Record + store.upsert 落库；内存模式用同构字典）
    def _load(self, key: str) -> dict | None:
        if self.con is not None:
            row = self.con.execute(
                "SELECT data FROM records WHERE key=?", (key,)).fetchone()
            return json.loads(row[0]) if row else None
        return self.mem.get(key)

    def _save(self, key: str, data: dict) -> None:
        data = dict(data)
        data["updated_at"] = datetime.now(timezone.utc).isoformat()
        if self.con is not None:
            _store.upsert(self.con, Record(key=key, data=data))
        else:
            self.mem[key] = data

    def _window_key(self, src, window): return f"anomaly:window:{src}:{window}"
    def _baseline_key(self, src): return f"anomaly:baseline:{src}"
    def _ingested_key(self, src): return f"anomaly:ingested:{src}"
    def _alert_key(self, fp): return f"anomaly:alert:{fp}"

    def get_window(self, src, window) -> dict | None:
        return self._load(self._window_key(src, window))

    def get_baseline(self, src) -> dict | None:
        return self._load(self._baseline_key(src))

    def get_ingested(self, src) -> dict | None:
        return self._load(self._ingested_key(src))

    def list_alerts(self, src=None) -> list[dict]:
        if self.con is not None:
            rows = self.con.execute(
                "SELECT data FROM records WHERE key LIKE 'anomaly:alert:%'").fetchall()
            alerts = [json.loads(r[0]) for r in rows]
        else:
            alerts = [d for k, d in self.mem.items() if k.startswith("anomaly:alert:")]
        if src:
            alerts = [a for a in alerts if a.get("src") == src]
        return sorted(alerts, key=lambda a: (a["window"], a["check"]))

    # ---- 基线
    @staticmethod
    def _series(baseline: dict, check: str) -> list[float]:
        return [w["metrics"][check] for w in baseline["windows"] if check in w["metrics"]]

    def _judge_against(self, check, value, series, direction, unit):
        """通用稳健基线判定。"""
        if len(series) < self.min_history:
            return NO_BASELINE, {
                "reason": f"历史窗口不足（{len(series)}/{self.min_history}），冷启动只积累基线不判定",
                "history_n": len(series), "value": value}
        med = _median(series)
        mad = _mad(series, med)
        z = _robust_z(value, med, mad)
        evidence = {
            "value": value, "baseline_median": round(med, 6),
            "baseline_mad": round(mad, 6), "robust_z": None if math.isinf(z) else round(z, 4),
            "direction": direction, "unit": unit,
            "history_n": len(series), "history": [round(x, 6) for x in series[-10:]],
            "threshold_z": self.delta_limit,
        }
        if mad <= 0 and value != med:
            evidence["reason"] = (
                f"{_CHECK_LABELS[check]}历史完全平稳（MAD=0，恒定为 {round(med, 6)}"
                f"{unit}），当前值 {value}{unit} 偏离历史恒定水平（方向：{direction}）")
            return ANOMALY, evidence
        if abs(z) <= self.delta_limit:
            evidence["reason"] = f"稳健偏离 {evidence['robust_z']} 未超过阈值 ±{self.delta_limit}"
            return OK, evidence
        evidence["reason"] = (
            f"{_CHECK_LABELS[check]}当前值 {value}{unit}，基线中位数 {round(med, 6)}{unit}，"
            f"稳健 z={evidence['robust_z']} 超过 ±{self.delta_limit}（方向：{direction}）")
        return ANOMALY, evidence

    def _judge_count_seasonal(self, value, baseline, weekday):
        """记录数判定：优先同星期几季节性基线。"""
        same_dow = [w for w in baseline["windows"] if w.get("weekday") == weekday]
        series_all = self._series(baseline, "total")
        if len(same_dow) < self.min_seasonal:
            return NO_BASELINE, {
                "reason": (f"同星期几（weekday={weekday}）历史样本不足"
                           f"（{len(same_dow)}/{self.min_seasonal}），季节性冷启动不告警，"
                           "避免把首个周末低量误判为故障"),
                "seasonal_n": len(same_dow), "overall_n": len(series_all),
                "value": value, "weekday": weekday}
        series = [w["metrics"]["total"] for w in same_dow]
        med = _median(series)
        mad = _mad(series, med)
        z = _robust_z(value, med, mad)
        evidence = {
            "value": value, "season": "same_weekday", "weekday": weekday,
            "seasonal_median": round(med, 6), "seasonal_mad": round(mad, 6),
            "robust_z": None if math.isinf(z) else round(z, 4),
            "seasonal_n": len(series),
            "seasonal_history": [round(x, 6) for x in series[-10:]],
            "overall_median": round(_median(series_all), 6) if series_all else None,
            "threshold_z": self.delta_limit, "unit": "条",
        }
        if mad <= 0 and value != med:
            evidence["reason"] = "同星期几历史完全平稳，当前记录数偏离季节性水平"
            evidence["direction"] = "突降" if value < med else "突增"
            return ANOMALY, evidence
        if abs(z) <= self.delta_limit:
            evidence["reason"] = (
                f"记录数 {value} 条处于同星期几季节性带内（中位数 {round(med, 6)}，"
                f"稳健 z={evidence['robust_z']}），属于季节性变化而非故障")
            return SEASONAL, evidence
        direction = "突降" if value < med else "突增"
        evidence["direction"] = direction
        evidence["reason"] = (
            f"记录数{direction}：当前 {value} 条，同星期几中位数 {round(med, 6)} 条，"
            f"稳健 z={evidence['robust_z']} 超过 ±{self.delta_limit}")
        return ANOMALY, evidence

    def _judge_window(self, metrics, baseline, weekday=None) -> dict:
        """对五类检查逐项判定，返回 {check: {verdict, evidence, ...}}。"""
        checks: dict[str, dict] = {}
        if baseline is None:
            for check, val in (
                    ("count", metrics["total"]), ("null_ratio", metrics["null_ratio"]),
                    ("duplicate", metrics["duplicate_rate"]),
                    ("outlier", metrics["outlier_ratio"]),
                    ("delay", metrics["delay_median_s"])):
                checks[check] = {"verdict": NO_BASELINE,
                                 "evidence": {"reason": "数据源首次出现，无任何历史基线",
                                              "value": val}}
            return checks

        checks["count"] = dict(zip(
            ("verdict", "evidence"),
            self._judge_count_seasonal(metrics["total"], baseline, weekday)))
        checks["null_ratio"] = dict(zip(
            ("verdict", "evidence"),
            self._judge_against("null_ratio", metrics["null_ratio"],
                                self._series(baseline, "null_ratio"), "升高", "")))
        checks["duplicate"] = dict(zip(
            ("verdict", "evidence"),
            self._judge_against("duplicate", metrics["duplicate_rate"],
                                self._series(baseline, "duplicate_rate"), "升高", "")))
        checks["outlier"] = dict(zip(
            ("verdict", "evidence"),
            self._judge_against("outlier", metrics["outlier_ratio"],
                                self._series(baseline, "outlier_ratio"), "升高", "")))
        checks["delay"] = dict(zip(
            ("verdict", "evidence"),
            self._judge_against("delay", metrics["delay_median_s"],
                                self._series(baseline, "delay_median_s"), "升高", "s")))
        return checks

    # ---- 告警（指纹去重）
    @staticmethod
    def fingerprint(src, window, check, evidence) -> str:
        direction = evidence.get("direction") or (
            "突降" if check == "count" else "升高")
        return _sha(f"{src}|{window}|{check}|{direction}")[:16]

    def _upsert_alert(self, src, window, check, verdict, evidence):
        fp = self.fingerprint(src, window, check, evidence)
        key = self._alert_key(fp)
        existing = self._load(key)
        alert = existing or {
            "fingerprint": fp, "src": src, "window": window, "check": check,
            "kind": _CHECK_LABELS[check], "status": "open",
            "first_seen": datetime.now(timezone.utc).isoformat(),
            "history": []}
        alert["last_seen"] = datetime.now(timezone.utc).isoformat()
        alert["last_verdict"] = verdict
        alert["evidence"] = evidence
        alert["history"].append({"at": alert["last_seen"], "verdict": verdict,
                                 "value": evidence.get("value")})
        self._save(key, alert)
        return alert, existing is None

    def _resolve_alert(self, src, window, check, resolution, evidence):
        """恢复补采：找到该窗口该检查的未解析告警并标记恢复（不新建告警）。"""
        fp = self.fingerprint(src, window, check, evidence)
        key = self._alert_key(fp)
        alert = self._load(key)
        if alert is None or alert["status"] != "open":
            # 恢复后的证据可能不再带异常方向，回退到 (src, window, check) 定位同一告警
            for cand in self.list_alerts(src):
                if (cand["window"] == window and cand["check"] == check
                        and cand["status"] == "open"):
                    alert = cand
                    key = self._alert_key(cand["fingerprint"])
                    break
        if alert is None or alert["status"] != "open":
            return None
        alert["status"] = "resolved"
        alert["resolution"] = resolution
        alert["resolved_at"] = datetime.now(timezone.utc).isoformat()
        alert["resolution_evidence"] = evidence
        self._save(key, alert)
        return alert

    # ---- 主入口
    def evaluate(self, src, window, records, *, batch_id=None, observed_at=None,
                 backfill=False) -> dict:
        """评估一个数据源的一个窗口，落库窗口/基线/告警工件并返回窗口工件。"""
        src = str(src).strip().lower()
        observed_at = observed_at or datetime.now(timezone.utc)
        baseline = self._load(self._baseline_key(src))
        ledger_art = self._load(self._ingested_key(src)) or {"src": src, "windows": {}}
        ingested: dict[str, list[str]] = dict(ledger_art.get("windows", {}))
        prior_seen = set(ingested.get(window, []))

        metrics, payload_hashes = compute_metrics(
            records, null_fields=self.null_fields, observed_at=observed_at,
            prior_seen=prior_seen)

        # 重放/重启：载荷全部已在该窗口历史运行中摄入 → 直接回显既有窗口工件，
        # 不重新判定、不重复告警（重复运行幂等的核心保证）。空载荷不触发该短路。
        prior = self._load(self._window_key(src, window))
        if records and metrics["total"] == 0 and prior is not None:
            return prior
        if not records and prior is not None:
            return prior

        # 窗口台账并入本次指纹（跨批次合并、跨重跑去重）。
        # 补采是权威重发：以补采载荷指纹替换故障期残数据指纹，避免脏指纹长期滞留。
        if backfill:
            window_ledger = sorted(set(payload_hashes))
        else:
            window_ledger = sorted(prior_seen | set(payload_hashes))
        ingested[window] = window_ledger

        # 分批摄入：合并该窗口此前批次的原始记录后再聚合，保证指标与判定跨分批一致。
        # 补采是来源对故障窗口的权威重发：以补采载荷替换故障期残数据，不做合并。
        if backfill:
            window_records = list(records)
        else:
            window_records = list(prior.get("records", [])) if prior else []
            window_records.extend(records)
        metrics, _ = compute_metrics(
            window_records, null_fields=self.null_fields,
            observed_at=observed_at, prior_seen=set())

        weekday = None
        try:
            weekday = datetime.fromisoformat(window).isoweekday()
        except ValueError:
            pass

        checks = self._judge_window(metrics, baseline, weekday)

        # 整体判定优先级：真实异常 > 短暂故障 > 季节性 > 无基线 > 正常
        verdicts = {c: d["verdict"] for c, d in checks.items()}
        if any(v == ANOMALY for v in verdicts.values()):
            overall = ANOMALY
        elif any(v == TRANSIENT_FAULT for v in verdicts.values()):
            overall = TRANSIENT_FAULT
        elif any(v == SEASONAL for v in verdicts.values()):
            overall = SEASONAL
        elif all(v == NO_BASELINE for v in verdicts.values()):
            overall = NO_BASELINE
        else:
            overall = OK

        artifact = {
            "src": src, "window": window, "weekday": weekday,
            "batch_id": batch_id, "backfill": bool(backfill),
            "observed_at": observed_at.isoformat(), "metrics": metrics,
            "checks": checks, "verdict": overall,
            "ingested": len(records),
            # 保留窗口内累计原始记录，供后续批次/补采重新聚合（跨分批指标一致）
            "records": window_records,
        }

        new_alerts, resolved_alerts = [], []
        prior_verdict = prior.get("verdict") if prior else None

        # 1) 异常检查 → 指纹告警（同指纹重跑只更新，不新增）
        for check, item in checks.items():
            if item["verdict"] == ANOMALY:
                alert, is_new = self._upsert_alert(
                    src, window, check, ANOMALY, item["evidence"])
                if is_new:
                    new_alerts.append(alert["fingerprint"])
                artifact.setdefault("alert_fingerprints", [])
                if alert["fingerprint"] not in artifact["alert_fingerprints"]:
                    artifact["alert_fingerprints"].append(alert["fingerprint"])

        # 2) 补采恢复：异常窗口重新评估且指标回到基线 → 回改 transient_fault 并解析告警。
        #    部分检查仍异常时整体保持 anomaly，仅解析已恢复检查的告警。
        if backfill and prior_verdict == ANOMALY:
            any_still_anomaly = False
            any_recovered = False
            for check, item in checks.items():
                if prior and prior["checks"].get(check, {}).get("verdict") == ANOMALY:
                    if item["verdict"] == ANOMALY:
                        any_still_anomaly = True
                    else:
                        any_recovered = True
                        ev = dict(item["evidence"])
                        ev["recovery_value"] = ev.get("value")
                        resolved = self._resolve_alert(
                            src, window, check, "backfill_recovered", ev)
                        if resolved:
                            resolved_alerts.append(resolved["fingerprint"])
                            item["verdict"] = TRANSIENT_FAULT
                            item.setdefault("evidence", {})["resolution"] = (
                                "补采后该检查回到基线，判定为来源短暂故障")
            if any_recovered and not any_still_anomaly:
                overall = TRANSIENT_FAULT
                artifact["verdict"] = overall
            elif any_still_anomaly:
                artifact["verdict"] = ANOMALY

        # 3) 宽限期扫描：超过宽限窗口仍未恢复的异常确认为真实异常（状态留痕）
        self._sweep_grace(src, window, baseline)

        # 4) 基线积累：仅正常/季节性/冷启动窗口入基线；异常与短暂故障窗口排除。
        #    摄入台账独立落库：异常窗口被排除出基线也保留指纹，重放依旧幂等。
        self._update_baseline(baseline, src, window, weekday, metrics,
                              artifact["verdict"])
        ledger_art["windows"] = dict(sorted(ingested.items()))
        ledger_art["last_window"] = window
        self._save(self._ingested_key(src), ledger_art)

        artifact["new_alert_fingerprints"] = new_alerts
        artifact["resolved_alert_fingerprints"] = resolved_alerts
        self._save(self._window_key(src, window), artifact)
        if self.con is not None:
            self.con.commit()
        return artifact

    def _sweep_grace(self, src, current_window, baseline):
        if baseline is None:
            return
        ordered = sorted(w["window"] for w in baseline["windows"])
        if current_window not in ordered:
            ordered = sorted(set(ordered) | {current_window})
        try:
            idx = ordered.index(current_window)
        except ValueError:
            return
        grace_boundary = set(ordered[max(0, idx - self.backfill_grace):idx])
        for alert in self.list_alerts(src):
            if alert["status"] != "open" or alert["window"] in grace_boundary \
                    or alert["window"] >= current_window:
                continue
            win = self.get_window(src, alert["window"])
            if win and win.get("verdict") == ANOMALY:
                alert["status"] = "confirmed"
                alert["confirmation"] = (
                    f"异常后 {self.backfill_grace} 个窗口内未见补采恢复，确认为真实异常")
                alert["confirmed_at"] = datetime.now(timezone.utc).isoformat()
                self._save(self._alert_key(alert["fingerprint"]), alert)

    def _update_baseline(self, baseline, src, window, weekday, metrics, verdict):
        excluded = verdict in (ANOMALY, TRANSIENT_FAULT)
        entry = {"window": window, "weekday": weekday,
                 "metrics": {k: metrics[k] for k in (
                     "total", "null_ratio", "duplicate_rate", "outlier_ratio",
                     "delay_median_s")},
                 "included": not excluded,
                 "exclusion_reason": None if not excluded else
                 ("异常窗口不参与下一窗口基线，防止污染" if verdict == ANOMALY
                  else "短暂故障窗口不参与基线，待恢复后由正常窗口重新积累")}
        if baseline is None:
            baseline = {"src": src, "windows": []}
        baseline["windows"] = [w for w in baseline["windows"] if w["window"] != window]
        if not excluded:
            baseline["windows"].append(entry)
        baseline["windows"].sort(key=lambda w: w["window"])
        baseline["last_window"] = window
        self._save(self._baseline_key(src), baseline)


# ---------------------------------------------------------------- 模块级便捷入口
def evaluate(src, window, records, *, con=None, **kw) -> dict:
    """模块级便捷函数：``anomaly.evaluate(...)``。"""
    return AnomalyDetector(con).evaluate(src, window, records, **kw)


def fingerprint(src, window, check, evidence) -> str:
    """暴露指纹算法，供后续告警模块复用。"""
    return AnomalyDetector.fingerprint(src, window, check, evidence)


def process(record):
    """流水线阶段占位：异常检测是窗口级聚合，不参与逐记录链式变换。

    保留与其他阶段同构的 ``process`` 接口（透传并打标），窗口判定请用
    :func:`evaluate` / :class:`AnomalyDetector`。
    """
    rec = dict(record)
    rec.setdefault("_stage_anomaly", "windowed-elsewhere")
    return rec

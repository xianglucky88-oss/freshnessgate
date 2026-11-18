"""单一职责：采集源新鲜度计算（流水线阶段）。

输入：单条记录（含来源 ``src``、事件时间字段、可选接收时间字段）+ 来源配置
（发布周期、容忍度、时钟漂移容忍、时区、计划发布时刻、规则版本）。
输出：在记录上挂 ``_stage_freshness`` 状态字典，向质量链路提供可解释的新鲜度判定，
并把每个来源的“最后有效更新”水位持久化到 SQLite，保证：

* 事件时间（event time）与实际接收时间（received time）分开使用：
  过期按事件时间相对当前时间推导，接收时间只用于度量传输延迟/迟到；
* 每天发布与每分钟发布的来源按各自周期计算阈值，绝不共用过期阈值；
* 补采（backfill）与重复批次不会刷新来源的最后更新时间；
* 容忍源端时钟漂移（事件时间小幅超前按 0 延迟处理并留痕）；
* 重复运行、源配置变更（规则切换）、跨批次补采均产生稳定、可复现的判定；
* 诊断证据可定位造成过期的具体时间边界（计划槽位 + 容忍边界）。
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone as _tz
from pathlib import Path
from zoneinfo import ZoneInfo

from .config import DB_PATH

_CONFIG_PATH = Path(__file__).with_name("sources.json")
_EPOCH = datetime(1970, 1, 1, tzinfo=_tz.utc)
_PERIOD_RE = re.compile(r"(\d+)\s*(ms|s|m|h|d)")
_UNIT_SECONDS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


# --------------------------------------------------------------------------- #
# 时间与周期解析
# --------------------------------------------------------------------------- #
def now_utc() -> datetime:
    """当前 UTC 时间（带时区，便于测试注入时钟）。"""
    return datetime.now(_tz.utc)


def parse_ts(value, assumed_tz: str = "UTC"):
    """解析 ISO-8601 时间字符串。

    带偏移的时间戳按其偏移解析；朴素时间戳（无偏移）按 ``assumed_tz`` 本地化，
    以支持跨时区发布源。返回带时区的 UTC ``datetime``；无法解析时返回 ``None``。
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(assumed_tz))
    return dt.astimezone(_tz.utc)


def iso(dt) -> str | None:
    """统一输出 UTC ISO-8601（``Z`` 后缀），保证证据 JSON 安全、可复现。"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_tz.utc)
    return dt.astimezone(_tz.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_period(value) -> float:
    """周期/时长解析：数字按秒；字符串支持 ``30s``/``5m``/``1h``/``1d`` 及组合。"""
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().lower()
    if text.isdigit():
        return float(text)
    total = 0.0
    matched = 0
    for num, unit in _PERIOD_RE.findall(text):
        total += int(num) * _UNIT_SECONDS[unit]
        matched += 1
    if not matched:
        raise ValueError(f"无法解析的周期表达式: {value!r}")
    return total


def parse_hhmm(value) -> tuple[int, int, int] | None:
    """解析 ``HH:MM[:SS]`` 计划发布时刻。"""
    if not value:
        return None
    parts = str(value).strip().split(":")
    if not 2 <= len(parts) <= 3:
        return None
    try:
        hh, mm = int(parts[0]), int(parts[1])
        ss = int(parts[2]) if len(parts) == 3 else 0
    except ValueError:
        return None
    if not (0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60):
        return None
    return hh, mm, ss


# --------------------------------------------------------------------------- #
# 来源配置
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SourceConfig:
    """单个采集源的新鲜度规则。"""

    name: str
    period_seconds: float
    tolerance_seconds: float
    clock_skew_seconds: float
    timezone: str = "UTC"
    expected_at: tuple[int, int, int] | None = None
    event_time_field: str = "ts"
    received_at_field: str | None = "received_at"
    rule_version: str = "v1"

    @property
    def threshold_seconds(self) -> float:
        """过期阈值 = 发布周期 + 迟到容忍；日更与分钟级来源因此天然不同。"""
        return self.period_seconds + self.tolerance_seconds

    @classmethod
    def from_dict(cls, name: str, raw: dict) -> "SourceConfig":
        hhmm = parse_hhmm(raw.get("expected_at"))
        return cls(
            name=name,
            period_seconds=parse_period(raw["period"]),
            tolerance_seconds=parse_period(raw.get("tolerance", 0)),
            clock_skew_seconds=parse_period(raw.get("clock_skew", 0)),
            timezone=raw.get("timezone", "UTC"),
            expected_at=hhmm,
            event_time_field=raw.get("event_time_field", "ts"),
            received_at_field=raw.get("received_at_field", "received_at"),
            rule_version=str(raw.get("rule_version", "v1")),
        )

    def signature(self) -> str:
        """配置签名：用于检测源配置变更（规则切换），不参与水位重置。"""
        payload = json.dumps(
            {
                "period": self.period_seconds,
                "tolerance": self.tolerance_seconds,
                "clock_skew": self.clock_skew_seconds,
                "timezone": self.timezone,
                "expected_at": self.expected_at,
                "event_time_field": self.event_time_field,
                "received_at_field": self.received_at_field,
                "rule_version": self.rule_version,
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]


def load_sources(path: str | Path | None = None) -> dict[str, SourceConfig]:
    """从 JSON 加载来源配置；文件缺失时返回空表（旧输入兼容：状态为 unconfigured）。"""
    path = Path(path) if path else _CONFIG_PATH
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    items = raw.get("sources", raw)
    return {name: SourceConfig.from_dict(name, cfg) for name, cfg in items.items()}


# --------------------------------------------------------------------------- #
# 水位状态（SQLite 持久化，崩溃恢复后仍可判定补采/重复）
# --------------------------------------------------------------------------- #
class _WatermarkStore:
    """每个来源只保存“最新有效事件时间”的水位；补采/重复不允许覆盖。"""

    def __init__(self, db_path: str | Path | None = None, con: sqlite3.Connection | None = None):
        self._owns_con = con is None
        if con is None:
            con = sqlite3.connect(str(db_path), timeout=30)
            # 独立使用（测试/诊断）时启用 WAL，避免与外部连接互锁。
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA busy_timeout=30000")
        self._con = con
        self._con.execute(
            "CREATE TABLE IF NOT EXISTS freshness_watermark("
            "src TEXT PRIMARY KEY, event_time TEXT NOT NULL, received_at TEXT, "
            "updater_id TEXT, updated_at TEXT NOT NULL, rule_version TEXT, source_sig TEXT)"
        )
        self._con.commit()
        self._cache: dict[str, dict] = {}

    def get(self, src: str):
        if src in self._cache:
            return self._cache[src]
        row = self._con.execute(
            "SELECT event_time, received_at, updater_id, updated_at, rule_version, source_sig "
            "FROM freshness_watermark WHERE src=?",
            (src,),
        ).fetchone()
        state = None
        if row:
            state = {
                "event_time": row[0],
                "received_at": row[1],
                "updater_id": row[2],
                "updated_at": row[3],
                "rule_version": row[4],
                "source_sig": row[5],
            }
        self._cache[src] = state
        return state

    def advance(self, src: str, event_time: datetime, received_at: datetime,
                updater_id: str, rule_version: str, source_sig: str) -> None:
        """仅由更新的事件时间驱动；写入即提交，保证中断恢复后水位不回退。"""
        updated = now_utc()
        self._con.execute(
            "INSERT INTO freshness_watermark(src,event_time,received_at,updater_id,"
            "updated_at,rule_version,source_sig) VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(src) DO UPDATE SET event_time=excluded.event_time, "
            "received_at=excluded.received_at, updater_id=excluded.updater_id, "
            "updated_at=excluded.updated_at, rule_version=excluded.rule_version, "
            "source_sig=excluded.source_sig",
            (src, iso(event_time), iso(received_at), updater_id,
             iso(updated), rule_version, source_sig),
        )
        self._con.commit()
        self._cache[src] = {
            "event_time": iso(event_time),
            "received_at": iso(received_at),
            "updater_id": updater_id,
            "updated_at": iso(updated),
            "rule_version": rule_version,
            "source_sig": source_sig,
        }

    def close(self) -> None:
        if self._owns_con:
            self._con.close()


# --------------------------------------------------------------------------- #
# 新鲜度评估器
# --------------------------------------------------------------------------- #
class FreshnessEvaluator:
    """有状态的新鲜度评估器；状态落在 SQLite，重复运行与跨批次判定稳定。"""

    def __init__(self, sources: dict[str, SourceConfig] | None = None,
                 db_path: str | Path = DB_PATH, clock=now_utc,
                 con: sqlite3.Connection | None = None):
        self.sources = sources if sources is not None else load_sources()
        self._store = _WatermarkStore(db_path, con=con)
        self._clock = clock

    def close(self) -> None:
        self._store.close()

    def process(self, record: dict, now: datetime | None = None) -> dict:
        """流水线阶段接口：在记录上挂可解释的 ``_stage_freshness`` 状态。"""
        rec = dict(record)
        rec["_stage_freshness"] = self.evaluate(rec, now=now)
        return rec

    # -- 计划槽位（时区感知） ------------------------------------------------ #
    def _slot_index(self, cfg: SourceConfig, dt: datetime) -> int:
        """dt 所在发布槽位的序号；序号差即可数缺口，与纪元对齐无关。"""
        if cfg.expected_at is not None and cfg.period_seconds % 86400 == 0:
            local = dt.astimezone(ZoneInfo(cfg.timezone))
            hh, mm, ss = cfg.expected_at
            slot_local = local.replace(hour=hh, minute=mm, second=ss, microsecond=0)
            if slot_local > local:
                slot_local -= timedelta(days=1)
            period_days = int(cfg.period_seconds // 86400)
            return slot_local.toordinal() // period_days - _EPOCH.toordinal() // period_days
        return int((dt - _EPOCH).total_seconds() // cfg.period_seconds)

    def _slot_start(self, cfg: SourceConfig, index: int) -> datetime:
        """槽位序号 → 该槽位的计划发布时刻（UTC）。"""
        if cfg.expected_at is not None and cfg.period_seconds % 86400 == 0:
            hh, mm, ss = cfg.expected_at
            period_days = int(cfg.period_seconds // 86400)
            ordinal = _EPOCH.toordinal() + index * period_days
            local = datetime.fromordinal(ordinal).replace(
                hour=hh, minute=mm, second=ss, tzinfo=ZoneInfo(cfg.timezone))
            return local.astimezone(_tz.utc)
        return _EPOCH + timedelta(seconds=index * cfg.period_seconds)

    # -- 单条记录判定 -------------------------------------------------------- #
    def evaluate(self, record: dict, now: datetime | None = None) -> dict:
        """评估一条记录，返回挂在 ``_stage_freshness`` 上的状态字典。"""
        now = now or self._clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=_tz.utc)
        now = now.astimezone(_tz.utc)
        rec = dict(record)
        src_raw = str(rec.get("src", "")).strip().lower()

        cfg = self.sources.get(src_raw) or self.sources.get(str(rec.get("src", "")).strip())
        if cfg is None:
            return self._base(rec, src_raw, now, "unconfigured",
                              ["来源未配置新鲜度规则，无法推导过期阈值"])

        reasons: list[str] = []
        et_field = cfg.event_time_field
        event_time = parse_ts(rec.get(et_field), cfg.timezone)
        if event_time is None:
            return self._base(rec, cfg.name, now, "invalid",
                              [f"事件时间字段 {et_field!r} 缺失或无法解析"], cfg)

        # 旧输入兼容：没有接收时间时退化为事件时间，并在证据中留痕。
        ra_field = cfg.received_at_field
        raw_received = rec.get(ra_field) if ra_field else None
        if raw_received in (None, ""):
            received_at = event_time
            reasons.append("接收时间缺失，按事件时间兜底（旧输入兼容）")
        else:
            received_at = parse_ts(raw_received, cfg.timezone)
            if received_at is None:
                return self._base(rec, cfg.name, now, "invalid",
                                  [f"接收时间字段 {ra_field!r} 无法解析"], cfg)

        latency = (received_at - event_time).total_seconds()
        observed_delay = latency
        state = self._store.get(cfg.name)
        watermark_before = parse_ts(state["event_time"]) if state else None

        evidence = self._base(rec, cfg.name, now, "fresh", reasons, cfg)
        evidence.update({
            "event_time_iso": iso(event_time),
            "received_at_iso": iso(received_at),
            "latency_seconds": latency,
            "observed_delay_seconds": observed_delay,
            "effective_age_seconds": (now - event_time).total_seconds(),
            "threshold_seconds": cfg.threshold_seconds,
            "watermark_before": iso(watermark_before),
            "watermark_after": iso(watermark_before),
            "advanced": False,
            "updater_id": state["updater_id"] if state else None,
            "gap_seconds": None,
            "missed_slots": 0,
            "boundary_iso": None,
            "overdue_seconds": None,
            "config_changed": bool(state and state.get("source_sig") != cfg.signature()),
            "rule_switched": bool(state and state.get("rule_version") != cfg.rule_version),
        })

        # 1) 源端时钟漂移：事件时间超前接收时间超过漂移容忍 → 不可信，不推进水位。
        if latency < -cfg.clock_skew_seconds:
            evidence["status"] = "clock_skew"
            evidence["reasons"].append(
                f"事件时间超前接收时间 {abs(latency):.0f}s，超过时钟漂移容忍 "
                f"{cfg.clock_skew_seconds:.0f}s，拒绝推进水位")
            return evidence
        if latency < 0:
            evidence["latency_seconds"] = 0.0
            evidence["reasons"].append(
                f"事件时间小幅超前 {abs(latency):.0f}s，处于时钟漂移容忍 "
                f"{cfg.clock_skew_seconds:.0f}s 内，延迟按 0 处理")

        # 2) 补采 / 重复批次：事件时间不新于水位 → 绝不刷新最后更新时间。
        if watermark_before is not None and event_time <= watermark_before:
            if event_time == watermark_before:
                evidence["status"] = "duplicate"
                evidence["reasons"].append(
                    "事件时间等于已见水位，判定为重复批次，最后更新时间保持不变")
            else:
                evidence["status"] = "backfill"
                evidence["reasons"].append(
                    "事件时间早于已见水位，判定为跨批次补采历史数据，"
                    "不得冒充刚发布的新数据，最后更新时间保持不变")
            evidence["gap_seconds"] = (watermark_before - event_time).total_seconds()
            evidence["missed_slots"] = max(
                0, self._slot_index(cfg, watermark_before) - self._slot_index(cfg, event_time))
            return evidence

        # 3) 新事件：计算与上一有效事件之间的缺口。
        if watermark_before is not None:
            evidence["gap_seconds"] = (event_time - watermark_before).total_seconds()
            evidence["missed_slots"] = max(
                0, self._slot_index(cfg, event_time)
                - self._slot_index(cfg, watermark_before) - 1)
            if evidence["missed_slots"]:
                evidence["reasons"].append(
                    f"与上一有效事件之间缺失 {evidence['missed_slots']} 个发布槽位")

        # 4) 迟到数据：传输延迟本身超过周期+容忍（但事件仍是最新事件）。
        #    late 是独立标志，可与 stale 并存，状态优先级不互相覆盖。
        evidence["late"] = observed_delay > cfg.threshold_seconds
        if evidence["late"]:
            evidence["reasons"].append(
                f"接收延迟 {observed_delay:.0f}s 超过阈值 {cfg.threshold_seconds:.0f}s，"
                "数据迟到但仍为最新事件，水位照常推进")

        # 5) 过期判定：事件年龄越过“所属槽位 + 周期 + 容忍”的具体时间边界。
        slot_idx = self._slot_index(cfg, event_time)
        boundary = self._slot_start(cfg, slot_idx) + timedelta(seconds=cfg.threshold_seconds)
        evidence["boundary_iso"] = iso(boundary)
        if now > boundary:
            evidence["status"] = "stale"
            evidence["overdue_seconds"] = (now - boundary).total_seconds()
            evidence["reasons"].append(
                f"当前时间已越过过期边界 {iso(boundary)}（计划槽位 "
                f"{iso(self._slot_start(cfg, slot_idx))} + 容忍 "
                f"{cfg.tolerance_seconds:.0f}s），数据过期 "
                f"{evidence['overdue_seconds']:.0f}s")

        # 6) 仅最新事件时间推进水位，并记录是谁推进的（可定位证据）。
        self._store.advance(cfg.name, event_time, received_at,
                            str(rec.get("id")), cfg.rule_version, cfg.signature())
        evidence["watermark_after"] = iso(event_time)
        evidence["advanced"] = True
        evidence["updater_id"] = str(rec.get("id"))
        return evidence

    # -- 来源级快照（诊断过期来源） ------------------------------------------ #
    def source_snapshot(self, src: str, now: datetime | None = None) -> dict:
        """返回来源级新鲜度快照，证据可定位造成过期的具体槽位与时间边界。"""
        now = now or self._clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=_tz.utc)
        now = now.astimezone(_tz.utc)
        cfg = self.sources.get(str(src).strip().lower())
        if cfg is None:
            return {"source": src, "status": "unconfigured", "reasons": ["来源未配置"]}
        state = self._store.get(cfg.name)
        if state is None:
            idx = self._slot_index(cfg, now)
            return {
                "source": cfg.name, "status": "no_data",
                "reasons": ["尚未接收到任何有效事件"],
                "timezone": cfg.timezone, "rule_version": cfg.rule_version,
                "current_slot_iso": iso(self._slot_start(cfg, idx)),
                "boundary_iso": iso(self._slot_start(cfg, idx)
                                    + timedelta(seconds=cfg.threshold_seconds)),
            }
        watermark = parse_ts(state["event_time"])
        cur_idx = self._slot_index(cfg, now)
        wm_idx = self._slot_index(cfg, watermark)
        missed = max(0, cur_idx - wm_idx - 1)
        # 过期判定锚定“第一个缺失槽位”的边界：该槽计划时刻 + 阈值已过即过期。
        # 当前槽在容忍窗内允许数据未到，不参与过期判定。
        first_missing_idx = wm_idx + 1
        slot = self._slot_start(cfg, first_missing_idx)
        boundary = slot + timedelta(seconds=cfg.threshold_seconds)
        stale = missed > 0 and now > boundary
        cur_slot = self._slot_start(cfg, cur_idx)
        return {
            "source": cfg.name,
            "status": "stale" if stale else "fresh",
            "timezone": cfg.timezone,
            "rule_version": cfg.rule_version,
            "period_seconds": cfg.period_seconds,
            "threshold_seconds": cfg.threshold_seconds,
            "watermark_event_time": state["event_time"],
            "last_received_at": state["received_at"],
            "updater_id": state["updater_id"],
            "current_slot_iso": iso(cur_slot),
            "first_missing_slot_iso": iso(slot),
            "expected_next_iso": iso(cur_slot + timedelta(seconds=cfg.period_seconds)),
            "boundary_iso": iso(boundary),
            "overdue_seconds": (now - boundary).total_seconds() if stale else 0.0,
            "missed_slots": missed,
            "config_changed": state.get("source_sig") != cfg.signature(),
            "rule_switched": state.get("rule_version") != cfg.rule_version,
            "reasons": [
                f"过期边界为 {iso(boundary)}（首个缺失槽位 {iso(slot)} + 容忍 "
                f"{cfg.tolerance_seconds:.0f}s），当前时间 {iso(now)}"
                + (f"，已逾期 {(now - boundary).total_seconds():.0f}s" if stale
                   else "，尚未越界"),
            ] + ([f"自上次有效更新以来缺失 {missed} 个发布槽位"] if missed else []),
        }

    # -- 内部工具 ------------------------------------------------------------ #
    @staticmethod
    def _base(record, src, now, status, reasons, cfg: SourceConfig | None = None) -> dict:
        return {
            "source": src,
            "status": status,
            "reasons": list(reasons),
            "now_iso": iso(now),
            "record_id": str(record.get("id")) if record.get("id") is not None else None,
            "period_seconds": cfg.period_seconds if cfg else None,
            "tolerance_seconds": cfg.tolerance_seconds if cfg else None,
            "clock_skew_seconds": cfg.clock_skew_seconds if cfg else None,
            "timezone": cfg.timezone if cfg else None,
            "expected_at": ":".join(f"{p:02d}" for p in cfg.expected_at)
            if cfg and cfg.expected_at else None,
            "rule_version": cfg.rule_version if cfg else None,
            "late": False,
        }


# --------------------------------------------------------------------------- #
# 流水线阶段接口（pipeline.py 以 process(record) 形式调用）
# --------------------------------------------------------------------------- #
_default: FreshnessEvaluator | None = None


def get_default() -> FreshnessEvaluator:
    global _default
    if _default is None:
        _default = FreshnessEvaluator(load_sources(), DB_PATH)
    return _default


def set_default(evaluator: FreshnessEvaluator | None) -> None:
    """注入自定义评估器（测试固定时钟/临时数据库），传 None 复位。"""
    global _default
    if _default is not None and evaluator is None:
        _default.close()
    _default = evaluator


def reset_default() -> None:
    set_default(None)


def process(record: dict, now: datetime | None = None) -> dict:
    """流水线阶段：在记录上挂可解释的 ``_stage_freshness`` 状态，其余字段原样保留。"""
    rec = dict(record)
    rec["_stage_freshness"] = get_default().evaluate(rec, now=now)
    return rec

"""单一职责：把 quarantine / reconcile 上报的质量异常转成「可恢复告警」。

设计要点
--------
1. 三类数据分开持久化（同库不同表，职责互不混淆）：
   * findings      —— 异常指纹（每次 quarantine/reconcile 上报的原始观察）；
   * alerts        —— 告警状态（指纹去重后的一条持续演变的告警）；
   * notifications —— 通知确认（每次发送尝试、失败原因、重试与送达确认）；
   另附 alert_audit 记录状态迁移，保证全过程可观测。
2. 去重指纹 ``fp = sha256(src | rule | event_key)``。同一故障无论来自
   quarantine 还是 reconcile、重跑多少轮，都只会落到同一条告警上。
3. 支持聚合、抑制、升级、确认、恢复与通知失败重试。优先级（高 -> 低）：
   恢复 > 抑制 > 确认 > 升级 > 聚合窗口。
   * 恢复：最高优先级，立即关闭并取消所有待发通知；
   * 抑制：抑制期内不产生任何通知，到期自动解除；
   * 确认：确认后停止升级与重复通知，迟到异常只计数不重开；
   * 升级：超过升级时限且未确认/未抑制时，升级级别并升级通知；
   * 聚合：聚合窗口内的重复异常只计数，窗口结束才允许下一条通知。
4. 状态与上报结果互相可追溯：finding 带 source/source_ref/batch，
   可从告警回查每一条来源上报，也可从任一条上报反查告警。

流水线阶段入口 ``process`` 仍保持纯记录变换（happy-path 无异常时透传）；
有状态能力由 ``AlertManager`` 提供，记录通过 ``FINDINGS_KEY`` 携带
quarantine/reconcile 的结果，由管理器批量接收。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

# 记录中携带 quarantine/reconcile 结果的字段名（阶段间数据接缝）
FINDINGS_KEY = "_quality_findings"

# 告警状态
ACTIVE = "active"          # 持续中的故障
SUPPRESSED = "suppressed"  # 被抑制（窗口到期后回到 active）
ACKED = "acked"            # 值班人员已确认
RECOVERED = "recovered"    # 故障已恢复

# 通知状态
PENDING = "pending"        # 尚未尝试（等待 run_after）
SENT = "sent"              # 已送达（通知确认）
FAILED = "failed"          # 发送失败，等待重试
EXHAUSTED = "exhausted"    # 超过最大尝试次数，放弃

# 上报类型
KIND_ANOMALY = "anomaly"
KIND_RECOVERY = "recovery"

# 级别由低到高
SEVERITIES = ("warning", "major", "critical")

DEFAULT_AGGREGATE_WINDOW = 300.0          # 聚合窗口 5 分钟
DEFAULT_ESCALATION_INTERVALS = (600.0, 1800.0)  # 10 分钟升 major，30 分钟升 critical
DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_BACKOFF_BASE = 30.0               # 退避基数：30, 60, 120, ... 秒


# ---------------------------------------------------------------- 时间工具
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


# ---------------------------------------------------------------- 异常指纹
def fingerprint_of(src: str, rule: str, event_key: str) -> str:
    """按 源 + 规则 + 事件指纹 派生稳定告警指纹（同一故障重跑不变）。"""
    payload = json.dumps(
        [str(src), str(rule), str(event_key)], sort_keys=True, ensure_ascii=False
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- 通知器
class NotificationError(Exception):
    """通知发送失败时由 Notifier 抛出，管理器据此安排重试。"""


class LoggingNotifier:
    """默认离线通知器：只落本地记录，不联网。

    测试与联调可注入会抛 ``NotificationError`` 的假实现模拟发送失败。
    """

    def __init__(self, sink: list[dict[str, Any]] | None = None):
        self.sent: list[dict[str, Any]] = sink if sink is not None else []

    def __call__(self, alert: dict[str, Any], notification: dict[str, Any]) -> None:
        self.sent.append({"alert_fp": alert["fp"], "kind": notification["kind"]})


# ---------------------------------------------------------------- 管理器
class AlertManager:
    """可恢复告警管理器：指纹去重、聚合/抑制/升级/确认/恢复/失败重试。"""

    def __init__(
        self,
        con: sqlite3.Connection,
        *,
        notifier: Callable[[dict[str, Any], dict[str, Any]], None] | None = None,
        aggregate_window: float = DEFAULT_AGGREGATE_WINDOW,
        escalation_intervals: Iterable[float] = DEFAULT_ESCALATION_INTERVALS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
        clock: Callable[[], datetime] = now_utc,
        suppressions: Iterable[dict[str, Any]] = (),
    ):
        self.con = con
        self.con.row_factory = sqlite3.Row
        self.notifier = notifier or LoggingNotifier()
        self.aggregate_window = float(aggregate_window)
        self.escalation_intervals = tuple(float(x) for x in escalation_intervals)
        self.max_attempts = int(max_attempts)
        self.backoff_base = float(backoff_base)
        self.clock = clock
        # 规则级自动抑制：{"src": ..., "rule": ..., "until": iso|datetime}
        self.suppressions = list(suppressions)
        self._init_schema()

    # ------------------------------------------------------------ 建表
    def _init_schema(self) -> None:
        cur = self.con.cursor()
        # 1) 异常指纹：每条 quarantine/reconcile 上报一行
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS findings(
                fp           TEXT NOT NULL,
                source       TEXT NOT NULL,
                source_ref   TEXT NOT NULL,
                batch        TEXT NOT NULL,
                src          TEXT NOT NULL,
                rule         TEXT NOT NULL,
                event_key    TEXT NOT NULL,
                kind         TEXT NOT NULL,
                observed_at  TEXT NOT NULL,
                detail       TEXT NOT NULL,
                PRIMARY KEY (fp, source, source_ref, batch, kind)
            )
            """
        )
        # 2) 告警状态：同一指纹仅一行，持续演变
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts(
                fp               TEXT PRIMARY KEY,
                src              TEXT NOT NULL,
                rule             TEXT NOT NULL,
                event_key        TEXT NOT NULL,
                status           TEXT NOT NULL,
                severity         TEXT NOT NULL,
                occurrence_count INTEGER NOT NULL,
                first_seen       TEXT NOT NULL,
                last_seen        TEXT NOT NULL,
                last_batch       TEXT NOT NULL,
                escalations      INTEGER NOT NULL DEFAULT 0,
                acknowledged_at  TEXT,
                recovered_at     TEXT,
                suppressed_until TEXT,
                last_notified_at TEXT
            )
            """
        )
        # 3) 通知确认：每次发送/重试一行，失败原因可观测
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications(
                nid         INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_fp    TEXT NOT NULL,
                kind        TEXT NOT NULL,
                attempt     INTEGER NOT NULL,
                status      TEXT NOT NULL,
                fail_reason TEXT,
                run_after   TEXT NOT NULL,
                sent_at     TEXT
            )
            """
        )
        # 4) 状态迁移审计
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_audit(
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_fp TEXT NOT NULL,
                ts       TEXT NOT NULL,
                event    TEXT NOT NULL,
                detail   TEXT NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_findings_fp ON findings(fp)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_notif_fp ON notifications(alert_fp)"
        )
        self.con.commit()

    # ------------------------------------------------------------ 内部工具
    def _now(self) -> datetime:
        return self.clock().astimezone(timezone.utc)

    def _audit(self, fp: str, event: str, detail: dict[str, Any]) -> None:
        self.con.execute(
            "INSERT INTO alert_audit(alert_fp, ts, event, detail) VALUES(?,?,?,?)",
            (fp, iso(self._now()), event, json.dumps(detail, ensure_ascii=False)),
        )

    def _get_alert(self, fp: str) -> dict[str, Any] | None:
        row = self.con.execute(
            "SELECT * FROM alerts WHERE fp=?", (fp,)
        ).fetchone()
        return dict(row) if row else None

    def _suppression_until(self, src: str, rule: str, now: datetime) -> datetime | None:
        """规则级自动抑制匹配，返回抑制截止时间。"""
        for sup in self.suppressions:
            if sup.get("src") not in (None, src):
                continue
            if sup.get("rule") not in (None, rule):
                continue
            until = sup["until"]
            if isinstance(until, str):
                until = parse_iso(until)
            until = until.astimezone(timezone.utc)
            if until > now:
                return until
        return None

    # ------------------------------------------------------------ 接收上报
    def ingest_finding(self, finding: dict[str, Any]) -> dict[str, Any]:
        """接收单条 quarantine/reconcile 结果并驱动告警状态机。"""
        src = str(finding.get("src", "")).strip().lower()
        rule = str(finding.get("rule", "")).strip()
        event_key = str(finding.get("event_key", "")).strip()
        if not src or not rule or not event_key:
            raise ValueError("finding 必须包含 src / rule / event_key")

        source = str(finding.get("source", "unknown"))
        source_ref = str(finding.get("source_ref", ""))
        batch = str(finding.get("batch", ""))
        kind = str(finding.get("kind", KIND_ANOMALY))
        now = self._now()
        observed = finding.get("observed_at")
        observed_at = parse_iso(observed) if observed else now
        detail = finding.get("detail", {})
        fp = finding.get("fp") or fingerprint_of(src, rule, event_key)

        # 恢复型上报走恢复路径（reconcile 可直接上报故障消失）
        if kind == KIND_RECOVERY:
            existed = self._record_finding(
                fp, source, source_ref, batch, src, rule, event_key,
                kind, observed_at, detail,
            )
            result = {"fp": fp, "replay": not existed, "status": "no_alert"}
            if self._get_alert(fp) is not None:
                result["status"] = self.mark_recovered(
                    fp, reason="recovery_finding")["status"]
            return result

        inserted = self._record_finding(
            fp, source, source_ref, batch, src, rule, event_key,
            kind, observed_at, detail,
        )
        alert = self._get_alert(fp)
        result: dict[str, Any] = {
            "fp": fp,
            "received": 1,
            "duplicates": 0 if inserted else 1,
            "replay": not inserted,
        }

        if alert is None:
            # —— 新故障：建立一条持续演变的告警 ——
            suppressed_until = self._suppression_until(src, rule, now)
            status = SUPPRESSED if suppressed_until else ACTIVE
            self.con.execute(
                """
                INSERT INTO alerts(fp, src, rule, event_key, status, severity,
                       occurrence_count, first_seen, last_seen, last_batch,
                       escalations, suppressed_until)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    fp, src, rule, event_key, status, SEVERITIES[0], 1,
                    iso(observed_at), iso(now), batch, 0,
                    iso(suppressed_until) if suppressed_until else None,
                ),
            )
            self._audit(fp, "open", {"source": source, "batch": batch,
                                     "suppressed": status == SUPPRESSED})
            result.update(new=True, reopened=False, status=status)
            if status == ACTIVE:
                self._create_notification(fp, "open")
        else:
            # 已知故障：同一条告警继续演变，绝不新建
            was_recovered = alert["status"] == RECOVERED
            prev_batch = alert["last_batch"]
            if inserted:
                # 新观察才推进计数；重放（重复上报）只保留幂等痕迹
                alert = self._touch_existing(alert, source, batch, observed_at, now)
            result.update(new=False, status=alert["status"])
            if was_recovered:
                # 旧批次重放保持关闭；新批次再次出现同一故障则重开
                if inserted and batch and batch != prev_batch:
                    alert = self._reopen(alert, source, batch, now)
                    result.update(reopened=True, status=alert["status"])
                    if alert["status"] == ACTIVE:
                        self._create_notification(fp, "reopen")
                else:
                    result.update(reopened=False)
            else:
                result.update(reopened=False)
                # 聚合窗口内不重复通知；active 且窗口已过才补发聚合通知
                if alert["status"] == ACTIVE and inserted and self._aggregate_due(alert, now):
                    self._create_notification(fp, "aggregate")
        self.con.commit()
        result["notifications"] = self._pending_note_ids(fp)
        return result

    def ingest(self, findings: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        """批量接收 quarantine/reconcile 结果。"""
        return [self.ingest_finding(f) for f in findings]

    def _record_finding(self, fp, source, source_ref, batch, src, rule,
                        event_key, kind, observed_at, detail) -> bool:
        """落一条异常指纹；重复上报（同来源同批次同引用）幂等忽略。返回是否新插入。"""
        cur = self.con.execute(
            """
            INSERT OR IGNORE INTO findings(fp, source, source_ref, batch, src,
                   rule, event_key, kind, observed_at, detail)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (fp, source, source_ref, batch, src, rule, event_key, kind,
             iso(observed_at), json.dumps(detail, ensure_ascii=False)),
        )
        return cur.rowcount == 1

    def _touch_existing(self, alert, source, batch, observed_at, now):
        """已知故障再次出现：计数 + 时间戳推进，状态机位置保持不变。"""
        alert["occurrence_count"] += 1
        alert["last_seen"] = iso(max(now, observed_at))
        if batch:
            alert["last_batch"] = batch
        self.con.execute(
            "UPDATE alerts SET occurrence_count=?, last_seen=?, last_batch=? WHERE fp=?",
            (alert["occurrence_count"], alert["last_seen"], alert["last_batch"], alert["fp"]),
        )
        self._audit(alert["fp"], "repeat",
                    {"source": source, "batch": batch,
                     "occurrence_count": alert["occurrence_count"]})
        return alert

    def _reopen(self, alert, source, batch, now):
        """恢复后新批次再次出现同一故障：重开原告警并重置升级/确认状态。"""
        suppressed_until = self._suppression_until(alert["src"], alert["rule"], now)
        status = SUPPRESSED if suppressed_until else ACTIVE
        self.con.execute(
            """
            UPDATE alerts SET status=?, severity=?, escalations=0,
                   acknowledged_at=NULL, recovered_at=NULL,
                   suppressed_until=?, first_seen=?, last_notified_at=NULL
             WHERE fp=?
            """,
            (status, SEVERITIES[0],
             iso(suppressed_until) if suppressed_until else None,
             iso(now), alert["fp"]),
        )
        self._audit(alert["fp"], "reopen", {"source": source, "batch": batch})
        alert.update(status=status, severity=SEVERITIES[0], escalations=0,
                     acknowledged_at=None, recovered_at=None, first_seen=iso(now),
                     last_notified_at=None)
        return alert

    def _aggregate_due(self, alert, now) -> bool:
        """聚合窗口是否已过（窗口内的重复异常被聚合，不产生新通知）。"""
        last = alert.get("last_notified_at")
        if not last:
            return True
        return (now - parse_iso(last)).total_seconds() >= self.aggregate_window

    # ------------------------------------------------------------ 批次收尾/恢复
    def close_batch(self, batch: str) -> list[str]:
        """一轮批次结束：本批次未再出现的活动告警判定为自动恢复。

        已确认（acked）的告警同样按「故障消失」恢复，确认只压制通知、
        不改变故障本身的恢复判定。
        """
        rows = self.con.execute(
            "SELECT fp FROM alerts WHERE status IN (?, ?)", (ACTIVE, ACKED)
        ).fetchall()
        recovered = []
        for row in rows:
            fp = row["fp"]
            seen = self.con.execute(
                "SELECT 1 FROM findings WHERE fp=? AND batch=? AND kind=? LIMIT 1",
                (fp, batch, KIND_ANOMALY),
            ).fetchone()
            if seen is None:
                self.mark_recovered(fp, reason=f"absent_in_batch:{batch}")
                recovered.append(fp)
        self.con.commit()
        return recovered

    def mark_recovered(self, fp: str, *, reason: str = "manual"):
        """恢复：最高优先级。关闭告警并取消所有尚未送达的通知。"""
        alert = self._get_alert(fp)
        if alert is None:
            raise KeyError(f"unknown alert fp: {fp}")
        now = iso(self._now())
        if alert["status"] != RECOVERED:
            self.con.execute(
                "UPDATE alerts SET status=?, recovered_at=?, suppressed_until=NULL WHERE fp=?",
                (RECOVERED, now, fp),
            )
            self._audit(fp, "recover", {"reason": reason})
        # 取消待发/待重试通知；已送达的保留为历史
        self.con.execute(
            "UPDATE notifications SET status='cancelled', fail_reason=COALESCE(fail_reason,'') "
            "WHERE alert_fp=? AND status IN (?, ?)",
            (fp, PENDING, FAILED),
        )
        self.con.commit()
        return self._get_alert(fp)

    # ------------------------------------------------------------ 确认
    def acknowledge(self, fp: str):
        """值班确认：停止升级与重复通知；迟到异常只计数不重开。"""
        alert = self._get_alert(fp)
        if alert is None:
            raise KeyError(f"unknown alert fp: {fp}")
        if alert["status"] == RECOVERED:
            raise ValueError(f"alert {fp} already recovered")
        now = iso(self._now())
        self.con.execute(
            "UPDATE alerts SET status=?, acknowledged_at=?, suppressed_until=NULL WHERE fp=?",
            (ACKED, now, fp),
        )
        self.con.execute(
            "UPDATE notifications SET status='cancelled', fail_reason=COALESCE(fail_reason,'') "
            "WHERE alert_fp=? AND status IN (?, ?)",
            (fp, PENDING, FAILED),
        )
        self._audit(fp, "acknowledge", {})
        self.con.commit()
        return self._get_alert(fp)

    # ------------------------------------------------------------ 抑制
    def suppress(self, fp: str, until: datetime | str):
        """显式抑制：抑制期内不发任何通知，``pump`` 到期自动解除。"""
        alert = self._get_alert(fp)
        if alert is None:
            raise KeyError(f"unknown alert fp: {fp}")
        if isinstance(until, str):
            until = parse_iso(until)
        until = until.astimezone(timezone.utc)
        self.con.execute(
            "UPDATE alerts SET status=?, suppressed_until=? WHERE fp=?",
            (SUPPRESSED, iso(until), fp),
        )
        self.con.execute(
            "UPDATE notifications SET status='cancelled', fail_reason=COALESCE(fail_reason,'') "
            "WHERE alert_fp=? AND status IN (?, ?)",
            (fp, PENDING, FAILED),
        )
        self._audit(fp, "suppress", {"until": iso(until)})
        self.con.commit()
        return self._get_alert(fp)

    # ------------------------------------------------------------ 通知与重试
    def _create_notification(self, fp: str, kind: str) -> int:
        """登记一条通知并立即尝试发送；失败则按退避安排重试。"""
        now = self._now()
        cur = self.con.execute(
            "INSERT INTO notifications(alert_fp, kind, attempt, status, run_after) "
            "VALUES(?,?,?,?,?)",
            (fp, kind, 1, PENDING, iso(now)),
        )
        nid = int(cur.lastrowid)
        self.con.execute(
            "UPDATE alerts SET last_notified_at=? WHERE fp=?", (iso(now), fp)
        )
        self._audit(fp, "notify", {"nid": nid, "kind": kind})
        self._attempt(nid)
        return nid

    def _attempt(self, nid: int) -> None:
        """执行一次发送尝试，记录成功确认或失败原因/重试时间。"""
        note = self.con.execute(
            "SELECT * FROM notifications WHERE nid=?", (nid,)
        ).fetchone()
        if note is None or note["status"] not in (PENDING, FAILED):
            return
        alert = self._get_alert(note["alert_fp"])
        try:
            self.notifier(dict(alert), dict(note))
        except NotificationError as exc:
            reason = str(exc)
            attempt = note["attempt"]
            if attempt >= self.max_attempts:
                self.con.execute(
                    "UPDATE notifications SET status=?, fail_reason=? WHERE nid=?",
                    (EXHAUSTED, reason, nid),
                )
                self._audit(note["alert_fp"], "notify_exhausted",
                            {"nid": nid, "attempt": attempt, "reason": reason})
            else:
                backoff = self.backoff_base * (2 ** (attempt - 1))
                run_after = self._now() + timedelta(seconds=backoff)
                self.con.execute(
                    "UPDATE notifications SET status=?, attempt=?, fail_reason=?, run_after=? "
                    "WHERE nid=?",
                    (FAILED, attempt + 1, reason, iso(run_after), nid),
                )
                self._audit(note["alert_fp"], "notify_failed",
                            {"nid": nid, "attempt": attempt,
                             "reason": reason, "retry_at": iso(run_after)})
        except Exception as exc:  # 非预期失败同样可观测、可重试
            reason = f"{type(exc).__name__}: {exc}"
            run_after = self._now() + timedelta(seconds=self.backoff_base)
            self.con.execute(
                "UPDATE notifications SET status=?, attempt=?, fail_reason=?, run_after=? "
                "WHERE nid=?",
                (FAILED, note["attempt"] + 1, reason, iso(run_after), nid),
            )
            self._audit(note["alert_fp"], "notify_failed",
                        {"nid": nid, "reason": reason, "retry_at": iso(run_after)})
        else:
            self.con.execute(
                "UPDATE notifications SET status=?, sent_at=?, fail_reason=NULL WHERE nid=?",
                (SENT, iso(self._now()), nid),
            )
            self._audit(note["alert_fp"], "notify_sent", {"nid": nid})

    def pump(self) -> dict[str, int]:
        """推进所有到期动作：失败重试、抑制解除、升级、聚合补发。

        优先级（高 -> 低）：恢复（只在 close_batch/mark_recovered 发生）
        > 抑制 > 确认 > 升级 > 聚合窗口。
        """
        now = self._now()
        retried = escalated = reminded = unsuppressed = 0

        # 1) 到期的失败通知重试
        due = self.con.execute(
            "SELECT nid FROM notifications WHERE status=? AND run_after<=?",
            (FAILED, iso(now)),
        ).fetchall()
        for row in due:
            self._attempt(row["nid"])
            retried += 1

        # 2) 活动告警的抑制解除 / 升级 / 聚合
        for row in self.con.execute(
            "SELECT * FROM alerts WHERE status IN (?, ?) ORDER BY first_seen",
            (ACTIVE, SUPPRESSED),
        ).fetchall():
            alert = dict(row)
            # 抑制到期自动解除
            if alert["status"] == SUPPRESSED:
                if alert["suppressed_until"] and parse_iso(alert["suppressed_until"]) <= now:
                    self.con.execute(
                        "UPDATE alerts SET status=?, suppressed_until=NULL WHERE fp=?",
                        (ACTIVE, alert["fp"]),
                    )
                    self._audit(alert["fp"], "unsuppress", {})
                    alert["status"] = ACTIVE
                    unsuppressed += 1
                else:
                    continue  # 抑制中：升级与聚合通知一律跳过

            # 确认后停止升级与重复通知（迟到异常只计数）
            if alert["status"] == ACKED:
                continue

            age = (now - parse_iso(alert["first_seen"])).total_seconds()
            # 升级：未确认且持续超过升级时限
            target_level = 0
            for idx, interval in enumerate(self.escalation_intervals, start=1):
                if age >= interval:
                    target_level = idx
            if target_level > alert["escalations"]:
                new_sev = SEVERITIES[min(target_level, len(SEVERITIES) - 1)]
                self.con.execute(
                    "UPDATE alerts SET severity=?, escalations=? WHERE fp=?",
                    (new_sev, target_level, alert["fp"]),
                )
                self._audit(alert["fp"], "escalate",
                            {"severity": new_sev, "escalations": target_level})
                self._create_notification(alert["fp"], "escalation")
                escalated += 1
                continue  # 升级通知优先，本轮不再发聚合通知

            # 聚合：窗口外的持续故障补发提醒
            if self._aggregate_due(alert, now):
                self._create_notification(alert["fp"], "aggregate")
                reminded += 1

        self.con.commit()
        return {"retried": retried, "escalated": escalated,
                "reminded": reminded, "unsuppressed": unsuppressed}

    # ------------------------------------------------------------ 查询/追溯
    def get_alert(self, fp: str) -> dict[str, Any]:
        alert = self._get_alert(fp)
        if alert is None:
            raise KeyError(f"unknown alert fp: {fp}")
        return alert

    def list_alerts(self, status: str | None = None) -> list[dict[str, Any]]:
        if status is None:
            rows = self.con.execute("SELECT * FROM alerts ORDER BY first_seen").fetchall()
        else:
            rows = self.con.execute(
                "SELECT * FROM alerts WHERE status=? ORDER BY first_seen", (status,)
            ).fetchall()
        return [dict(r) for r in rows]

    def list_findings(self, *, alert_fp: str | None = None,
                      source: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM findings WHERE 1=1"
        args: list[Any] = []
        if alert_fp is not None:
            sql += " AND fp=?"
            args.append(alert_fp)
        if source is not None:
            sql += " AND source=?"
            args.append(source)
        sql += " ORDER BY observed_at"
        return [dict(r) for r in self.con.execute(sql, args).fetchall()]

    def list_notifications(self, *, alert_fp: str | None = None,
                           status: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM notifications WHERE 1=1"
        args: list[Any] = []
        if alert_fp is not None:
            sql += " AND alert_fp=?"
            args.append(alert_fp)
        if status is not None:
            sql += " AND status=?"
            args.append(status)
        sql += " ORDER BY nid"
        return [dict(r) for r in self.con.execute(sql, args).fetchall()]

    def list_audit(self, alert_fp: str | None = None) -> list[dict[str, Any]]:
        if alert_fp is None:
            rows = self.con.execute(
                "SELECT * FROM alert_audit ORDER BY id").fetchall()
        else:
            rows = self.con.execute(
                "SELECT * FROM alert_audit WHERE alert_fp=? ORDER BY id",
                (alert_fp,)).fetchall()
        return [dict(r) for r in rows]

    def find_alert_by_finding(self, source: str, source_ref: str,
                              batch: str) -> dict[str, Any] | None:
        """从一条 quarantine/reconcile 上报反查它归属的告警。"""
        row = self.con.execute(
            "SELECT fp FROM findings WHERE source=? AND source_ref=? AND batch=? LIMIT 1",
            (source, source_ref, batch),
        ).fetchone()
        return self._get_alert(row["fp"]) if row else None

    def traceback(self, fp: str) -> dict[str, Any]:
        """沿来源回查：告警 -> 全部上报指纹 / 通知确认 / 状态迁移。"""
        return {
            "alert": self.get_alert(fp),
            "findings": self.list_findings(alert_fp=fp),
            "notifications": self.list_notifications(alert_fp=fp),
            "audit": self.list_audit(fp),
        }

    def stats(self) -> dict[str, int]:
        def count(table, where="", args=()):
            return self.con.execute(
                f"SELECT COUNT(*) FROM {table} {where}", args).fetchone()[0]
        return {
            "alerts": count("alerts"),
            "active": count("alerts", "WHERE status=?", (ACTIVE,)),
            "suppressed": count("alerts", "WHERE status=?", (SUPPRESSED,)),
            "acked": count("alerts", "WHERE status=?", (ACKED,)),
            "recovered": count("alerts", "WHERE status=?", (RECOVERED,)),
            "findings": count("findings"),
            "notifications": count("notifications"),
            "failed_notifications": count("notifications", "WHERE status=?", (FAILED,)),
        }

    def _pending_note_ids(self, fp: str) -> list[int]:
        return [r["nid"] for r in self.con.execute(
            "SELECT nid FROM notifications WHERE alert_fp=? "
            "AND status != 'cancelled' ORDER BY nid", (fp,)
        ).fetchall()]


# ---------------------------------------------------------------- 流水线接缝
def extract_findings(record: dict[str, Any]) -> list[dict[str, Any]]:
    """取出 quarantine/reconcile 挂在记录上的质量结果（没有则为空）。"""
    found = record.get(FINDINGS_KEY)
    if not found:
        return []
    if isinstance(found, dict):
        return [found]
    return list(found)


def process(record):
    """流水线阶段入口：保持单一职责的纯记录变换，happy-path 透传。

    有状态的告警接收由 ``AlertManager.ingest`` 承担；本函数只保证阶段
    契约（标记 + 不破坏原记录），让无异常的正常链路继续通过。
    """
    rec = dict(record)
    rec.setdefault("_stage_alert", "ok")
    return rec

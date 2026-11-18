"""单一职责：采集输入的质量入口 —— 输入 schema 完整性检查。

设计约束（对应题目里的“质量入口”要求）：

* 本模块只负责判定与修复单条记录，不做持久化、不做批次编排。
* 判定结果是一份 **可序列化的契约**（``report``），其中包含：
  - ``rule_version``：本次校验使用的规则版本；
  - ``raw_digest``：原始样本的规范 JSON 摘要（sha256）；
  - ``fields``：逐字段结果（状态 / 动作 / 期望 / 实际类型 / 依据）；
  - ``decision`` / ``blocked`` / ``route``：放行、修复或阻断。
  该契约会随 quarantine 样本和正式落库记录一起持久化。
* 失败关闭（fail-closed）：无法确认语义的记录一律 ``block``，调用方必须
  在写入正式结果之前把 ``block`` 记录交给 quarantine。
* 旧格式兼容：缺省 ``schema_version`` 的记录按 v1（legacy）校验，不注入新
  字段，旧消费方仍能读取；v2 通过显式别名/类型规则平滑接收升级后的采集接口。

本模块仅依赖标准库，便于独立测试和复用。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

# ---------------------------------------------------------------------------
# 规则版本与策略（策略表是“依据可查”的入口：describe_policy() 可整体导出）
# ---------------------------------------------------------------------------

RULE_VERSION = "2026.11.1"
LEGACY_SCHEMA_VERSION = "1"
CURRENT_SCHEMA_VERSION = "2"
METADATA_KEYS = frozenset({"schema_version", "schemaVersion"})
UNKNOWN_POLICIES = ("allow", "strip", "block")
UNKNOWN_POLICY_ENV = "FRESHNESSGATE_UNKNOWN_FIELD_POLICY"

#: 每一种决策都带明确依据；报告里的 ``policy`` 引用这里的键。
POLICY: dict[str, dict[str, Any]] = {
    "missing_required": {
        "action": "block",
        "rationale": "必填字段缺失说明采集契约未被满足，下游无法解释其语义；阻断并隔离，等待源端修复。",
    },
    "null_not_allowed": {
        "action": "block",
        "rationale": "非可空字段收到显式 null，等同于关键值缺失；阻断而不是用默认值掩盖。",
    },
    "type_mismatch": {
        "action": "block",
        "rationale": "非白名单类型变化会改变字段含义（例如数量变成文本），自动修复可能掩盖上游升级，阻断。",
    },
    "type_coerced": {
        "action": "repair",
        "rationale": "仅旧格式值允许按 v1 兼容路径做无损整数化（十进制整数字符串 / 整值浮点），修复后记录在 repairs 中。",
    },
    "invalid_format": {
        "action": "block",
        "rationale": "声明为时间戳的字段无法解析时，其含义不可确认；阻断并隔离。",
    },
    "unknown_enum": {
        "action": "block",
        "rationale": "未知枚举可能是新状态或脏数据，无法安全映射到既有语义；阻断并隔离，交由人工判定。",
    },
    "rename": {
        "action": "repair",
        "rationale": "重命名在 registry 中声明为 1:1 且无损（v1.name→v2.title、v1.ts→v2.observed_at），可安全修复。",
    },
    "rename_conflict": {
        "action": "block",
        "rationale": "新旧字段同时存在且值不一致，无法判断权威值；阻断以避免静默选错。",
    },
    "duplicate_alias": {
        "action": "repair",
        "rationale": "新旧字段同时存在且值一致，删除冗余别名不会丢失信息，按修复处理。",
    },
    "unknown_field": {
        "action": "pass",
        "effect": "allow",
        "rationale": "新增字段不改变已知字段语义；默认放行并登记到 unknown_fields，保留前向兼容；"
        "可通过环境变量 FRESHNESSGATE_UNKNOWN_FIELD_POLICY=allow|strip|block 调整。",
        "configurable": ["allow", "strip", "block"],
    },
    "unsupported_schema_version": {
        "action": "block",
        "rationale": "未知主版本可能改变关键字段语义，不能用旧规则解释；阻断而不是猜测。",
    },
    "version_conflict": {
        "action": "block",
        "rationale": "同一记录声明了两个不同的 schema_version，版本不可判定；阻断。",
    },
    "legacy_missing_version": {
        "action": "allow",
        "rationale": "缺失 schema_version 视为旧格式（v1）并走兼容校验路径；不向记录写入新字段。",
    },
    "record_not_object": {
        "action": "block",
        "rationale": "采集结果不是对象，无法按字段契约校验；阻断并隔离原始样本。",
    },
    "internal_error": {
        "action": "block",
        "rationale": "校验器异常时默认阻断（fail-closed），绝不让未经验证的记录进入正式结果。",
    },
    "version_supported": {
        "action": "pass",
        "rationale": "声明版本在支持列表内，按其字段规则校验。",
    },
}


# ---------------------------------------------------------------------------
# Schema registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldRule:
    """单个字段的契约。"""

    name: str
    accepted: tuple[str, ...]
    required: bool = True
    nullable: bool = False
    enum: frozenset[str] | None = None
    aliases: tuple[str, ...] = ()
    #: 是否允许 v1 兼容的无损整数化（旧采集接口的字符串/整值浮点 value）
    coerce_int_string: bool = False
    #: 是否要求 ISO-8601 时间戳语义
    datetime: bool = False

    def expected(self) -> str:
        parts = list(self.accepted)
        if self.enum:
            parts.append("enum[" + ",".join(sorted(self.enum)) + "]")
        if self.datetime:
            parts.append("iso8601")
        if self.nullable:
            parts.append("null")
        return "|".join(parts)


@dataclass(frozen=True)
class SchemaRule:
    version: str
    format: str
    fields: tuple[FieldRule, ...]
    unknown_policy: str = "allow"
    description: str = ""

    def field_map(self) -> dict[str, FieldRule]:
        return {f.name: f for f in self.fields}


# v1：旧采集接口。缺省 schema_version 时走这条路径。
LEGACY = SchemaRule(
    version=LEGACY_SCHEMA_VERSION,
    format="legacy",
    unknown_policy="allow",
    description="旧采集格式：name/value/status/ts，value 为整数。",
    fields=(
        FieldRule("id", ("str",), required=True),
        FieldRule("src", ("str",), required=True),
        FieldRule("name", ("str",), required=True),
        FieldRule("value", ("int",), required=True, coerce_int_string=True),
        FieldRule("status", ("str",), required=True,
                  enum=frozenset({"active", "sold", "pending"})),
        FieldRule("ts", ("str",), required=True),
    ),
)

# v2：采集接口升级后。title/observed_at 是 v1 字段的无损重命名；
# value 由整数放宽为 number；status 枚举扩展；unit 为可选新增字段。
CURRENT = SchemaRule(
    version=CURRENT_SCHEMA_VERSION,
    format="v2",
    unknown_policy="allow",
    description="升级后的采集格式：title/value(number)/status(扩展枚举)/observed_at。",
    fields=(
        FieldRule("id", ("str",), required=True),
        FieldRule("src", ("str",), required=True),
        FieldRule("title", ("str",), required=True, aliases=("name",)),
        FieldRule("value", ("int", "number"), required=True),
        FieldRule("status", ("str",), required=True,
                  enum=frozenset({"active", "sold", "pending", "archived", "discontinued"})),
        FieldRule("observed_at", ("str",), required=True, aliases=("ts",), datetime=True),
        FieldRule("unit", ("str",), required=False, nullable=True),
    ),
)

SCHEMAS: dict[str, SchemaRule] = {
    LEGACY.version: LEGACY,
    CURRENT.version: CURRENT,
}


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------


def _actual_type(value: Any) -> str:
    """返回稳定的类型名；bool 不并入 int。"""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _same_value(a: Any, b: Any) -> bool:
    """类型敏感的值相等判断（避免 True == 1 这类别名误判）。"""
    return _actual_type(a) == _actual_type(b) and a == b


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False,
                          separators=(",", ":"), default=str)
    except Exception:  # pragma: no cover - 极端非法键
        return repr(value)


def digest_of(raw: Any) -> str:
    """原始样本摘要（与校验结果里的 raw_digest 一致，可独立复算）。"""
    return "sha256:" + hashlib.sha256(_canonical_json(raw).encode("utf-8")).hexdigest()


def _valid_datetime(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        datetime.fromisoformat(text)
    except ValueError:
        return False
    return True


def _normalize_version(value: Any) -> str:
    """版本归一化：v2 / 2.0 / "2" 均视为主版本 2；未知文本原样返回。"""
    text = str(value).strip().lower()
    if text.startswith("v"):
        text = text[1:]
    if re.fullmatch(r"\d+(?:\.\d+)*", text):
        return text.split(".")[0]
    return text


def _declared_version(raw: Mapping[str, Any]) -> tuple[Any, bool]:
    present = [k for k in ("schema_version", "schemaVersion") if k in raw]
    if not present:
        return None, False
    first = raw["schema_version"] if "schema_version" in raw else raw["schemaVersion"]
    if len(present) == 2:
        if _normalize_version(raw["schema_version"]) != _normalize_version(raw["schemaVersion"]):
            return first, True
    return first, False


def _effective_unknown_policy(rule: SchemaRule) -> tuple[str, str]:
    override = os.environ.get(UNKNOWN_POLICY_ENV, "").strip().lower()
    if override in UNKNOWN_POLICIES:
        return override, "env"
    return rule.unknown_policy, "schema"


def _reason(code: str, field: str | None, action: str, policy: str, detail: str) -> dict[str, Any]:
    return {
        "code": code,
        "field": field,
        "action": action,
        "policy": policy,
        "policy_action": POLICY.get(policy, {}).get("action", action),
        "rationale": POLICY.get(policy, {}).get("rationale", ""),
        "detail": detail,
    }


def _field_result(rule: FieldRule, status: str, action: str, detail: str,
                  actual_type: str | None = None, actual: Any = None,
                  policy: str | None = None,
                  renamed_from: str | None = None,
                  expected: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "field": rule.name,
        "status": status,
        "action": action,
        "expected": expected if expected is not None else rule.expected(),
        "actual_type": actual_type,
        "detail": detail,
        "policy": policy,
    }
    if actual is not None:
        result["actual"] = actual if _actual_type(actual) in ("str", "int", "number", "bool") else _actual_type(actual)
    if renamed_from is not None:
        result["renamed_from"] = renamed_from
    return result


def _new_report(raw: Any) -> dict[str, Any]:
    return {
        "rule_version": RULE_VERSION,
        "schema_version": None,
        "format": None,
        "declared_version": None,
        "compatibility": {
            "declared": None,
            "detected": None,
            "supported": False,
            "mode": "unknown",
        },
        "compat": False,
        "raw_digest": digest_of(raw),
        "decision": "pass",
        "blocked": False,
        "route": "persist",
        "reasons": [],
        "fields": {},
        "unknown_fields": [],
        "repairs": [],
        "policy_version": RULE_VERSION,
    }


def _attach(out: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    out = dict(out) if isinstance(out, Mapping) else {}
    out["_schema"] = report
    return out


def _finish(report: dict[str, Any], out: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    actions = {r["action"] for r in report["fields"].values() if isinstance(r, dict)}
    # 版本级/记录级原因也参与决策
    actions.update(r["action"] for r in report["reasons"])
    blocked = "block" in actions or any(r["action"] == "block" for r in report["reasons"])
    report["blocked"] = blocked
    if blocked:
        report["decision"] = "block"
        report["route"] = "quarantine"
    elif "repair" in actions:
        report["decision"] = "repair"
        report["route"] = "persist"
    else:
        report["decision"] = "pass"
        report["route"] = "persist"
    return _attach(out, report), report


def _version_gate(raw: Mapping[str, Any], report: dict[str, Any]) -> SchemaRule | None:
    declared, conflict = _declared_version(raw)
    report["declared_version"] = declared

    if conflict:
        report["compatibility"] = {
            "declared": declared,
            "detected": None,
            "supported": False,
            "mode": "conflict",
        }
        report["reasons"].append(
            _reason("version_conflict", None, "block", "version_conflict",
                    f"schema_version 与 schemaVersion 不一致：{raw.get('schema_version')!r} vs "
                    f"{raw.get('schemaVersion')!r}")
        )
        return None

    if declared is None:
        rule = SCHEMAS[LEGACY_SCHEMA_VERSION]
        report["schema_version"] = rule.version
        report["format"] = rule.format
        report["compat"] = True
        report["compatibility"] = {
            "declared": None,
            "detected": rule.version,
            "supported": True,
            "mode": "legacy-missing-version",
        }
        report["reasons"].append(
            _reason("legacy_missing_version", None, "pass", "legacy_missing_version",
                    "未声明 schema_version，按旧格式 v1 校验；旧格式输出保持不变。")
        )
        return rule

    version = _normalize_version(declared)
    rule = SCHEMAS.get(version)
    report["compatibility"] = {
        "declared": declared,
        "detected": version,
        "supported": rule is not None,
        "mode": "current" if version == CURRENT_SCHEMA_VERSION else "legacy-supported",
    }
    if rule is None:
        report["reasons"].append(
            _reason("unsupported_schema_version", None, "block", "unsupported_schema_version",
                    f"不支持的 schema_version={declared!r}；支持 {sorted(SCHEMAS)}")
        )
        return None

    report["schema_version"] = rule.version
    report["format"] = rule.format
    report["compat"] = rule.version != CURRENT_SCHEMA_VERSION
    report["reasons"].append(
        _reason("version_supported", None, "pass", "version_supported",
                f"schema_version={declared!r} 归一化为 {version}，按 {rule.format} 规则校验。")
    )
    return rule


def _resolve_aliases(out: dict[str, Any], fr: FieldRule) -> dict[str, Any]:
    """把声明过的旧字段名折叠到规范字段名。

    返回值 status:
      - ``absent``   规范字段和别名都不存在
      - ``ok``       只有规范字段
      - ``renamed``  仅旧别名存在，已改名为规范字段
      - ``dedupe``   规范字段与值相同的别名并存，别名已删除
      - ``conflict`` 新旧字段值不一致（不做任何修改）
    """
    present = [a for a in fr.aliases if a in out]

    if fr.name in out:
        dropped: list[str] = []
        for alias in present:
            if _same_value(out[alias], out[fr.name]):
                dropped.append(alias)
            else:
                return {
                    "status": "conflict",
                    "detail": f"{fr.name}={out[fr.name]!r} 与 {alias}={out[alias]!r} 不一致",
                    "aliases": present,
                }
        for alias in dropped:
            del out[alias]
        return {
            "status": "dedupe" if dropped else "ok",
            "renamed_from": None,
            "dropped": dropped,
        }

    if present:
        first = present[0]
        for alias in present[1:]:
            if not _same_value(out[alias], out[first]):
                return {
                    "status": "conflict",
                    "detail": f"别名 {first}={out[first]!r} 与 {alias}={out[alias]!r} 不一致",
                    "aliases": present,
                }
        value = out.pop(first)
        for alias in present[1:]:
            del out[alias]
        out[fr.name] = value
        return {
            "status": "renamed",
            "renamed_from": first,
            "dropped": present[1:],
        }

    return {"status": "absent", "renamed_from": None, "dropped": []}


def _try_coerce(fr: FieldRule, value: Any) -> tuple[Any, str] | None:
    """只实现 registry 显式声明的无损兼容转换。"""
    if not fr.coerce_int_string or "int" not in fr.accepted:
        return None
    actual = _actual_type(value)
    if actual == "str":
        text = value.strip()
        if re.fullmatch(r"[+-]?\d+", text):
            return int(text), "十进制整数字符串 → int（v1 兼容）"
        return None
    if actual == "number" and isinstance(value, float) and value.is_integer():
        return int(value), "整值浮点 → int（v1 兼容，无损）"
    return None


def _validate_field(fr: FieldRule, out: dict[str, Any], report: dict[str, Any],
                    alias: dict[str, Any]) -> None:
    renamed_from = alias.get("renamed_from")

    if alias["status"] == "conflict":
        report["fields"][fr.name] = _field_result(
            fr, "rename_conflict", "block", alias["detail"], policy="rename_conflict")
        report["reasons"].append(
            _reason("rename_conflict", fr.name, "block", "rename_conflict", alias["detail"]))
        return

    if alias["status"] == "renamed":
        report["repairs"].append({
            "field": fr.name,
            "action": "rename",
            "from": renamed_from,
            "to": fr.name,
            "policy": "rename",
        })
        report["reasons"].append(
            _reason("rename", fr.name, "repair", "rename",
                    f"{renamed_from} → {fr.name}（registry 声明为无损别名）"))

    if alias["status"] == "dedupe":
        report["repairs"].append({
            "field": fr.name,
            "action": "drop_duplicate_alias",
            "from": alias.get("dropped"),
            "to": fr.name,
            "policy": "duplicate_alias",
        })
        report["reasons"].append(
            _reason("duplicate_alias", fr.name, "repair", "duplicate_alias",
                    f"删除与 {fr.name} 同值的冗余别名 {alias.get('dropped')}"))

    if fr.name not in out:
        if fr.required:
            report["fields"][fr.name] = _field_result(
                fr, "missing", "block", "必填字段缺失", policy="missing_required",
                renamed_from=renamed_from)
            report["reasons"].append(
                _reason("missing_required", fr.name, "block", "missing_required",
                        f"必填字段 {fr.name!r} 缺失"))
        else:
            report["fields"][fr.name] = _field_result(
                fr, "absent", "pass", "可选字段未提供", policy=None,
                renamed_from=renamed_from)
        return

    value = out[fr.name]
    actual = _actual_type(value)
    coerced = False
    coerce_detail = ""

    if value is None:
        if fr.nullable:
            report["fields"][fr.name] = _field_result(
                fr, "null", "pass", "可空字段为 null，按契约接受", actual_type=actual,
                renamed_from=renamed_from)
        else:
            report["fields"][fr.name] = _field_result(
                fr, "null", "block", "非可空字段收到 null", actual_type=actual,
                policy="null_not_allowed", renamed_from=renamed_from)
            report["reasons"].append(
                _reason("null_not_allowed", fr.name, "block", "null_not_allowed",
                        f"字段 {fr.name!r} 不允许 null"))
        return

    if actual not in fr.accepted:
        original_type = actual
        coerced = _try_coerce(fr, value)
        if coerced is None:
            detail = f"期望 {fr.expected()}，实际 {actual}"
            report["fields"][fr.name] = _field_result(
                fr, "type", "block", detail, actual_type=actual, actual=value,
                policy="type_mismatch", renamed_from=renamed_from)
            report["reasons"].append(
                _reason("type_mismatch", fr.name, "block", "type_mismatch",
                        f"字段 {fr.name!r} 类型不匹配：{detail}"))
            return
        value, coerce_detail = coerced
        out[fr.name] = value
        coerced = True
        actual = _actual_type(value)
        report["repairs"].append({
            "field": fr.name,
            "action": "coerce",
            "from": original_type,
            "to": actual,
            "detail": coerce_detail,
            "policy": "type_coerced",
        })
        report["reasons"].append(
            _reason("type_coerced", fr.name, "repair", "type_coerced",
                    f"字段 {fr.name!r}：{coerce_detail}"))

    if fr.datetime and not _valid_datetime(out[fr.name]):
        detail = f"期望 {fr.expected()}，实际 {out[fr.name]!r} 无法解析"
        report["fields"][fr.name] = _field_result(
            fr, "format", "block", detail, actual_type=actual, actual=out[fr.name],
            policy="invalid_format", renamed_from=renamed_from)
        report["reasons"].append(
            _reason("invalid_format", fr.name, "block", "invalid_format",
                    f"字段 {fr.name!r} 不是合法 ISO-8601 时间戳"))
        return

    if fr.enum is not None and out[fr.name] not in fr.enum:
        detail = f"未知枚举值 {out[fr.name]!r}，允许 {sorted(fr.enum)}"
        report["fields"][fr.name] = _field_result(
            fr, "enum", "block", detail, actual_type=actual, actual=out[fr.name],
            policy="unknown_enum", renamed_from=renamed_from)
        report["reasons"].append(
            _reason("unknown_enum", fr.name, "block", "unknown_enum",
                    f"字段 {fr.name!r} {detail}"))
        return

    action = "pass"
    status = "ok"
    detail = "校验通过"
    if coerced:
        action, status, detail = "repair", "coerced", "已按 v1 兼容规则无损整数化"
    if alias["status"] == "dedupe":
        action, status, detail = "repair", "deduped", "已删除同值冗余别名"
    if renamed_from:
        action = "repair"
        status = "renamed"
        detail = f"{renamed_from} → {fr.name}（registry 声明的无损别名）"
    report["fields"][fr.name] = _field_result(
        fr, status, action, detail,
        actual_type=actual, actual=out[fr.name], renamed_from=renamed_from)


def _handle_unknown_fields(out: dict[str, Any], rule: SchemaRule,
                           report: dict[str, Any]) -> None:
    known = set(METADATA_KEYS)
    for fr in rule.fields:
        known.add(fr.name)
        known.update(fr.aliases)

    policy, source = _effective_unknown_policy(rule)
    for key in [k for k in out if k not in known and k != "_schema"]:
        value = out[key]
        detail = f"未在 {rule.format} registry 中声明；unknown-field policy={policy}（来源 {source}）"
        if policy == "allow":
            report["unknown_fields"].append(key)
            report["fields"][key] = {
                "field": key,
                "status": "unknown",
                "action": "pass",
                "expected": "未声明字段",
                "actual_type": _actual_type(value),
                "detail": detail,
                "policy": "unknown_field",
            }
        elif policy == "strip":
            report["unknown_fields"].append(key)
            report["fields"][key] = {
                "field": key,
                "status": "unknown",
                "action": "repair",
                "expected": "未声明字段",
                "actual_type": _actual_type(value),
                "detail": detail + "；已从落库数据中剥离",
                "policy": "unknown_field",
            }
            report["repairs"].append({"field": key, "action": "drop_unknown",
                                      "detail": detail, "policy": "unknown_field"})
            del out[key]
        else:  # block
            report["unknown_fields"].append(key)
            report["fields"][key] = {
                "field": key,
                "status": "unknown",
                "action": "block",
                "expected": "未声明字段",
                "actual_type": _actual_type(value),
                "detail": detail,
                "policy": "unknown_field",
            }
            report["reasons"].append(
                _reason("unknown_field", key, "block", "unknown_field",
                        f"未知字段 {key!r} 在当前策略下阻断"))


# ---------------------------------------------------------------------------
# 公共 API
# ---------------------------------------------------------------------------


def evaluate(raw: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """校验并（必要时）修复单条原始记录。

    返回 ``(out, report)``：

    * ``out`` 是规范化后的新 dict；block 时调用方 **不得** 持久化它；
    * ``report`` 是持久化契约，含规则版本、原始摘要与逐字段结果。
    """
    report = _new_report(raw)

    if not isinstance(raw, Mapping):
        detail = f"期望 object，实际 {_actual_type(raw)}"
        report["reasons"].append(
            _reason("record_not_object", None, "block", "record_not_object", detail))
        report["fields"]["<record>"] = {
            "field": "<record>",
            "status": "type",
            "action": "block",
            "expected": "object",
            "actual_type": _actual_type(raw),
            "detail": detail,
            "policy": "record_not_object",
        }
        return _finish(report, {})

    out = dict(raw)
    rule = _version_gate(raw, report)
    if rule is None:
        return _finish(report, out)

    for fr in rule.fields:
        alias = _resolve_aliases(out, fr)
        _validate_field(fr, out, report, alias)

    _handle_unknown_fields(out, rule, report)

    # 保留输入里的版本声明（旧格式不注入新字段，保持兼容路径）
    return _finish(report, out)


def validate(raw: Any) -> dict[str, Any]:
    """只返回校验报告，不返回规范化记录（诊断/回放用）。"""
    return evaluate(raw)[1]


def process(record: Any) -> dict[str, Any]:
    """流水线阶段：返回带 ``_schema`` 契约的记录。

    ``_schema`` 里 ``decision`` 为 ``block`` 的记录必须由调用方交给隔离区，
    不能写入正式结果。模块本身不做任何 IO，保证可独立测试。
    """
    return evaluate(record)[0]


def error_report(raw: Any, exc: BaseException, stage: str = "schema_check") -> dict[str, Any]:
    """校验/下游阶段异常时的 fail-closed 报告，保证坏记录也能被隔离诊断。"""
    report = _new_report(raw)
    detail = f"{stage} 异常：{type(exc).__name__}: {exc}"
    report["reasons"].append(
        _reason("internal_error", None, "block", "internal_error", detail))
    report["fields"]["<record>"] = {
        "field": "<record>",
        "status": "error",
        "action": "block",
        "expected": "valid record",
        "actual_type": _actual_type(raw),
        "detail": detail,
        "policy": "internal_error",
    }
    return _finish(report, {})[1]


def policy_table() -> dict[str, dict[str, Any]]:
    """完整策略表（新增字段/重命名/类型变化等决策依据）。"""
    return {name: dict(spec) for name, spec in POLICY.items()}


def describe_policy() -> dict[str, Any]:
    """可审计的规则快照，便于运维/审计在任意时刻复核决策依据。"""
    def field_spec(fr: FieldRule) -> dict[str, Any]:
        return {
            "accepted": list(fr.accepted),
            "required": fr.required,
            "nullable": fr.nullable,
            "enum": sorted(fr.enum) if fr.enum else None,
            "aliases": list(fr.aliases),
            "datetime": fr.datetime,
            "coerce_int_string": fr.coerce_int_string,
        }

    return {
        "gate": "schema_check",
        "rule_version": RULE_VERSION,
        "current_schema_version": CURRENT_SCHEMA_VERSION,
        "supported_versions": sorted(SCHEMAS),
        "legacy_missing_version": LEGACY_SCHEMA_VERSION,
        "unknown_field_default": LEGACY.unknown_policy,
        "unknown_field_env": UNKNOWN_POLICY_ENV,
        "unknown_field_options": list(UNKNOWN_POLICIES),
        "policies": policy_table(),
        "schemas": {
            version: {
                "format": rule.format,
                "description": rule.description,
                "unknown_policy": rule.unknown_policy,
                "fields": {fr.name: field_spec(fr) for fr in rule.fields},
            }
            for version, rule in SCHEMAS.items()
        },
    }

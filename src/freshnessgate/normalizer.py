"""单一职责：字段规整/规范化（对记录做单一职责变换：normalizer）。规范阶段。

本阶段在 ``schema_check`` 之后运行，因此只做 **不改变字段类型与语义** 的规整：
去空白、统一 ``src`` 大小写。数值类型由质量门负责，这里不再做 ``int()`` 截断
（v2 的 value 允许小数，截断会悄悄改变含义）。缺失字段也不会被凭空补出，
避免把“没采到”伪装成空串。
"""
def process(record):
    rec = dict(record)
    if "src" in rec:
        rec["src"] = str(rec["src"]).strip().lower()
    for text_field in ("name", "title"):
        if text_field in rec and rec[text_field] is not None:
            rec[text_field] = str(rec[text_field]).strip()
    return rec

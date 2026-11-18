"""命令行入口：跑流水线、查看策略、回放隔离样本。

用法（``PYTHONPATH=src``）::

    python -m freshnessgate.cli                 # 跑默认批次
    python -m freshnessgate.cli --fixtures X    # 跑指定批次
    python -m freshnessgate.cli policy          # 导出策略快照
    python -m freshnessgate.cli replay          # 回放隔离样本
    python -m freshnessgate.cli recover         # 从只追加日志重建隔离库
"""
import argparse
import json
import sys

from . import pipeline, quarantine, schema_check


def _dump(value):
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str))


def main(argv=None):
    parser = argparse.ArgumentParser(prog="freshnessgate")
    parser.add_argument("command", nargs="?", default="run",
                        choices=["run", "policy", "replay", "recover", "quarantine"])
    parser.add_argument("--fixtures", default=None, help="批次 JSON 路径")
    args = parser.parse_args(argv)

    if args.command == "run":
        _dump(pipeline.run(fixtures_path=args.fixtures))
    elif args.command == "policy":
        _dump(schema_check.describe_policy())
    elif args.command == "quarantine":
        _dump(quarantine.samples())
    elif args.command == "replay":
        _dump(quarantine.replay_all())
    elif args.command == "recover":
        _dump({"recovered": quarantine.recover_from_journal()})
    return 0


if __name__ == "__main__":
    sys.exit(main())

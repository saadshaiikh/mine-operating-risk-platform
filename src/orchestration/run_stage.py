from __future__ import annotations

import argparse
from pathlib import Path

from src.common.io import repo_root
from src.ingestion.load_raw_msha import load_run


def _resolve_latest_run_id() -> str:
    base_dir = repo_root() / "data" / "raw" / "msha"
    latest_link = base_dir / "latest"
    if latest_link.exists() and latest_link.is_symlink():
        return latest_link.resolve().name
    latest_txt = base_dir / "latest.txt"
    if latest_txt.exists():
        return latest_txt.read_text(encoding="utf-8").strip()
    raise SystemExit("No latest run pointer found. Provide --run-id.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage MSHA raw datasets")
    parser.add_argument("--source-system", default="msha")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    if args.source_system.lower() != "msha":
        raise SystemExit("Only MSHA source system is supported in V1")

    run_id = args.run_id or _resolve_latest_run_id()
    load_run(run_id)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse

from src.connectors.msha.download import download_all
from src.ingestion.load_raw_msha import load_run


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and stage MSHA datasets")
    parser.add_argument("--source-system", default="msha")
    args = parser.parse_args()

    if args.source_system.lower() != "msha":
        raise SystemExit("Only MSHA source system is supported in V1")

    manifest = download_all()
    run_id = manifest["run_id"]
    load_run(run_id)


if __name__ == "__main__":
    main()

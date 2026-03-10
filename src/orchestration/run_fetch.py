from __future__ import annotations

import argparse

from src.connectors.msha.download import download_all


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch MSHA raw datasets")
    parser.add_argument("--source-system", default="msha")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--datasets", nargs="*", default=None)
    args = parser.parse_args()

    if args.source_system.lower() != "msha":
        raise SystemExit("Only MSHA source system is supported in V1")

    download_all(sources=args.datasets, run_id=args.run_id)


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


MANIFEST_NAME = "manifest.json"


def write_manifest(run_dir: Path, manifest: dict[str, Any]) -> None:
    path = run_dir / MANIFEST_NAME
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_manifest(run_dir: Path) -> dict[str, Any]:
    path = run_dir / MANIFEST_NAME
    return json.loads(path.read_text(encoding="utf-8"))

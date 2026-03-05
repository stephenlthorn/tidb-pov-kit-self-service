#!/usr/bin/env python3
"""Vercel Flask entrypoint for TiDB PoV UI."""

from __future__ import annotations

import os
from pathlib import Path

from setup.poc_web_ui import ROOT, create_app


def resolve_config_path() -> Path:
    override = os.environ.get("POV_CONFIG_PATH", "").strip()
    if override:
        path = Path(override)
        return path if path.is_absolute() else (ROOT / path).resolve()

    if os.environ.get("VERCEL"):
        tmp_cfg = Path("/tmp/tidb-pov-config.yaml")
        if not tmp_cfg.exists():
            source = ROOT / "config.yaml"
            fallback = ROOT / "config.yaml.example"
            seed = source if source.exists() else fallback
            if seed.exists():
                tmp_cfg.write_text(seed.read_text(encoding="utf-8"), encoding="utf-8")
            else:
                tmp_cfg.write_text("{}\n", encoding="utf-8")
        return tmp_cfg

    return ROOT / "config.yaml"


app = create_app(resolve_config_path())


if __name__ == "__main__":
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8787"))
    app.run(host=host, port=port, debug=False)

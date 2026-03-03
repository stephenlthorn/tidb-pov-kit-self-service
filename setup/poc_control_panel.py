#!/usr/bin/env python3
"""Interactive control panel for the TiDB Cloud self-service PoC kit."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Dict, List, Tuple

try:
    import yaml
except ModuleNotFoundError:
    print(
        "Missing dependency: pyyaml. Install dependencies first "
        "(bash setup/01_install_deps.sh or pip install pyyaml).",
        file=sys.stderr,
    )
    raise SystemExit(3)


ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "results"
REPORT_PDF = RESULTS_DIR / "tidb_pov_report.pdf"
RESULTS_DB = RESULTS_DIR / "results.db"

# Reuse tier logic from intake wizard
sys.path.insert(0, str(ROOT))
from setup.pre_poc_intake import (  # type: ignore  # noqa: E402
    SCENARIOS,
    TIERS,
    TIER_LABELS,
    build_tier_modules,
    tier_test_profile,
)


class UI:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    MAGENTA = "\033[35m"

    def __init__(self) -> None:
        self.use_color = self._supports_color()
        width = shutil.get_terminal_size((100, 30)).columns
        self.width = max(72, min(width, 120))

    def _supports_color(self) -> bool:
        if os.environ.get("NO_COLOR"):
            return False
        if not sys.stdout.isatty():
            return False
        term = os.environ.get("TERM", "")
        if term.lower() == "dumb":
            return False
        return True

    def style(self, text: str, *codes: str) -> str:
        if not self.use_color or not codes:
            return text
        return "".join(codes) + text + self.RESET

    def clear(self) -> None:
        if os.name == "nt":
            os.system("cls")
        else:
            print("\033[2J\033[H", end="")

    def _wrap(self, text: str, indent: int = 0) -> List[str]:
        width = self.width - 4 - indent
        return textwrap.wrap(text, width=max(24, width)) or [""]

    def divider(self, ch: str = "=") -> str:
        return ch * self.width

    def render(self, title: str, subtitle: str = "", bullets: List[str] | None = None, status: List[str] | None = None) -> None:
        self.clear()
        print(self.style(self.divider("="), self.CYAN))
        print(self.style(" TiDB PoC Control Panel ", self.BOLD, self.CYAN))
        print(self.style(self.divider("="), self.CYAN))
        print("")

        if status:
            for line in status:
                print(self.style(f"  {line}", self.DIM))
            print("")
            print(self.style(self.divider("-"), self.DIM))
            print("")

        print(self.style(title, self.BOLD, self.MAGENTA))
        if subtitle:
            print("")
            for line in self._wrap(subtitle):
                print(line)
        if bullets:
            print("")
            for bullet in bullets:
                wrapped = self._wrap(bullet, indent=2)
                print(f"  - {wrapped[0]}")
                for extra in wrapped[1:]:
                    print(f"    {extra}")
        print("")

    def menu(
        self,
        title: str,
        subtitle: str,
        options: List[Tuple[str, str, str]],
        status: List[str] | None = None,
        allow_back: bool = False,
    ) -> str:
        while True:
            self.render(title=title, subtitle=subtitle, status=status)

            for i, (_, label, desc) in enumerate(options, 1):
                print(self.style(f"  {i}. {label}", self.BOLD, self.GREEN))
                if desc:
                    for line in self._wrap(desc, indent=6):
                        print(self.style(f"      {line}", self.DIM))
                print("")

            nav = ["Enter number"]
            if allow_back:
                nav.append("B=Back")
            nav.append("Q=Quit")
            prompt = self.style("Select option", self.BOLD) + f" [{', '.join(nav)}]: "
            raw = input(prompt).strip()
            low = raw.lower()

            if low in {"q", "quit", "exit"}:
                return "__quit__"
            if allow_back and low in {"b", "back"}:
                return "__back__"
            if raw.isdigit():
                idx = int(raw) - 1
                if 0 <= idx < len(options):
                    return options[idx][0]

            print(self.style("\nInvalid selection. Press Enter to continue.", self.RED, self.BOLD))
            input()

    def confirm(self, title: str, subtitle: str, token: str = "CONFIRM") -> bool:
        self.render(title=title, subtitle=subtitle)
        raw = input(self.style(f"Type {token} to continue (or press Enter to cancel): ", self.BOLD)).strip()
        return raw == token

    def pause(self, msg: str = "Press Enter to continue.") -> None:
        input(self.style(msg, self.DIM))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Open the TiDB PoC control panel")
    p.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    p.add_argument("--runner", default="run_all.sh", help="Path to run_all.sh")
    return p.parse_args()


def load_cfg(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_cfg(path: Path, cfg: Dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)


def status_lines(cfg: Dict, config_path: Path) -> List[str]:
    tier = (cfg.get("tier") or {}).get("selected", "serverless")
    scenario = (cfg.get("pre_poc") or {}).get("scenario_template", "oltp_migration")
    report_state = "ready" if REPORT_PDF.exists() else "not generated"
    return [
        f"Config: {config_path}",
        f"Tier: {tier} ({TIER_LABELS.get(tier, tier)})",
        f"Scenario: {SCENARIOS.get(scenario, scenario)}",
        f"Report PDF: {report_state}",
    ]


def run_cmd(cmd: List[str], cwd: Path) -> int:
    return subprocess.call(cmd, cwd=str(cwd))


def run_defaults(ui: UI, runner: Path, config_path: Path) -> None:
    ui.render(
        title="Run PoC with Defaults",
        subtitle="This runs the kit using current config values and module defaults.",
        bullets=[
            "Uses current tier/security choices in config.",
            "Skips intake wizard and runs directly.",
        ],
    )
    ui.pause("Press Enter to start run, or Ctrl+C to cancel.")

    cmd = [str(runner), str(config_path), "--no-menu", "--no-wizard"]
    rc = run_cmd(cmd, ROOT)
    if rc == 0:
        print(ui.style("\nPoC run completed successfully.", ui.GREEN, ui.BOLD))
    else:
        print(ui.style(f"\nPoC run exited with code {rc}.", ui.YELLOW, ui.BOLD))
    ui.pause()


def choose_tier_manual(ui: UI, config_path: Path, tier: str) -> None:
    cfg = load_cfg(config_path)
    cfg.setdefault("modules", {})
    cfg.setdefault("test", {})
    cfg.setdefault("tier", {})
    cfg.setdefault("pre_poc", {})

    scenario = cfg["pre_poc"].get("scenario_template", "oltp_migration")
    if scenario not in SCENARIOS:
        scenario = "oltp_migration"

    run_ha_sim = False
    enable_optional_advanced = False

    if tier in {"dedicated", "byoc"}:
        opt = ui.menu(
            title=f"{TIER_LABELS[tier]} Options",
            subtitle="Enable optional advanced modules?",
            options=[
                ("yes", "Enable", "Enable optional advanced modules where supported."),
                ("no", "Disable", "Keep optional advanced modules disabled."),
            ],
            allow_back=True,
        )
        if opt in {"__quit__", "__back__"}:
            return
        enable_optional_advanced = (opt == "yes")
    else:
        opt_ha = ui.menu(
            title=f"{TIER_LABELS[tier]} Options",
            subtitle="Enable simulated HA module (M3) on this non-Dedicated tier?",
            options=[
                ("no", "No", "Keep M3 disabled."),
                ("yes", "Yes", "Enable simulated HA probe."),
            ],
            allow_back=True,
        )
        if opt_ha in {"__quit__", "__back__"}:
            return
        run_ha_sim = (opt_ha == "yes")

        opt_adv = ui.menu(
            title=f"{TIER_LABELS[tier]} Options",
            subtitle="Enable optional advanced modules (may require TiFlash/tier features)?",
            options=[
                ("no", "No", "Keep optional advanced modules disabled."),
                ("yes", "Yes", "Enable optional advanced modules."),
            ],
            allow_back=True,
        )
        if opt_adv in {"__quit__", "__back__"}:
            return
        enable_optional_advanced = (opt_adv == "yes")

    modules = build_tier_modules(
        tier=tier,
        scenario=scenario,
        run_ha_sim=run_ha_sim,
        enable_optional_advanced=enable_optional_advanced,
        existing=cfg.get("modules", {}),
    )

    apply_profile = ui.menu(
        title="Apply Tier Profile",
        subtitle="Apply recommended test scale/duration/concurrency for this tier?",
        options=[
            ("yes", "Apply profile", "Use recommended tier profile values."),
            ("no", "Keep current test values", "Do not overwrite test block."),
        ],
        allow_back=True,
    )
    if apply_profile in {"__quit__", "__back__"}:
        return

    cfg["modules"] = modules
    if apply_profile == "yes":
        cfg["test"].update(tier_test_profile(tier))

    cfg["tier"].update(
        {
            "selected": tier,
            "recommended": tier,
            "decision_tree_version": "manual-control-panel",
            "decision_notes": ["Manually selected from control panel."],
        }
    )

    save_cfg(config_path, cfg)
    print(ui.style(f"\nSaved tier configuration: {tier} ({TIER_LABELS[tier]}).", ui.GREEN, ui.BOLD))
    ui.pause()


def tier_help_screen(ui: UI) -> None:
    ui.render(
        title="Cloud Tier Guide",
        subtitle="Quick reference for tier selection.",
        bullets=[
            "Serverless (Starter): fastest pilot/dev path with minimal setup.",
            "Essential: production baseline when PITR/regional failover is needed.",
            "Premium: stronger enterprise controls and broader CDC suitability.",
            "Dedicated: needed for VPC peering, 90-day backup retention, full HA tests.",
            "BYOC: deployment inside your cloud account/VPC for sovereignty/compliance/IAM/KMS control.",
            "Dedicated cluster option is fully supported in this menu.",
        ],
    )
    ui.pause()


def choose_cloud_tier(ui: UI, config_path: Path) -> None:
    while True:
        choice = ui.menu(
            title="Choose Cloud Tier",
            subtitle="Select a tier directly or run the guided decision-tree flow.",
            options=[
                ("guided", "Guided Recommendation (decision tree)", "Walk through gates and apply tier/module profile."),
                ("serverless", f"{TIER_LABELS['serverless']}", "Fast pilot path."),
                ("essential", f"{TIER_LABELS['essential']}", "Balanced production baseline."),
                ("premium", f"{TIER_LABELS['premium']}", "Enterprise controls."),
                ("dedicated", f"{TIER_LABELS['dedicated']}", "Dedicated cluster path."),
                ("byoc", f"{TIER_LABELS['byoc']}", "Own-account deployment path."),
                ("help", "Tier Help Guide", "Show quick gating and capability guidance."),
                ("back", "Back", "Return to main menu."),
            ],
            allow_back=True,
        )

        if choice in {"__quit__", "back", "__back__"}:
            return

        if choice == "help":
            tier_help_screen(ui)
            continue

        if choice == "guided":
            cmd = [
                sys.executable,
                str(ROOT / "setup" / "pre_poc_intake.py"),
                "--mode",
                "tier",
                "--config",
                str(config_path),
                "--output-config",
                str(config_path),
                "--output-json",
                str(RESULTS_DIR / "pre_poc_intake.json"),
                "--output-md",
                str(RESULTS_DIR / "pre_poc_checklist.md"),
            ]
            run_cmd(cmd, ROOT)
            ui.pause()
            continue

        if choice in TIERS:
            choose_tier_manual(ui, config_path, choice)
            continue


def run_security_screener(ui: UI, config_path: Path) -> None:
    cmd = [
        sys.executable,
        str(ROOT / "setup" / "pre_poc_intake.py"),
        "--mode",
        "security",
        "--config",
        str(config_path),
        "--output-config",
        str(config_path),
        "--output-json",
        str(RESULTS_DIR / "pre_poc_intake.json"),
        "--output-md",
        str(RESULTS_DIR / "pre_poc_checklist.md"),
    ]
    run_cmd(cmd, ROOT)
    ui.pause()


def open_or_print_report(ui: UI) -> None:
    if not REPORT_PDF.exists():
        ui.render(
            title="Report Not Available",
            subtitle="Print/Open PDF is available only after a completed PoC run.",
            bullets=[
                f"Expected file: {REPORT_PDF}",
                "Run PoC with defaults first.",
            ],
        )
        ui.pause()
        return

    options = [("show", "Show report path", "Display report path only.")]
    if shutil.which("open"):
        options.append(("open", "Open PDF", "Open report in default PDF viewer."))
    if shutil.which("lp"):
        options.append(("print", "Send to printer", "Send report to default printer via lp."))
    options.append(("back", "Back", "Return to main menu."))

    choice = ui.menu(
        title="Report Actions",
        subtitle=f"Report is ready: {REPORT_PDF}",
        options=options,
        allow_back=True,
    )

    if choice in {"__quit__", "back", "__back__"}:
        return
    if choice == "show":
        print(f"\nReport: {REPORT_PDF}")
    elif choice == "open":
        subprocess.call(["open", str(REPORT_PDF)])
    elif choice == "print":
        subprocess.call(["lp", str(REPORT_PDF)])
    ui.pause()


def clear_local_results() -> None:
    if not RESULTS_DIR.exists():
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        return
    for child in RESULTS_DIR.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink(missing_ok=True)


def drop_configured_database(config_path: Path) -> Tuple[bool, str]:
    cfg = load_cfg(config_path)
    tidb = cfg.get("tidb") or {}

    host = tidb.get("host")
    user = tidb.get("user")
    password = tidb.get("password")
    database = tidb.get("database")
    port = int(tidb.get("port", 4000))
    ssl = bool(tidb.get("ssl", True))

    if not all([host, user, password, database]):
        return False, "Missing one or more required TiDB fields (host/user/password/database)."

    try:
        import mysql.connector

        ssl_args = {"ssl_disabled": False} if ssl else {"ssl_disabled": True}
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            connection_timeout=30,
            **ssl_args,
        )
        cur = conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS `{database}`")
        conn.close()
        return True, f"Dropped database `{database}`."
    except Exception as e:  # pragma: no cover
        return False, str(e)


def clear_poc_data(ui: UI, config_path: Path) -> None:
    ok = ui.confirm(
        title="Clear PoC Data",
        subtitle="This deletes local run artifacts in results/. Database data can be dropped in the next step.",
        token="CLEAR",
    )
    if not ok:
        print(ui.style("\nCancelled.", ui.YELLOW))
        ui.pause()
        return

    clear_local_results()
    print(ui.style("\nCleared local results artifacts.", ui.GREEN, ui.BOLD))

    drop_choice = ui.menu(
        title="Database Reset",
        subtitle="Do you also want to drop the configured PoC database?",
        options=[
            ("no", "No", "Keep database as-is."),
            ("yes", "Yes", "Drop configured database now."),
        ],
        allow_back=False,
    )

    if drop_choice == "yes":
        ok2 = ui.confirm(
            title="Confirm Database Drop",
            subtitle="This will execute DROP DATABASE IF EXISTS on the configured database.",
            token="DROP",
        )
        if ok2:
            success, msg = drop_configured_database(config_path)
            if success:
                print(ui.style(f"\n{msg}", ui.GREEN, ui.BOLD))
            else:
                print(ui.style(f"\nDatabase reset failed: {msg}", ui.RED, ui.BOLD))

    ui.pause()


def main() -> int:
    args = parse_args()

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = (ROOT / config_path).resolve()

    runner = Path(args.runner)
    if not runner.is_absolute():
        runner = (ROOT / runner).resolve()

    if not config_path.exists():
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 1

    ui = UI()

    while True:
        cfg = load_cfg(config_path)
        report_ready = REPORT_PDF.exists() and RESULTS_DB.exists()

        choice = ui.menu(
            title="Main Menu",
            subtitle="Choose an action for the PoC workflow.",
            status=status_lines(cfg, config_path),
            options=[
                ("run", "Run PoC with Defaults", "Execute PoC immediately using current config and defaults."),
                ("tier", "Choose Cloud Tier", "Guided or manual tier selection (includes Dedicated option)."),
                ("security", "Security Screener", "Run shared-responsibility security checklist."),
                (
                    "report",
                    "Print/Open PDF Report",
                    "Available after completed PoC." if report_ready else "Currently unavailable: complete PoC first.",
                ),
                ("clear", "Clear PoC Data", "Clear results artifacts and optionally drop the PoC database."),
                ("exit", "Exit", "Leave control panel."),
            ],
            allow_back=False,
        )

        if choice in {"__quit__", "exit"}:
            return 0

        if choice == "run":
            run_defaults(ui, runner, config_path)
            continue

        if choice == "tier":
            choose_cloud_tier(ui, config_path)
            continue

        if choice == "security":
            run_security_screener(ui, config_path)
            continue

        if choice == "report":
            open_or_print_report(ui)
            continue

        if choice == "clear":
            clear_poc_data(ui, config_path)
            continue


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCancelled.")
        raise SystemExit(130)

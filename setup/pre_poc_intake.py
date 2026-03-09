#!/usr/bin/env python3
"""
Pre-PoC intake wizard for TiDB Cloud self-service kit.

What it does:
1. Runs a fast tier decision tree (Serverless/Essential/Premium/Dedicated/BYOC)
2. Captures Security Architecture + Shared Responsibility checklist responses
3. Builds a go/no-go recommendation for continuing the PoC
4. Writes a resolved config with tier-aware module defaults
5. Writes JSON + Markdown intake artifacts under results/
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import sys
import textwrap
from datetime import datetime, timezone
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


TIERS = ["serverless", "essential", "premium", "dedicated", "byoc"]
TIER_LABELS = {
    "serverless": "Serverless (Starter)",
    "essential": "Essential",
    "premium": "Premium",
    "dedicated": "Dedicated",
    "byoc": "BYOC",
}

SCENARIOS = {
    "oltp_migration": "OLTP migration (default)",
    "htap_analytics": "HTAP + analytics",
    "ai_vector": "AI/vector search",
}

COMMON_ISSUES = [
    {
        "issue": "Network access not allowlisted",
        "avoid": "Pre-add the runner IP in TiDB Cloud Security -> Network Access before execution.",
    },
    {
        "issue": "Tier/module mismatch",
        "avoid": "Use tier-aware defaults: disable M3 HA node-failure and M4 HTAP unless Dedicated/BYOC prerequisites are met.",
    },
    {
        "issue": "TiFlash not ready",
        "avoid": "Provision TiFlash nodes first and wait for replica progress to reach 100% before enabling HTAP/vector modules.",
    },
    {
        "issue": "IMPORT INTO environment mismatch",
        "avoid": "Use S3-based import paths for cloud runs; rely on automatic LOAD DATA/INSERT fallback otherwise.",
    },
    {
        "issue": "Data generation takes too long",
        "avoid": "Start with tier-appropriate scale (Serverless: small) and increase only after baseline validation.",
    },
]

SECURITY_ITEMS = [
    {
        "id": "shared_responsibility_ack",
        "prompt": "Has the team reviewed and accepted the shared responsibility model (customer vs PingCAP boundaries)?",
        "blocking": True,
        "applies_to": "all",
        "owner": "Customer",
    },
    {
        "id": "data_residency_boundary",
        "prompt": "Can this PoC keep production-like data/backups within approved account and region boundaries?",
        "blocking": True,
        "applies_to": "all",
        "owner": "Customer",
    },
    {
        "id": "network_controls",
        "prompt": "Can your team manage VPC, Security Group, and NACL controls needed for this deployment?",
        "blocking": True,
        "applies_to": ["dedicated", "byoc"],
        "owner": "Customer",
    },
    {
        "id": "private_connectivity",
        "prompt": "Can you enforce private connectivity (Private Endpoint/PrivateLink or VPC peering where required) without public routing?",
        "blocking": True,
        "applies_to": "all",
        "owner": "Customer",
    },
    {
        "id": "cmk_control",
        "prompt": "Can your security team manage AWS KMS CMKs and key lifecycle controls for data at rest?",
        "blocking": True,
        "applies_to": ["premium", "dedicated", "byoc"],
        "owner": "Customer",
    },
    {
        "id": "iam_role_hygiene",
        "prompt": "Can elevated provisioning IAM roles be removed/disabled after bootstrap (least-privilege runtime only)?",
        "blocking": True,
        "applies_to": ["dedicated", "byoc"],
        "owner": "Customer",
    },
    {
        "id": "jit_access",
        "prompt": "Can support access be enforced as customer-controlled Just-in-Time (bastion + VPN enablement + scoped role)?",
        "blocking": True,
        "applies_to": ["dedicated", "byoc"],
        "owner": "Customer",
    },
    {
        "id": "auditability",
        "prompt": "Can CloudTrail and TiDB audit logs be retained in customer-controlled storage/SIEM for auditability?",
        "blocking": True,
        "applies_to": "all",
        "owner": "Customer",
    },
    {
        "id": "supply_chain_scan",
        "prompt": "Will your security team scan images/artifacts before deployment where applicable?",
        "blocking": False,
        "applies_to": ["dedicated", "byoc"],
        "owner": "Customer",
    },
    {
        "id": "continuity_expectation",
        "prompt": "Is the team aligned that data-plane availability can continue even if control-plane automation is temporarily unavailable?",
        "blocking": False,
        "applies_to": "all",
        "owner": "Shared",
    },
]

DECISION_ORDER = [
    "need_customer_vpc",
    "need_vpc_peering",
    "need_pitr",
    "need_backup_90d",
    "need_regional_ha",
    "need_cdc",
    "allow_essential_cdc_whitelist",
    "need_enterprise_controls",
]

DECISION_META = {
    "need_customer_vpc": {
        "title": "Step 1 - Data Sovereignty / Deployment Control",
        "question": "Do you need TiDB deployed inside your cloud account/VPC for sovereignty, compliance, or your own IAM/KMS/spend?",
        "yes_desc": "Select BYOC immediately.",
        "no_desc": "Continue to network requirements.",
    },
    "need_vpc_peering": {
        "title": "Step 2 - Network Connectivity",
        "question": "Do you require VPC peering (not just Private Endpoint/PrivateLink)?",
        "yes_desc": "Select Dedicated.",
        "no_desc": "Continue to DR/backup requirements.",
    },
    "need_pitr": {
        "title": "Step 3 - DR and Backup",
        "question": "Do you need point-in-time restore (PITR)?",
        "yes_desc": "Minimum tier becomes Essential.",
        "no_desc": "Continue to HA requirement.",
    },
    "need_backup_90d": {
        "title": "Step 3a - Backup Retention",
        "question": "Do you need longer backup retention (up to 90 days)?",
        "yes_desc": "Select Dedicated.",
        "no_desc": "Continue to next requirement.",
    },
    "need_regional_ha": {
        "title": "Step 4 - HA Requirement",
        "question": "Is this production and requires cross-AZ (regional) failover?",
        "yes_desc": "Minimum tier becomes Essential.",
        "no_desc": "Serverless remains valid if no other hard gates.",
    },
    "need_cdc": {
        "title": "Step 5 - Data Movement / CDC",
        "question": "Do you need Changefeed / CDC (Kafka, MySQL, etc.)?",
        "yes_desc": "Premium+ is usually preferred.",
        "no_desc": "Continue to enterprise controls.",
    },
    "allow_essential_cdc_whitelist": {
        "title": "Step 5a - Essential CDC Constraint",
        "question": "If needed, can you proceed with Essential CDC whitelist dependency?",
        "yes_desc": "Essential may remain acceptable.",
        "no_desc": "Prefer Premium+.",
    },
    "need_enterprise_controls": {
        "title": "Step 6 - Enterprise Controls",
        "question": "Do you need enterprise controls (maintenance window/CMEK/audit governance)?",
        "yes_desc": "Select Premium or higher.",
        "no_desc": "Keep the currently derived tier.",
    },
}


class WizardUI:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    MAGENTA = "\033[35m"

    def __init__(self, interactive: bool):
        self.interactive = interactive
        self.use_color = interactive and self._supports_color()
        width = shutil.get_terminal_size((100, 30)).columns
        self.width = max(72, min(width, 110))

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
        if not self.interactive:
            return
        if os.name == "nt":
            os.system("cls")
        else:
            print("\033[2J\033[H", end="")

    def _wrap(self, text: str, indent: int = 0) -> List[str]:
        width = self.width - 4 - indent
        return textwrap.wrap(text, width=max(24, width)) or [""]

    def render(
        self,
        title: str,
        subtitle: str = "",
        step: str = "",
        bullets: List[str] | None = None,
    ) -> None:
        self.clear()
        bar = "=" * self.width
        print(self.style(bar, self.CYAN))
        print(self.style(" TiDB Cloud Pre-PoC Intake Wizard ", self.BOLD, self.CYAN))
        if step:
            print(self.style(f" {step}", self.DIM))
        print(self.style(bar, self.CYAN))
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
                if wrapped:
                    print(f"  - {wrapped[0]}")
                    for extra in wrapped[1:]:
                        print(f"    {extra}")
        print("")

    def menu(
        self,
        title: str,
        subtitle: str,
        options: List[Tuple[str, str, str]],
        default_idx: int = 0,
        allow_back: bool = False,
        allow_quit: bool = True,
        step: str = "",
        bullets: List[str] | None = None,
    ) -> str:
        while True:
            self.render(title=title, subtitle=subtitle, step=step, bullets=bullets)

            for i, (_, label, desc) in enumerate(options, 1):
                line = self.style(f"  {i}. {label}", self.BOLD, self.GREEN)
                if i - 1 == default_idx:
                    line += self.style("  (default)", self.DIM)
                print(line)
                if desc:
                    for wrapped in self._wrap(desc, indent=6):
                        print(self.style(f"      {wrapped}", self.DIM))
                print("")

            nav = []
            if allow_back:
                nav.append("B=Back")
            if allow_quit:
                nav.append("Q=Quit")
            nav.append("Enter=Default")

            prompt = self.style("Select option", self.BOLD) + f" [{', '.join(nav)}]: "
            raw = input(prompt).strip()
            low = raw.lower()

            if raw == "":
                return options[default_idx][0]

            if allow_back and low in {"b", "back"}:
                return "__back__"

            if allow_quit and low in {"q", "quit", "exit"}:
                return "__quit__"

            if raw.isdigit():
                idx = int(raw) - 1
                if 0 <= idx < len(options):
                    return options[idx][0]

            print(self.style("\nInvalid selection. Press Enter to try again.", self.RED, self.BOLD))
            input()

    def yes_no(
        self,
        title: str,
        question: str,
        default: bool,
        allow_back: bool,
        step: str,
        yes_desc: str,
        no_desc: str,
        bullets: List[str] | None = None,
    ) -> str:
        options = [
            ("yes", "Yes", yes_desc),
            ("no", "No", no_desc),
        ]
        default_idx = 0 if default else 1
        return self.menu(
            title=title,
            subtitle=question,
            options=options,
            default_idx=default_idx,
            allow_back=allow_back,
            allow_quit=True,
            step=step,
            bullets=bullets,
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run pre-PoC intake and output resolved config")
    p.add_argument("--config", required=True, help="Input config.yaml path")
    p.add_argument("--output-config", required=True, help="Resolved config output path")
    p.add_argument("--output-json", required=False, help="Intake JSON output path")
    p.add_argument("--output-md", required=False, help="Intake Markdown output path")
    p.add_argument(
        "--mode",
        choices=["full", "tier", "security"],
        default="full",
        help="Wizard mode: full intake, tier setup only, or security screener only.",
    )
    p.add_argument("--tier", choices=TIERS, help="Force tier selection")
    p.add_argument("--non-interactive", action="store_true", help="Skip prompts and use defaults")
    p.add_argument("--allow-blocked", action="store_true", help="Proceed even with blocking checklist failures")
    return p.parse_args()


def load_yaml(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def dump_yaml(path: str, payload: Dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, sort_keys=False)


def parse_existing_scenario(cfg: Dict) -> str:
    maybe = cfg.get("pre_poc", {}).get("scenario_template")
    return maybe if maybe in SCENARIOS else "oltp_migration"


def parse_default_decision_tree(cfg: Dict) -> Dict[str, bool]:
    defaults = {
        "need_customer_vpc": False,
        "need_vpc_peering": False,
        "need_pitr": False,
        "need_backup_90d": False,
        "need_regional_ha": False,
        "need_cdc": False,
        "allow_essential_cdc_whitelist": False,
        "need_enterprise_controls": False,
    }
    dt = cfg.get("pre_poc", {}).get("decision_tree", {})
    for key in defaults:
        if isinstance(dt.get(key), bool):
            defaults[key] = dt[key]
    return defaults


def active_decision_steps(ans: Dict[str, bool]) -> List[str]:
    steps = ["need_customer_vpc"]
    if ans.get("need_customer_vpc"):
        return steps

    steps.extend(["need_vpc_peering", "need_pitr"])
    if ans.get("need_pitr"):
        steps.append("need_backup_90d")

    steps.extend(["need_regional_ha", "need_cdc"])
    if ans.get("need_cdc"):
        steps.append("allow_essential_cdc_whitelist")

    steps.append("need_enterprise_controls")
    return steps


def normalize_decision_answers(ans: Dict[str, bool], defaults: Dict[str, bool]) -> Dict[str, bool]:
    out = {}
    for key in DECISION_ORDER:
        out[key] = bool(ans.get(key, defaults.get(key, False)))

    if out["need_customer_vpc"]:
        # For BYOC-gated path, remaining feature gates are not required for selection.
        out["need_vpc_peering"] = False
        out["need_backup_90d"] = False
        out["allow_essential_cdc_whitelist"] = False
    else:
        if not out["need_pitr"]:
            out["need_backup_90d"] = False
        if not out["need_cdc"]:
            out["allow_essential_cdc_whitelist"] = False

    return out


def decide_tier(ans: Dict[str, bool]) -> Tuple[str, List[str]]:
    notes: List[str] = []

    if ans["need_customer_vpc"]:
        notes.append("Data sovereignty / own-cloud requirement gates to BYOC.")
        return "byoc", notes

    if ans["need_vpc_peering"]:
        notes.append("VPC peering requirement gates to Dedicated.")
        return "dedicated", notes

    if ans["need_pitr"]:
        if ans["need_backup_90d"]:
            notes.append("PITR + 90-day retention requirement gates to Dedicated.")
            return "dedicated", notes
        candidate = "essential"
        notes.append("PITR required: minimum tier is Essential.")
    else:
        if ans["need_regional_ha"]:
            candidate = "essential"
            notes.append("Regional failover for production implies at least Essential.")
        else:
            candidate = "serverless"
            notes.append("No PITR/regional HA gate: Serverless is valid for pilot/dev.")

    if ans["need_cdc"]:
        if candidate == "serverless":
            candidate = "premium"
            notes.append("CDC requirement upgrades selection to Premium (avoid Essential whitelist dependency).")
        elif candidate == "essential" and not ans["allow_essential_cdc_whitelist"]:
            candidate = "premium"
            notes.append("CDC requested without Essential whitelist assumption -> Premium.")

    if ans["need_enterprise_controls"] and candidate in {"serverless", "essential"}:
        candidate = "premium"
        notes.append("Enterprise controls requirement upgrades selection to Premium.")

    return candidate, notes


def build_tier_modules(
    tier: str,
    scenario: str,
    run_ha_sim: bool,
    enable_optional_advanced: bool,
    existing: Dict[str, bool],
) -> Dict[str, bool]:
    modules = {
        "customer_queries": True,
        "baseline_perf": True,
        "elastic_scale": True,
        "high_availability": False,
        "write_contention": True,
        "htap": False,
        "online_ddl": True,
        "mysql_compat": True,
        "data_import": True,
        "vector_search": False,
    }

    for k, v in (existing or {}).items():
        if isinstance(v, bool):
            modules[k] = v

    if tier in {"dedicated", "byoc"}:
        modules["high_availability"] = True
        modules["htap"] = True
    else:
        modules["high_availability"] = run_ha_sim
        modules["htap"] = enable_optional_advanced

    if scenario == "ai_vector":
        modules["vector_search"] = True

    if not enable_optional_advanced:
        modules["vector_search"] = False

    return modules


def tier_test_profile(tier: str) -> Dict:
    profiles = {
        "serverless": {
            "data_scale": "small",
            "duration_seconds": 120,
            "concurrency_levels": [8, 16, 32],
            "ramp_duration_seconds": 300,
            "warm_phase_enabled": True,
            "warm_phase_duration_seconds": 300,
            "warm_phase_concurrency": 32,
        },
        "essential": {
            "data_scale": "small",
            "duration_seconds": 180,
            "concurrency_levels": [16, 32, 64],
            "ramp_duration_seconds": 600,
            "warm_phase_enabled": True,
            "warm_phase_duration_seconds": 420,
            "warm_phase_concurrency": 64,
        },
        "premium": {
            "data_scale": "small",
            "duration_seconds": 180,
            "concurrency_levels": [16, 32, 64],
            "ramp_duration_seconds": 600,
            "warm_phase_enabled": True,
            "warm_phase_duration_seconds": 420,
            "warm_phase_concurrency": 64,
        },
        "dedicated": {
            "data_scale": "small",
            "duration_seconds": 300,
            "concurrency_levels": [16, 64, 256],
            "ramp_duration_seconds": 1200,
            "warm_phase_enabled": True,
            "warm_phase_duration_seconds": 600,
            "warm_phase_concurrency": 256,
        },
        "byoc": {
            "data_scale": "small",
            "duration_seconds": 300,
            "concurrency_levels": [16, 64, 256],
            "ramp_duration_seconds": 1200,
            "warm_phase_enabled": True,
            "warm_phase_duration_seconds": 600,
            "warm_phase_concurrency": 256,
        },
    }
    return profiles[tier]


def applicable(item: Dict, tier: str) -> bool:
    applies_to = item["applies_to"]
    if applies_to == "all":
        return True
    return tier in applies_to


def summarize_security(items: List[Dict], non_interactive: bool) -> Dict:
    blocking_failures = [r["id"] for r in items if r["status"] == "fail" and r["blocking"]]
    non_blocking_failures = [r["id"] for r in items if r["status"] == "fail" and not r["blocking"]]

    if non_interactive:
        recommendation = "review_required"
        proceed = True
    elif blocking_failures:
        recommendation = "hold"
        proceed = False
    elif non_blocking_failures:
        recommendation = "proceed_with_risks"
        proceed = True
    else:
        recommendation = "proceed"
        proceed = True

    return {
        "items": items,
        "blocking_failures": blocking_failures,
        "non_blocking_failures": non_blocking_failures,
        "recommendation": recommendation,
        "proceed": proceed,
    }


def run_decision_tree_interactive(
    ui: WizardUI,
    defaults: Dict[str, bool],
    existing: Dict[str, bool] | None = None,
) -> Tuple[str, Dict[str, bool]]:
    ans = copy.deepcopy(existing or defaults)
    ans = normalize_decision_answers(ans, defaults)

    idx = 0
    while True:
        steps = active_decision_steps(ans)
        if idx >= len(steps):
            break

        key = steps[idx]
        meta = DECISION_META[key]
        default_val = bool(ans.get(key, defaults.get(key, False)))

        result = ui.yes_no(
            title=meta["title"],
            question=meta["question"],
            default=default_val,
            allow_back=True,
            step=f"Decision Tree {idx + 1}/{len(steps)}",
            yes_desc=meta["yes_desc"],
            no_desc=meta["no_desc"],
            bullets=["Select an option, then continue.", "Use B to go to the previous question."],
        )

        if result == "__quit__":
            return "quit", ans

        if result == "__back__":
            if idx == 0:
                return "back", ans
            idx -= 1
            continue

        ans[key] = (result == "yes")

        if key == "need_customer_vpc" and ans[key]:
            ans["need_vpc_peering"] = False
            ans["need_backup_90d"] = False
            ans["allow_essential_cdc_whitelist"] = False

        if key == "need_pitr" and not ans[key]:
            ans["need_backup_90d"] = False

        if key == "need_cdc" and not ans[key]:
            ans["allow_essential_cdc_whitelist"] = False

        idx += 1

    return "ok", normalize_decision_answers(ans, defaults)


def run_security_checklist_interactive(
    ui: WizardUI,
    tier: str,
    existing_statuses: Dict[str, str] | None = None,
) -> Tuple[str, Dict]:
    existing_statuses = existing_statuses or {}

    applicable_items = [item for item in SECURITY_ITEMS if applicable(item, tier)]
    statuses = {}
    idx = 0

    while idx < len(applicable_items):
        item = applicable_items[idx]
        default_status = existing_statuses.get(item["id"], "pass")
        default_idx = 0 if default_status != "fail" else 1

        options = [
            ("pass", "PASS", "Requirement is met for this PoC."),
            ("fail", "FAIL", "Gap exists; this will affect go/no-go decision."),
        ]

        result = ui.menu(
            title="Security Architecture & Shared Responsibility",
            subtitle=item["prompt"],
            options=options,
            default_idx=default_idx,
            allow_back=True,
            allow_quit=True,
            step=f"Security Checklist {idx + 1}/{len(applicable_items)} | {'Blocking' if item['blocking'] else 'Non-blocking'}",
            bullets=[
                f"Owner: {item['owner']}",
                "Use B to revisit previous control.",
            ],
        )

        if result == "__quit__":
            return "quit", {}

        if result == "__back__":
            if idx == 0:
                return "back", {}
            idx -= 1
            continue

        statuses[item["id"]] = result
        idx += 1

    rows = []
    for item in SECURITY_ITEMS:
        if not applicable(item, tier):
            status = "na"
        else:
            status = statuses.get(item["id"], "pass")

        rows.append(
            {
                "id": item["id"],
                "prompt": item["prompt"],
                "status": status,
                "blocking": item["blocking"],
                "owner": item["owner"],
            }
        )

    return "ok", summarize_security(rows, non_interactive=False)


def run_security_checklist_non_interactive(tier: str) -> Dict:
    rows = []
    for item in SECURITY_ITEMS:
        if applicable(item, tier):
            status = "not_assessed"
        else:
            status = "na"

        rows.append(
            {
                "id": item["id"],
                "prompt": item["prompt"],
                "status": status,
                "blocking": item["blocking"],
                "owner": item["owner"],
            }
        )

    return summarize_security(rows, non_interactive=True)


def choose_scenario_interactive(ui: WizardUI, default_scenario: str) -> str:
    options = [(k, f"{k}: {v}", "") for k, v in SCENARIOS.items()]
    default_idx = list(SCENARIOS.keys()).index(default_scenario)
    return ui.menu(
        title="PoC Scenario Template",
        subtitle="Choose the scenario to shape defaults and module emphasis.",
        options=options,
        default_idx=default_idx,
        allow_back=False,
        allow_quit=True,
        step="Section 1/6",
        bullets=["This can be changed later before finalizing."],
    )


def choose_tier_interactive(
    ui: WizardUI,
    recommended_tier: str,
    selected_tier: str,
    decision_notes: List[str],
) -> str:
    options = []
    for t in TIERS:
        label = f"{t}: {TIER_LABELS[t]}"
        desc = ""
        if t == recommended_tier:
            label += " (recommended)"
            desc = "Best fit based on the decision tree responses."
        elif t == "serverless":
            desc = "Fastest low-friction pilot path."
        elif t in {"dedicated", "byoc"}:
            desc = "Needed for stricter network/control requirements."
        options.append((t, label, desc))

    default_idx = TIERS.index(selected_tier if selected_tier in TIERS else recommended_tier)
    bullets = decision_notes or ["No additional gating notes were produced."]
    return ui.menu(
        title="Tier Selection",
        subtitle="Review recommendation and choose the tier to run now.",
        options=options,
        default_idx=default_idx,
        allow_back=True,
        allow_quit=True,
        step="Section 3/6",
        bullets=bullets,
    )


def choose_advanced_options_interactive(
    ui: WizardUI,
    selected_tier: str,
    scenario: str,
    run_ha_sim: bool,
    enable_optional_advanced: bool,
) -> Tuple[str, bool, bool]:
    if selected_tier in {"dedicated", "byoc"}:
        result = ui.yes_no(
            title="Advanced Module Toggle",
            question="Enable optional advanced modules (including vector search when supported)?",
            default=enable_optional_advanced,
            allow_back=True,
            step="Section 4/6",
            yes_desc="Enable optional advanced modules.",
            no_desc="Keep optional advanced modules disabled.",
            bullets=["Dedicated/BYOC already enables HA and HTAP by default."],
        )
        if result in {"__back__", "__quit__"}:
            return result, run_ha_sim, enable_optional_advanced
        return "ok", run_ha_sim, (result == "yes")

    result_ha = ui.yes_no(
        title="HA Behavior on Non-Dedicated Tiers",
        question="Enable Module 3 simulated HA probe on this tier?",
        default=run_ha_sim,
        allow_back=True,
        step="Section 4/6 | Option 1/2",
        yes_desc="Run simulated HA probe.",
        no_desc="Keep Module 3 disabled for this run.",
        bullets=["Full node-stop HA validation is Dedicated/BYOC only."],
    )
    if result_ha in {"__back__", "__quit__"}:
        return result_ha, run_ha_sim, enable_optional_advanced

    default_adv = enable_optional_advanced or (scenario == "ai_vector")
    result_adv = ui.yes_no(
        title="Optional Advanced Modules",
        question="Enable optional advanced modules that may require TiFlash or tier-specific features?",
        default=default_adv,
        allow_back=True,
        step="Section 4/6 | Option 2/2",
        yes_desc="Enable optional advanced modules.",
        no_desc="Leave optional advanced modules disabled.",
        bullets=["This affects HTAP/vector module defaults."],
    )
    if result_adv == "__quit__":
        return "__quit__", run_ha_sim, enable_optional_advanced
    if result_adv == "__back__":
        return "back_to_ha", run_ha_sim, enable_optional_advanced

    return "ok", (result_ha == "yes"), (result_adv == "yes")


def choose_apply_profile_interactive(ui: WizardUI, default: bool) -> str:
    return ui.yes_no(
        title="Test Profile",
        question="Apply tier-specific recommended test profile (scale, duration, concurrency)?",
        default=default,
        allow_back=True,
        step="Section 6/6",
        yes_desc="Apply tier defaults for this run.",
        no_desc="Keep existing test values from config.",
        bullets=["You can still edit config manually afterward."],
    )


def review_and_confirm_interactive(
    ui: WizardUI,
    scenario: str,
    recommended_tier: str,
    selected_tier: str,
    security: Dict,
    include_security: bool = True,
    include_decision: bool = True,
) -> str:
    bullets = [
        f"Scenario: {SCENARIOS.get(scenario, scenario)}",
        f"Recommended tier: {TIER_LABELS.get(recommended_tier, recommended_tier)}",
        f"Selected tier: {TIER_LABELS.get(selected_tier, selected_tier)}",
        "Choose continue to write resolved config and artifacts.",
    ]
    if include_security:
        bullets.insert(3, f"Checklist recommendation: {security.get('recommendation')}")
        bullets.insert(4, f"Blocking failures: {len(security.get('blocking_failures', []))}")

    options = [
        ("continue", "Continue", "Write resolved config and checklist artifacts."),
        ("edit_tier", "Edit tier selection", "Choose a different tier."),
    ]
    if include_decision:
        options.insert(1, ("edit_decision", "Edit decision tree", "Return to requirement questions."))
    if include_security:
        options.append(("edit_security", "Edit security checklist", "Re-answer security controls."))
    options.append(("cancel", "Cancel wizard", "Abort without writing updates."))

    return ui.menu(
        title="Review and Continue",
        subtitle="Final check before generating outputs.",
        options=options,
        default_idx=0,
        allow_back=True,
        allow_quit=False,
        step="Final",
        bullets=bullets,
    )


def write_markdown(path: str, payload: Dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("# TiDB Cloud Pre-PoC Intake Checklist\n\n")
        f.write(f"Generated: {payload['generated_at']}\n\n")

        f.write("## Tier Decision\n\n")
        f.write(f"- Recommended tier: **{payload['recommended_tier_label']}**\n")
        f.write(f"- Selected tier: **{payload['selected_tier_label']}**\n")
        f.write(f"- Scenario template: **{payload['scenario_label']}**\n")
        f.write("\nDecision reasoning:\n")
        for n in payload["decision_notes"]:
            f.write(f"- {n}\n")

        f.write("\nDecision tree responses:\n")
        for k, v in payload["decision_tree"].items():
            f.write(f"- `{k}`: `{v}`\n")

        f.write("\n## Security Architecture & Shared Responsibility\n\n")
        f.write(f"- Recommendation: **{payload['security']['recommendation']}**\n")
        f.write(f"- Proceed: **{payload['security']['proceed']}**\n")
        if payload["security"]["blocking_failures"]:
            f.write("- Blocking failures:\n")
            for item_id in payload["security"]["blocking_failures"]:
                f.write(f"  - `{item_id}`\n")

        f.write("\nChecklist results:\n")
        for item in payload["security"]["items"]:
            status = item["status"].upper()
            block = "blocking" if item["blocking"] else "non-blocking"
            f.write(f"- [{status}] `{item['id']}` ({block}, owner: {item['owner']}): {item['prompt']}\n")

        f.write("\n## Tier-Adjusted Module Plan\n\n")
        for k, v in payload["resolved_modules"].items():
            f.write(f"- `{k}`: `{str(v).lower()}`\n")

        f.write("\n## Common Issues To Avoid\n\n")
        for row in COMMON_ISSUES:
            f.write(f"- **{row['issue']}**: {row['avoid']}\n")


def main() -> int:
    args = parse_args()
    cfg = load_yaml(args.config)

    interactive = (not args.non_interactive) and sys.stdin.isatty()
    ui = WizardUI(interactive=interactive)

    print("\n=== TiDB Cloud Pre-PoC Intake ===")

    default_scenario = parse_existing_scenario(cfg)
    default_tree = parse_default_decision_tree(cfg)

    scenario = default_scenario
    ans = copy.deepcopy(default_tree)
    recommended_tier = "serverless"
    decision_notes: List[str] = []
    selected_tier = cfg.get("tier", {}).get("selected") if cfg.get("tier", {}).get("selected") in TIERS else "serverless"
    run_ha_sim = False
    enable_optional_advanced = False
    security: Dict = {}
    apply_profile = True

    mode = args.mode

    if interactive and not args.tier:
        if mode == "security":
            section = "security"
            ans = normalize_decision_answers(default_tree, default_tree)
            recommended_tier, decision_notes = decide_tier(ans)
            if selected_tier not in TIERS:
                selected_tier = recommended_tier
            apply_profile = False
        else:
            section = "scenario"

        security_statuses: Dict[str, str] = {}
        existing_security = cfg.get("pre_poc", {}).get("security")
        if isinstance(existing_security, dict):
            security = existing_security
        else:
            security = run_security_checklist_non_interactive(selected_tier)

        while True:
            if section == "scenario":
                result = choose_scenario_interactive(ui, scenario)
                if result == "__quit__":
                    print("\nCancelled.")
                    return 130
                scenario = result
                section = "decision"
                continue

            if section == "decision":
                status, out = run_decision_tree_interactive(ui, default_tree, existing=ans)
                if status == "quit":
                    print("\nCancelled.")
                    return 130
                if status == "back":
                    section = "scenario"
                    continue
                ans = out
                recommended_tier, decision_notes = decide_tier(ans)
                if selected_tier not in TIERS:
                    selected_tier = recommended_tier
                section = "tier"
                continue

            if section == "tier":
                result = choose_tier_interactive(ui, recommended_tier, selected_tier, decision_notes)
                if result == "__quit__":
                    print("\nCancelled.")
                    return 130
                if result == "__back__":
                    section = "decision" if mode != "security" else "security"
                    continue
                selected_tier = result
                section = "advanced"
                continue

            if section == "advanced":
                status, new_ha, new_adv = choose_advanced_options_interactive(
                    ui,
                    selected_tier=selected_tier,
                    scenario=scenario,
                    run_ha_sim=run_ha_sim,
                    enable_optional_advanced=enable_optional_advanced,
                )
                if status == "__quit__":
                    print("\nCancelled.")
                    return 130
                if status == "__back__":
                    section = "tier"
                    continue
                if status == "back_to_ha":
                    continue
                run_ha_sim, enable_optional_advanced = new_ha, new_adv
                section = "security" if mode in {"full", "security"} else "profile"
                continue

            if section == "security":
                status, out = run_security_checklist_interactive(ui, selected_tier, existing_statuses=security_statuses)
                if status == "quit":
                    print("\nCancelled.")
                    return 130
                if status == "back":
                    if mode == "security":
                        print("\nCancelled.")
                        return 130
                    section = "advanced"
                    continue
                security = out
                security_statuses = {r["id"]: r["status"] for r in security.get("items", [])}
                section = "review" if mode == "security" else "profile"
                continue

            if section == "profile":
                result = choose_apply_profile_interactive(ui, default=apply_profile)
                if result == "__quit__":
                    print("\nCancelled.")
                    return 130
                if result == "__back__":
                    section = "security" if mode == "full" else "advanced"
                    continue
                apply_profile = (result == "yes")
                section = "review"
                continue

            if section == "review":
                action = review_and_confirm_interactive(
                    ui,
                    scenario=scenario,
                    recommended_tier=recommended_tier,
                    selected_tier=selected_tier,
                    security=security,
                    include_security=(mode != "tier"),
                    include_decision=(mode != "security"),
                )

                if action == "__back__":
                    section = "security" if mode == "security" else "profile"
                    continue

                if action == "cancel":
                    print("\nCancelled.")
                    return 130

                if action == "edit_decision":
                    section = "decision"
                    continue

                if action == "edit_tier":
                    section = "tier"
                    continue

                if action == "edit_security":
                    section = "security"
                    continue

                if action == "continue":
                    break

    else:
        scenario = default_scenario
        ans = normalize_decision_answers(default_tree, default_tree)
        recommended_tier, decision_notes = decide_tier(ans)

        if args.tier:
            selected_tier = args.tier
        elif cfg.get("tier", {}).get("selected") in TIERS:
            selected_tier = cfg.get("tier", {}).get("selected")
        else:
            selected_tier = recommended_tier

        run_ha_sim = False
        enable_optional_advanced = False

        if mode == "security":
            security = run_security_checklist_non_interactive(selected_tier)
            apply_profile = False
        elif mode == "tier":
            existing_security = cfg.get("pre_poc", {}).get("security")
            if isinstance(existing_security, dict):
                security = existing_security
            else:
                security = run_security_checklist_non_interactive(selected_tier)
            apply_profile = True
        else:
            security = run_security_checklist_non_interactive(selected_tier)
            apply_profile = True

    print(f"\nRecommended tier: {TIER_LABELS.get(recommended_tier, recommended_tier)}")
    print(f"Selected tier   : {TIER_LABELS.get(selected_tier, selected_tier)}")

    if mode == "tier":
        proceed = True
    else:
        proceed = bool(security.get("proceed", True))
        if (not proceed) and args.allow_blocked:
            proceed = True

    resolved = copy.deepcopy(cfg)
    resolved.setdefault("modules", {})
    resolved.setdefault("test", {})
    resolved.setdefault("tier", {})
    resolved.setdefault("pre_poc", {})

    resolved_modules = build_tier_modules(
        tier=selected_tier,
        scenario=scenario,
        run_ha_sim=run_ha_sim,
        enable_optional_advanced=enable_optional_advanced,
        existing=resolved.get("modules", {}),
    )
    resolved["modules"] = resolved_modules

    if "customer_query_validation" in resolved["modules"]:
        resolved["modules"]["customer_query_validation"] = resolved["modules"]["customer_queries"]

    if apply_profile:
        resolved["test"].update(tier_test_profile(selected_tier))

    resolved["tier"].update(
        {
            "selected": selected_tier,
            "recommended": recommended_tier,
            "decision_tree_version": "2026-03-03",
            "decision_notes": decision_notes,
        }
    )

    resolved["pre_poc"].update(
        {
            "scenario_template": scenario,
            "decision_tree": ans,
            "security": security,
            "go_no_go": "proceed" if proceed else "hold",
        }
    )

    generated_at = datetime.now(timezone.utc).isoformat()
    output = {
        "generated_at": generated_at,
        "recommended_tier": recommended_tier,
        "recommended_tier_label": TIER_LABELS.get(recommended_tier, recommended_tier),
        "selected_tier": selected_tier,
        "selected_tier_label": TIER_LABELS.get(selected_tier, selected_tier),
        "scenario": scenario,
        "scenario_label": SCENARIOS.get(scenario, scenario),
        "decision_notes": decision_notes,
        "decision_tree": ans,
        "security": security,
        "go_no_go": "proceed" if proceed else "hold",
        "resolved_modules": resolved_modules,
        "recommended_test_profile": tier_test_profile(selected_tier),
        "common_issues": COMMON_ISSUES,
        "config_in": os.path.abspath(args.config),
        "config_out": os.path.abspath(args.output_config),
    }

    dump_yaml(args.output_config, resolved)
    print(f"Resolved config written: {args.output_config}")

    if args.output_json:
        os.makedirs(os.path.dirname(args.output_json), exist_ok=True)
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        print(f"Intake JSON written   : {args.output_json}")

    if args.output_md:
        write_markdown(args.output_md, output)
        print(f"Checklist markdown    : {args.output_md}")

    print("\nModule plan:")
    for k, v in resolved_modules.items():
        print(f"  - {k}: {str(v).lower()}")

    if proceed:
        print("\nPre-PoC recommendation: PROCEED")
        return 0

    print("\nPre-PoC recommendation: HOLD")
    print("Blocking checklist items require resolution before proceeding.")
    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCancelled.")
        raise SystemExit(130)

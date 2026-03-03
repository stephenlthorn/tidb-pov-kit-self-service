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
import sys
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run pre-PoC intake and output resolved config")
    p.add_argument("--config", required=True, help="Input config.yaml path")
    p.add_argument("--output-config", required=True, help="Resolved config output path")
    p.add_argument("--output-json", required=False, help="Intake JSON output path")
    p.add_argument("--output-md", required=False, help="Intake Markdown output path")
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


def ask_yes_no(prompt: str, default: bool = True, non_interactive: bool = False) -> bool:
    if non_interactive:
        return default
    suffix = "[Y/n]" if default else "[y/N]"
    while True:
        raw = input(f"{prompt} {suffix} ").strip().lower()
        if not raw:
            return default
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("Please answer y or n.")


def ask_choice(prompt: str, options: List[str], default_idx: int = 0, non_interactive: bool = False) -> str:
    if non_interactive:
        return options[default_idx]

    print(f"\n{prompt}")
    for i, opt in enumerate(options, 1):
        marker = " (default)" if i - 1 == default_idx else ""
        print(f"  {i}. {opt}{marker}")

    while True:
        raw = input("Select option number: ").strip()
        if not raw:
            return options[default_idx]
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1]
        print(f"Enter a number between 1 and {len(options)}.")


def decide_tier(ans: Dict[str, bool]) -> Tuple[str, List[str]]:
    notes: List[str] = []

    # Step 1 — Sovereignty / deployment control
    if ans["need_customer_vpc"]:
        notes.append("Data sovereignty / own-cloud requirement gates to BYOC.")
        return "byoc", notes

    # Step 2 — VPC peering requirement
    if ans["need_vpc_peering"]:
        notes.append("VPC peering requirement gates to Dedicated.")
        return "dedicated", notes

    # Baseline candidate from PITR + HA
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

    # Step 5 — CDC
    if ans["need_cdc"]:
        if candidate == "serverless":
            candidate = "premium"
            notes.append("CDC requirement upgrades selection to Premium (avoid Essential whitelist dependency).")
        elif candidate == "essential" and not ans["allow_essential_cdc_whitelist"]:
            candidate = "premium"
            notes.append("CDC requested without Essential whitelist assumption -> Premium.")

    # Step 6 — Enterprise controls
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

    # Preserve any explicit user choices first
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
        modules["vector_search"] = modules.get("vector_search", False)

    return modules


def tier_test_profile(tier: str) -> Dict:
    profiles = {
        "serverless": {
            "data_scale": "small",
            "duration_seconds": 120,
            "concurrency_levels": [8, 16, 32],
            "ramp_duration_seconds": 300,
        },
        "essential": {
            "data_scale": "medium",
            "duration_seconds": 180,
            "concurrency_levels": [16, 32, 64],
            "ramp_duration_seconds": 600,
        },
        "premium": {
            "data_scale": "medium",
            "duration_seconds": 180,
            "concurrency_levels": [16, 32, 64],
            "ramp_duration_seconds": 600,
        },
        "dedicated": {
            "data_scale": "medium",
            "duration_seconds": 300,
            "concurrency_levels": [16, 64, 256],
            "ramp_duration_seconds": 1200,
        },
        "byoc": {
            "data_scale": "medium",
            "duration_seconds": 300,
            "concurrency_levels": [16, 64, 256],
            "ramp_duration_seconds": 1200,
        },
    }
    return profiles[tier]


def applicable(item: Dict, tier: str) -> bool:
    applies_to = item["applies_to"]
    if applies_to == "all":
        return True
    return tier in applies_to


def run_security_checklist(tier: str, non_interactive: bool) -> Dict:
    responses = []
    blocking_failures = []
    non_blocking_failures = []

    print("\nSecurity Architecture & Shared Responsibility checklist")
    print("Answer each item based on current environment readiness.")

    for item in SECURITY_ITEMS:
        if not applicable(item, tier):
            responses.append(
                {
                    "id": item["id"],
                    "prompt": item["prompt"],
                    "status": "na",
                    "blocking": item["blocking"],
                    "owner": item["owner"],
                }
            )
            continue

        if non_interactive:
            status = "not_assessed"
        else:
            ok = ask_yes_no(item["prompt"], default=True, non_interactive=False)
            status = "pass" if ok else "fail"

        row = {
            "id": item["id"],
            "prompt": item["prompt"],
            "status": status,
            "blocking": item["blocking"],
            "owner": item["owner"],
        }
        responses.append(row)

        if status == "fail":
            if item["blocking"]:
                blocking_failures.append(item["id"])
            else:
                non_blocking_failures.append(item["id"])

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
        "items": responses,
        "blocking_failures": blocking_failures,
        "non_blocking_failures": non_blocking_failures,
        "recommendation": recommendation,
        "proceed": proceed,
    }


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


def parse_existing_scenario(cfg: Dict) -> str:
    maybe = cfg.get("pre_poc", {}).get("scenario_template")
    return maybe if maybe in SCENARIOS else "oltp_migration"


def main() -> int:
    args = parse_args()
    cfg = load_yaml(args.config)

    interactive = (not args.non_interactive) and sys.stdin.isatty()

    print("\n=== TiDB Cloud Pre-PoC Intake ===")
    if interactive:
        print("This will gather tier requirements, security readiness, and module defaults before running the kit.")

    # Scenario template
    default_scenario = parse_existing_scenario(cfg)
    if interactive:
        scenario = ask_choice(
            "Select PoC scenario template:",
            [f"{key}: {label}" for key, label in SCENARIOS.items()],
            default_idx=list(SCENARIOS.keys()).index(default_scenario),
            non_interactive=False,
        ).split(":", 1)[0]
    else:
        scenario = default_scenario

    # Decision tree inputs
    default_tree = {
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
    for k in default_tree:
        if isinstance(dt.get(k), bool):
            default_tree[k] = dt[k]

    if interactive:
        print("\nTier decision tree (fast + feature-gated)")

    ans = {
        "need_customer_vpc": ask_yes_no(
            "Q1: Need TiDB deployed in your own cloud account/VPC for sovereignty/compliance/IAM/KMS/spend control?",
            default=default_tree["need_customer_vpc"],
            non_interactive=not interactive,
        ),
    }

    if ans["need_customer_vpc"]:
        ans["need_vpc_peering"] = False
        ans["need_pitr"] = default_tree["need_pitr"]
        ans["need_backup_90d"] = default_tree["need_backup_90d"]
        ans["need_regional_ha"] = default_tree["need_regional_ha"]
        ans["need_cdc"] = default_tree["need_cdc"]
        ans["allow_essential_cdc_whitelist"] = default_tree["allow_essential_cdc_whitelist"]
        ans["need_enterprise_controls"] = default_tree["need_enterprise_controls"]
    else:
        ans["need_vpc_peering"] = ask_yes_no(
            "Q2: Do you require VPC peering (not only private endpoint/private link)?",
            default=default_tree["need_vpc_peering"],
            non_interactive=not interactive,
        )

        ans["need_pitr"] = ask_yes_no(
            "Q3: Do you need point-in-time restore (PITR)?",
            default=default_tree["need_pitr"],
            non_interactive=not interactive,
        )
        if ans["need_pitr"]:
            ans["need_backup_90d"] = ask_yes_no(
                "Q3a: Do you need longer backup retention (up to 90 days)?",
                default=default_tree["need_backup_90d"],
                non_interactive=not interactive,
            )
        else:
            ans["need_backup_90d"] = False

        ans["need_regional_ha"] = ask_yes_no(
            "Q4: Is this production and requires cross-AZ (regional) failover?",
            default=default_tree["need_regional_ha"],
            non_interactive=not interactive,
        )
        ans["need_cdc"] = ask_yes_no(
            "Q5: Do you need CDC/Changefeed (Kafka/MySQL/etc.)?",
            default=default_tree["need_cdc"],
            non_interactive=not interactive,
        )
        if ans["need_cdc"]:
            ans["allow_essential_cdc_whitelist"] = ask_yes_no(
                "If required, can you proceed with Essential CDC whitelist dependency?",
                default=default_tree["allow_essential_cdc_whitelist"],
                non_interactive=not interactive,
            )
        else:
            ans["allow_essential_cdc_whitelist"] = False

        ans["need_enterprise_controls"] = ask_yes_no(
            "Q6: Need enterprise controls (maintenance window/CMEK/audit governance)?",
            default=default_tree["need_enterprise_controls"],
            non_interactive=not interactive,
        )

    recommended_tier, decision_notes = decide_tier(ans)

    # Selection: recommended by default, serverless as fallback baseline
    if args.tier:
        selected_tier = args.tier
    elif interactive:
        opt_labels = [f"{t}: {TIER_LABELS[t]}" for t in TIERS]
        default_idx = TIERS.index(recommended_tier if recommended_tier in TIERS else "serverless")
        selected_tier = ask_choice(
            "Select tier for this PoC run:",
            opt_labels,
            default_idx=default_idx,
            non_interactive=False,
        ).split(":", 1)[0]
    else:
        selected_tier = cfg.get("tier", {}).get("selected")
        if selected_tier not in TIERS:
            selected_tier = recommended_tier if recommended_tier in TIERS else "serverless"

    print(f"\nRecommended tier: {TIER_LABELS.get(recommended_tier, recommended_tier)}")
    print(f"Selected tier   : {TIER_LABELS.get(selected_tier, selected_tier)}")

    # Optional advanced toggles
    run_ha_sim = False
    enable_optional_advanced = False
    if interactive:
        if selected_tier not in {"dedicated", "byoc"}:
            run_ha_sim = ask_yes_no(
                "Enable Module 3 simulated HA probe on non-Dedicated tier?",
                default=False,
                non_interactive=False,
            )
            enable_optional_advanced = ask_yes_no(
                "Enable optional advanced modules that may require TiFlash or tier-specific features?",
                default=False,
                non_interactive=False,
            )
        else:
            enable_optional_advanced = ask_yes_no(
                "Enable optional advanced modules (including vector search when supported)?",
                default=False,
                non_interactive=False,
            )
    else:
        run_ha_sim = False
        enable_optional_advanced = False

    security = run_security_checklist(selected_tier, non_interactive=not interactive)

    proceed = bool(security["proceed"])
    if (not proceed) and args.allow_blocked:
        proceed = True

    if (not proceed) and interactive:
        print("\nBlocking checklist items were marked FAIL.")
        proceed = ask_yes_no("Continue anyway for a technical dry run?", default=False, non_interactive=False)

    # Build resolved config
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

    # Backward-compatible alias for module 0 toggle
    if "customer_query_validation" in resolved["modules"]:
        resolved["modules"]["customer_query_validation"] = resolved["modules"]["customer_queries"]

    apply_profile = True
    if interactive:
        apply_profile = ask_yes_no(
            "Apply tier-specific recommended test profile (scale/duration/concurrency)?",
            default=True,
            non_interactive=False,
        )
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

    # Short terminal summary
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

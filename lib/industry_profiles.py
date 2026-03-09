"""Industry profile registry shared by UI, workload, and data generation."""

from __future__ import annotations

from typing import Dict, List

INDUSTRY_DEFAULT = "general_auto"

INDUSTRY_PROFILES: Dict[str, Dict] = {
    "general_auto": {
        "label": "General / Auto",
        "description": "Generic multi-workload baseline for broad PoV coverage.",
        "primary_focus": "Balanced OLTP + migration baseline",
        "recommended_modules": [
            "customer_queries",
            "baseline_perf",
            "elastic_scale",
            "write_contention",
            "online_ddl",
            "mysql_compat",
            "data_import",
        ],
        "recommended_scenario": "oltp_migration",
        "workload_family": "general_auto",
        "analytics_family": "general_auto",
        "default_workload_mix": "mixed",
        "schema_recommendation": "tidb_optimized",
        "ddl_target_table": "transactions",
        "ddl_reference_column": "reference_id",
        "htap_tables": ["transactions", "transaction_items"],
    },
    "banking": {
        "label": "Banking",
        "description": "Payments, account balance checks, and transfer-heavy OLTP.",
        "primary_focus": "Low-latency account and payment processing",
        "recommended_modules": [
            "customer_queries",
            "baseline_perf",
            "elastic_scale",
            "high_availability",
            "write_contention",
            "online_ddl",
            "mysql_compat",
            "data_import",
        ],
        "recommended_scenario": "oltp_migration",
        "workload_family": "banking",
        "analytics_family": "banking",
        "default_workload_mix": "mixed",
        "schema_recommendation": "tidb_optimized",
        "ddl_target_table": "bank_payments",
        "ddl_reference_column": "reference_id",
        "htap_tables": ["bank_payments", "bank_accounts"],
    },
    "healthcare": {
        "label": "Healthcare",
        "description": "Patient activity, claims, and encounter workflow workload.",
        "primary_focus": "Claim lifecycle and patient/encounter transaction flow",
        "recommended_modules": [
            "customer_queries",
            "baseline_perf",
            "elastic_scale",
            "high_availability",
            "online_ddl",
            "mysql_compat",
            "data_import",
        ],
        "recommended_scenario": "oltp_migration",
        "workload_family": "healthcare",
        "analytics_family": "healthcare",
        "default_workload_mix": "mixed",
        "schema_recommendation": "tidb_optimized",
        "ddl_target_table": "hc_claims",
        "ddl_reference_column": "claim_ref",
        "htap_tables": ["hc_claims", "hc_encounters"],
    },
    "gaming": {
        "label": "Gaming",
        "description": "Session spikes, purchases, and player state updates.",
        "primary_focus": "High-ingest player events and in-game purchase traffic",
        "recommended_modules": [
            "customer_queries",
            "baseline_perf",
            "elastic_scale",
            "write_contention",
            "htap",
            "online_ddl",
            "mysql_compat",
            "data_import",
        ],
        "recommended_scenario": "htap_analytics",
        "workload_family": "gaming",
        "analytics_family": "gaming",
        "default_workload_mix": "write_heavy",
        "schema_recommendation": "tidb_optimized",
        "ddl_target_table": "gm_purchases",
        "ddl_reference_column": "order_ref",
        "htap_tables": ["gm_purchases", "gm_sessions"],
    },
    "retail_ecommerce": {
        "label": "Retail / Ecommerce",
        "description": "Order lifecycle, inventory-sensitive reads, checkout writes.",
        "primary_focus": "Order placement and customer order history reads",
        "recommended_modules": [
            "customer_queries",
            "baseline_perf",
            "elastic_scale",
            "online_ddl",
            "mysql_compat",
            "data_import",
            "htap",
        ],
        "recommended_scenario": "htap_analytics",
        "workload_family": "retail_ecommerce",
        "analytics_family": "retail_ecommerce",
        "default_workload_mix": "mixed",
        "schema_recommendation": "tidb_optimized",
        "ddl_target_table": "rt_orders",
        "ddl_reference_column": "order_ref",
        "htap_tables": ["rt_orders", "rt_order_items"],
    },
    "saas": {
        "label": "SaaS",
        "description": "Multi-tenant usage, metering events, and tenant-level reads.",
        "primary_focus": "Tenant traffic isolation and usage metering",
        "recommended_modules": [
            "customer_queries",
            "baseline_perf",
            "elastic_scale",
            "high_availability",
            "online_ddl",
            "mysql_compat",
            "data_import",
        ],
        "recommended_scenario": "oltp_migration",
        "workload_family": "saas",
        "analytics_family": "saas",
        "default_workload_mix": "mixed",
        "schema_recommendation": "tidb_optimized",
        "ddl_target_table": "saas_usage_events",
        "ddl_reference_column": "event_ref",
        "htap_tables": ["saas_usage_events", "saas_users"],
    },
    "iot_telemetry": {
        "label": "IoT / Telemetry",
        "description": "Device ingest streams plus status and alert lookups.",
        "primary_focus": "High-write telemetry ingestion and fleet status reads",
        "recommended_modules": [
            "customer_queries",
            "baseline_perf",
            "elastic_scale",
            "write_contention",
            "htap",
            "online_ddl",
            "mysql_compat",
            "data_import",
        ],
        "recommended_scenario": "htap_analytics",
        "workload_family": "iot_telemetry",
        "analytics_family": "iot_telemetry",
        "default_workload_mix": "write_heavy",
        "schema_recommendation": "tidb_optimized",
        "ddl_target_table": "iot_telemetry",
        "ddl_reference_column": "event_ref",
        "htap_tables": ["iot_telemetry", "iot_alerts"],
    },
    "adtech": {
        "label": "AdTech",
        "description": "Impression/click pipelines with campaign budget updates.",
        "primary_focus": "Burst writes for ad events with campaign spend queries",
        "recommended_modules": [
            "customer_queries",
            "baseline_perf",
            "elastic_scale",
            "write_contention",
            "htap",
            "online_ddl",
            "mysql_compat",
            "data_import",
        ],
        "recommended_scenario": "htap_analytics",
        "workload_family": "adtech",
        "analytics_family": "adtech",
        "default_workload_mix": "write_heavy",
        "schema_recommendation": "tidb_optimized",
        "ddl_target_table": "ad_impressions",
        "ddl_reference_column": "bid_ref",
        "htap_tables": ["ad_impressions", "ad_clicks"],
    },
    "logistics": {
        "label": "Logistics",
        "description": "Shipment lifecycle, tracking events, and dispatch updates.",
        "primary_focus": "Shipment status reads with frequent tracking writes",
        "recommended_modules": [
            "customer_queries",
            "baseline_perf",
            "elastic_scale",
            "high_availability",
            "write_contention",
            "online_ddl",
            "mysql_compat",
            "data_import",
        ],
        "recommended_scenario": "oltp_migration",
        "workload_family": "logistics",
        "analytics_family": "logistics",
        "default_workload_mix": "mixed",
        "schema_recommendation": "tidb_optimized",
        "ddl_target_table": "lg_tracking_events",
        "ddl_reference_column": "detail_ref",
        "htap_tables": ["lg_shipments", "lg_tracking_events"],
    },
}

INDUSTRY_KEYS: List[str] = list(INDUSTRY_PROFILES.keys())


def normalize_industry_key(raw: str | None) -> str:
    value = str(raw or INDUSTRY_DEFAULT).strip().lower()
    return value if value in INDUSTRY_PROFILES else INDUSTRY_DEFAULT


def get_industry_profile(raw: str | None) -> Dict:
    key = normalize_industry_key(raw)
    out = dict(INDUSTRY_PROFILES[key])
    out["key"] = key
    return out


def resolve_industry_from_cfg(cfg: Dict | None) -> Dict:
    cfg = cfg or {}
    industry_cfg = cfg.get("industry") or {}
    return get_industry_profile(industry_cfg.get("selected"))


def industry_labels() -> Dict[str, str]:
    return {k: v["label"] for k, v in INDUSTRY_PROFILES.items()}


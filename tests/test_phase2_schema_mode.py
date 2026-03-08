import unittest

from report import collect_metrics as cm
from setup.generate_data import (
    SCHEMA_MODE_DEFAULT,
    resolve_run_mode,
    resolve_schema_mode,
    schema_ddls,
)


class Phase2SchemaModeTests(unittest.TestCase):
    def test_schema_mode_defaults_to_tidb_optimized(self):
        self.assertEqual(resolve_schema_mode({}), SCHEMA_MODE_DEFAULT)

    def test_schema_mode_falls_back_on_invalid_value(self):
        self.assertEqual(resolve_schema_mode({"schema_mode": "unknown"}), SCHEMA_MODE_DEFAULT)

    def test_schema_mode_mysql_compatible_supported(self):
        self.assertEqual(resolve_schema_mode({"schema_mode": "mysql_compatible"}), "mysql_compatible")

    def test_run_mode_defaults_to_validation(self):
        self.assertEqual(resolve_run_mode({}), "validation")

    def test_run_mode_performance_supported(self):
        self.assertEqual(resolve_run_mode({"run_mode": "performance"}), "performance")

    def test_tidb_schema_contains_nonclustered_and_autorandom(self):
        schema_a, _schema_b, schema_c = schema_ddls("tidb_optimized")
        self.assertIn("PRIMARY KEY NONCLUSTERED", schema_a)
        self.assertIn("SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4", schema_a)
        self.assertIn("transaction_items (\n    id              BIGINT AUTO_RANDOM PRIMARY KEY", schema_a)
        self.assertIn("tenant_users (\n    id          BIGINT AUTO_INCREMENT PRIMARY KEY NONCLUSTERED", schema_c)

    def test_mysql_schema_keeps_auto_increment_defaults(self):
        schema_a, _schema_b, schema_c = schema_ddls("mysql_compatible")
        self.assertIn("users (\n    id          BIGINT AUTO_INCREMENT PRIMARY KEY,", schema_a)
        self.assertIn("transaction_items (\n    id              BIGINT AUTO_INCREMENT PRIMARY KEY,", schema_a)
        self.assertIn("tenant_users (\n    id          BIGINT AUTO_INCREMENT PRIMARY KEY,", schema_c)

    def test_summary_carries_run_context(self):
        payload = {
            "modules": {
                "01_baseline_perf": {
                    "status": "passed",
                    "tidb": {"c8": {"count": 1, "p99_ms": 10.0, "tps": 1000.0}},
                }
            },
            "compat_checks": {"pct": 95.0},
            "comparison_enabled": False,
            "run_context": {"run_mode": "performance", "schema_mode": "tidb_optimized"},
        }
        summary = cm._build_summary(payload)
        self.assertEqual(summary["run_mode"], "performance")
        self.assertEqual(summary["schema_mode"], "tidb_optimized")


if __name__ == "__main__":
    unittest.main()


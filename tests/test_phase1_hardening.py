import unittest

from lib.tidb_cloud import (
    requires_username_prefix,
    validate_tidb_cloud_username,
)
from report import collect_metrics as cm


class Phase1HardeningTests(unittest.TestCase):
    def test_requires_username_prefix_for_serverless_tidb_cloud(self):
        self.assertTrue(
            requires_username_prefix(
                "gateway01.us-east-1.prod.aws.tidbcloud.com",
                tier="serverless",
            )
        )

    def test_does_not_require_prefix_for_dedicated(self):
        self.assertFalse(
            requires_username_prefix(
                "gateway01.us-east-1.prod.aws.tidbcloud.com",
                tier="dedicated",
            )
        )

    def test_validate_tidb_cloud_username_blocks_bare_root(self):
        msg = validate_tidb_cloud_username(
            {
                "host": "gateway01.us-east-1.prod.aws.tidbcloud.com",
                "user": "root",
            },
            tier="serverless",
        )
        self.assertIsNotNone(msg)
        self.assertIn("prefix", msg.lower())

    def test_validate_tidb_cloud_username_accepts_prefixed_user(self):
        msg = validate_tidb_cloud_username(
            {
                "host": "gateway01.us-east-1.prod.aws.tidbcloud.com",
                "user": "abc123.root",
            },
            tier="serverless",
        )
        self.assertIsNone(msg)

    def test_validate_tidb_cloud_username_skips_non_cloud_hosts(self):
        msg = validate_tidb_cloud_username(
            {"host": "mysql.internal.local", "user": "root"},
            tier="serverless",
        )
        self.assertIsNone(msg)

    def test_ha_phase_candidates_keep_legacy_alias(self):
        self.assertEqual(
            cm._phase_candidates("03_high_availability", "failure"),
            ["failure", "during_failure"],
        )

    def test_ha_canonical_phase_maps_legacy_name(self):
        self.assertEqual(
            cm._canonical_phase("03_high_availability", "during_failure"),
            "failure",
        )

    def test_summary_includes_warm_metrics(self):
        payload = {
            "modules": {
                "01_baseline_perf": {
                    "status": "passed",
                    "tidb": {
                        "c8": {"p99_ms": 12.5, "tps": 1234.0},
                        "warm_steady": {
                            "count": 50,
                            "p50_ms": 2.4,
                            "p95_ms": 5.6,
                            "p99_ms": 7.8,
                            "tps": 1111.0,
                        },
                    },
                },
            },
            "compat_checks": {"pct": 96.5},
            "comparison_enabled": False,
        }
        summary = cm._build_summary(payload)
        self.assertAlmostEqual(summary["warm_p50_ms"], 2.4)
        self.assertAlmostEqual(summary["warm_p95_ms"], 5.6)
        self.assertAlmostEqual(summary["warm_p99_ms"], 7.8)
        self.assertAlmostEqual(summary["warm_tps"], 1111.0)
        self.assertIn("best_observed_p99_ms", summary)


if __name__ == "__main__":
    unittest.main()


import unittest

from setup.poc_web_ui import normalize_cfg
from setup.pre_poc_intake import tier_test_profile


class SmallDefaultTests(unittest.TestCase):
    def test_normalize_cfg_invalid_scale_defaults_to_small(self):
        cfg = normalize_cfg({"test": {"data_scale": "xlarge"}})
        self.assertEqual(cfg["test"]["data_scale"], "small")

    def test_normalize_cfg_invalid_runner_size_defaults_to_small(self):
        cfg = normalize_cfg({"aws_runner": {"instance_size": "xxl"}})
        self.assertEqual(cfg["aws_runner"]["instance_size"], "small")

    def test_tier_profiles_default_scale_small(self):
        for tier in ("serverless", "essential", "premium", "dedicated", "byoc"):
            profile = tier_test_profile(tier)
            self.assertEqual(profile["data_scale"], "small")

    def test_normalize_cfg_point_get_defaults(self):
        cfg = normalize_cfg({"test": {}})
        self.assertTrue(cfg["test"]["point_get_phase_enabled"])
        self.assertGreaterEqual(int(cfg["test"]["point_get_duration_seconds"]), 30)
        self.assertGreaterEqual(int(cfg["test"]["point_get_concurrency"]), 1)


if __name__ == "__main__":
    unittest.main()

import unittest

from lib.dataset_registry import (
    as_csv_uris,
    dataset_bootstrap_enabled,
    dataset_bootstrap_required,
    dataset_skip_synthetic_generation,
    normalize_dataset_profile_key,
    resolve_dataset_profile_from_cfg,
    resolve_manifest_entry,
)


class DatasetRegistryTests(unittest.TestCase):
    def test_normalize_profile_key(self):
        self.assertEqual(normalize_dataset_profile_key("banking"), "banking")
        self.assertEqual(normalize_dataset_profile_key("unknown"), "general_auto")

    def test_profile_from_cfg_prefers_dataset_override(self):
        cfg = {
            "industry": {"selected": "gaming"},
            "dataset_bootstrap": {"profile_key": "healthcare"},
        }
        self.assertEqual(resolve_dataset_profile_from_cfg(cfg), "healthcare")

    def test_profile_from_cfg_falls_back_to_industry(self):
        cfg = {"industry": {"selected": "retail_ecommerce"}}
        self.assertEqual(resolve_dataset_profile_from_cfg(cfg), "retail_ecommerce")

    def test_bootstrap_flags(self):
        cfg = {"dataset_bootstrap": {"enabled": True, "required": True, "skip_synthetic_generation": True}}
        self.assertTrue(dataset_bootstrap_enabled(cfg))
        self.assertTrue(dataset_bootstrap_required(cfg))
        self.assertTrue(dataset_skip_synthetic_generation(cfg))

    def test_manifest_resolution_dict(self):
        manifest = {
            "datasets": {
                "general_auto": {"key": "general_auto", "oltp": {"uris": ["s3://a"]}},
                "banking": {"key": "banking", "oltp": {"uris": ["s3://b"]}},
            }
        }
        row = resolve_manifest_entry(manifest, "banking")
        self.assertEqual(row.get("key"), "banking")
        self.assertEqual(row.get("oltp", {}).get("uris"), ["s3://b"])

    def test_manifest_resolution_list(self):
        manifest = {
            "datasets": [
                {"key": "general_auto", "oltp": {"uris": ["s3://a"]}},
                {"key": "healthcare", "oltp": {"uris": ["s3://h"]}},
            ]
        }
        row = resolve_manifest_entry(manifest, "healthcare")
        self.assertEqual(row.get("key"), "healthcare")
        self.assertEqual(row.get("oltp", {}).get("uris"), ["s3://h"])

    def test_as_csv_uris(self):
        self.assertEqual(as_csv_uris("s3://x"), ["s3://x"])
        self.assertEqual(as_csv_uris(["s3://a", "", None, "s3://b"]), ["s3://a", "s3://b"])
        self.assertEqual(as_csv_uris(None), [])


if __name__ == "__main__":
    unittest.main()

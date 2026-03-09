import unittest

from lib.industry_profiles import INDUSTRY_DEFAULT, INDUSTRY_PROFILES, get_industry_profile, normalize_industry_key
from load.workload_definitions import analytical_workload_for_cfg, transactional_workload_for_cfg


class IndustryProfileTests(unittest.TestCase):
    def test_invalid_industry_falls_back(self):
        self.assertEqual(normalize_industry_key("unknown"), INDUSTRY_DEFAULT)

    def test_registry_contains_v1_profiles(self):
        expected = {
            "general_auto",
            "banking",
            "healthcare",
            "gaming",
            "retail_ecommerce",
            "saas",
            "iot_telemetry",
            "adtech",
            "logistics",
        }
        self.assertTrue(expected.issubset(set(INDUSTRY_PROFILES.keys())))

    def test_banking_workload_switches_tables(self):
        counts = {"bank_customers": 1000, "bank_accounts": 1200, "bank_payments": 10000}
        workload = transactional_workload_for_cfg({"industry": {"selected": "banking"}}, counts)
        sql_text = " ".join(item["sql"] for item in workload)
        self.assertIn("bank_accounts", sql_text)
        self.assertIn("bank_payments", sql_text)

    def test_general_auto_keeps_legacy_schema_a(self):
        counts = {"users": 1000, "accounts": 1200, "transactions": 10000}
        workload = transactional_workload_for_cfg({"industry": {"selected": "general_auto"}}, counts)
        sql_text = " ".join(item["sql"] for item in workload)
        self.assertIn("accounts", sql_text)
        self.assertIn("transactions", sql_text)

    def test_healthcare_analytics_switches_tables(self):
        counts = {"hc_claims": 10000, "hc_encounters": 10000}
        workload = analytical_workload_for_cfg({"industry": {"selected": "healthcare"}}, counts)
        sql_text = " ".join(item["sql"] for item in workload)
        self.assertIn("hc_claims", sql_text)

    def test_profile_includes_recommended_modules(self):
        profile = get_industry_profile("gaming")
        self.assertIn("recommended_modules", profile)
        self.assertIn("baseline_perf", profile["recommended_modules"])

    def test_each_industry_resolves_expected_workload_family(self):
        expected_prefix = {
            "general_auto": "select_",
            "banking": "bank_",
            "healthcare": "hc_",
            "gaming": "gm_",
            "retail_ecommerce": "rt_",
            "saas": "saas_",
            "iot_telemetry": "iot_",
            "adtech": "ad_",
            "logistics": "lg_",
        }
        for industry_key, prefix in expected_prefix.items():
            with self.subTest(industry=industry_key):
                profile = get_industry_profile(industry_key)
                self.assertEqual(profile["key"], industry_key)
                self.assertTrue(profile.get("recommended_modules"))

                txn = transactional_workload_for_cfg({"industry": {"selected": industry_key}}, {})
                qtypes = [str(item.get("query_type", "")) for item in txn]
                self.assertTrue(any(q.startswith(prefix) for q in qtypes), qtypes)


if __name__ == "__main__":
    unittest.main()

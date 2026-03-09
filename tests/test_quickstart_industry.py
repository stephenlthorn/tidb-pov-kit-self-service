import tempfile
import unittest
import uuid
from pathlib import Path

import yaml

from setup.poc_web_ui import MODULE_ORDER, create_app, create_user, get_user_by_email


class QuickstartIndustryRouteTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config_path = Path(self.tmp.name) / "config.yaml"
        self.config_path.write_text("{}", encoding="utf-8")
        self.app = create_app(self.config_path)
        self.app.config.update(TESTING=True)
        self.client = self.app.test_client()
        email = f"quickstart-{uuid.uuid4().hex}@example.com"
        ok, msg = create_user(email, "TestPassword!123", "admin")
        self.assertTrue(ok, msg)
        row = get_user_by_email(email)
        self.assertIsNotNone(row)
        with self.client.session_transaction() as sess:
            sess["auth_user_id"] = int(row["id"])

    def tearDown(self):
        self.tmp.cleanup()

    def _post(self, data):
        resp = self.client.post("/quickstart-deploy", data=data, follow_redirects=False)
        self.assertIn(resp.status_code, (302, 303))
        return yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}

    def test_quickstart_persists_industry_and_small_default(self):
        data = {
            "wiz_tier": "serverless",
            "wiz_industry": "banking",
            "wiz_workload_preset": "balanced_poc",
            "wiz_tests_menu_present": "1",
            "wiz_tests_touched": "0",
            "wiz_action": "save",
        }
        cfg = self._post(data)
        self.assertEqual(cfg.get("industry", {}).get("selected"), "banking")
        self.assertEqual(cfg.get("test", {}).get("data_scale"), "small")
        self.assertTrue(cfg.get("modules", {}).get("baseline_perf"))
        # Banking overlay adds HA even when the baseline preset does not.
        self.assertTrue(cfg.get("modules", {}).get("high_availability"))

    def test_manual_test_touch_preserves_manual_selection(self):
        data = {
            "wiz_tier": "serverless",
            "wiz_industry": "gaming",
            "wiz_workload_preset": "balanced_poc",
            "wiz_tests_menu_present": "1",
            "wiz_tests_touched": "1",
            "wiz_action": "save",
        }
        for key in MODULE_ORDER:
            data[f"wiz_mod_{key}"] = "on" if key == "customer_queries" else ""
        cfg = self._post(data)
        self.assertTrue(cfg.get("modules", {}).get("customer_queries"))
        self.assertFalse(cfg.get("modules", {}).get("baseline_perf"))
        self.assertFalse(cfg.get("modules", {}).get("high_availability"))


if __name__ == "__main__":
    unittest.main()

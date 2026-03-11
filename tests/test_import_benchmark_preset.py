import tempfile
import unittest
import uuid
from pathlib import Path
import os
import sys

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from setup.poc_web_ui import create_app, create_user, get_user_by_email


class ImportBenchmarkPresetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config_path = Path(self.tmp.name) / "config.yaml"
        self.config_path.write_text("{}", encoding="utf-8")
        self.app = create_app(self.config_path)
        self.app.config.update(TESTING=True)
        self.client = self.app.test_client()

        email = f"import-benchmark-{uuid.uuid4().hex}@example.com"
        ok, msg = create_user(email, "TestPassword!123", "admin")
        self.assertTrue(ok, msg)
        row = get_user_by_email(email)
        self.assertIsNotNone(row)
        with self.client.session_transaction() as sess:
            sess["auth_user_id"] = int(row["id"])

    def tearDown(self):
        self.tmp.cleanup()

    def test_import_benchmark_preset_sets_s3_import_defaults(self):
        resp = self.client.post(
            "/quickstart-deploy",
            data={
                "wiz_tier": "serverless",
                "wiz_industry": "banking",
                "wiz_workload_preset": "import_benchmark",
                "wiz_tests_menu_present": "1",
                "wiz_tests_touched": "0",
                "wiz_action": "save",
            },
            follow_redirects=False,
        )
        self.assertIn(resp.status_code, (302, 303))

        cfg = yaml.safe_load(self.config_path.read_text(encoding="utf-8")) or {}
        test_cfg = cfg.get("test", {})
        ds_cfg = cfg.get("dataset_bootstrap", {})

        self.assertEqual(test_cfg.get("import_methods"), ["import_into"])
        self.assertEqual(test_cfg.get("import_into_source_uri"), "__AUTO_DATASET_OLTP__")
        self.assertEqual(int(test_cfg.get("import_into_threads", 0)), 8)
        self.assertTrue(ds_cfg.get("enabled"))
        self.assertTrue(ds_cfg.get("required"))
        self.assertEqual(ds_cfg.get("profile_key"), "banking")


if __name__ == "__main__":
    unittest.main()

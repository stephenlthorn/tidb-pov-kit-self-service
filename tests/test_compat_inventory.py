import runpy
import unittest
from pathlib import Path


class CompatInventoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        mod_path = Path(__file__).resolve().parents[1] / "tests" / "06_mysql_compat" / "run.py"
        cls.mod = runpy.run_path(str(mod_path))

    def test_source_inventory_families_present(self):
        checks = self.mod["SOURCE_UNSUPPORTED_CHECKS"]
        self.assertIn("mysql", checks)
        self.assertIn("postgres", checks)
        self.assertIn("mssql", checks)
        self.assertGreater(len(checks["mysql"]), 10)
        self.assertGreater(len(checks["postgres"]), 8)
        self.assertGreater(len(checks["mssql"]), 8)

    def test_mysql_has_expected_feature_checks(self):
        checks = self.mod["SOURCE_UNSUPPORTED_CHECKS"]["mysql"]
        features = {row["feature"] for row in checks}
        self.assertIn("Stored Procedures", features)
        self.assertIn("Triggers", features)
        self.assertIn("Events", features)
        self.assertIn("UDF Plugins", features)

    def test_safe_int_helper(self):
        safe_int = self.mod["_safe_int"]
        self.assertEqual(safe_int("12"), 12)
        self.assertEqual(safe_int(0), 0)
        self.assertIsNone(safe_int("abc"))
        self.assertIsNone(safe_int(None))


if __name__ == "__main__":
    unittest.main()

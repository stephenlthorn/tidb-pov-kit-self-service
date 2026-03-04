import shutil
import unittest
from pathlib import Path

from load.tidb_blaster import create_run_dir, normalize_blaster_config, plan_commands


class TiDBBlasterTests(unittest.TestCase):
    def test_normalize_uses_tidb_cfg_for_dsn(self):
        cfg = normalize_blaster_config(
            {},
            {
                "host": "gateway01.example.tidbcloud.com",
                "port": 4000,
                "user": "abc.root",
                "password": "secret",
                "database": "test",
            },
        )
        dsn = cfg["cluster"]["tidb_dsn"]
        self.assertTrue(dsn.startswith("mysql://abc.root:secret@gateway01.example.tidbcloud.com:4000/test"))

    def test_plan_commands_renders_rawsql(self):
        cfg = normalize_blaster_config(
            {
                "mode": "rawsql",
                "cluster": {"tidb_dsn": "mysql://u:p@127.0.0.1:4000/test"},
                "loadgen": {"hosts": ["localhost", "localhost"]},
                "rawsql": {
                    "sql_file": "load/sql/rawsql_mix.sql",
                    "threads_total": 12,
                    "connections_total": 20,
                    "duration_sec": 30,
                },
            },
            {},
        )
        run_dir = create_run_dir("rawsql", "test-plan")
        try:
            plan = plan_commands(cfg, run_dir)
            self.assertEqual(len(plan), 2)
            for row in plan:
                self.assertIn("tiup bench rawsql run", row["command"])
                self.assertIn("--query-files", row["command"])
                self.assertGreaterEqual(int(row["threads"]), 1)
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

    def test_create_run_dir_pattern(self):
        run_dir = create_run_dir("rawsql", "hot reads")
        try:
            self.assertTrue(run_dir.exists())
            self.assertIn("_rawsql_hot-reads", run_dir.name)
            self.assertTrue((run_dir / "loadgens").exists())
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()

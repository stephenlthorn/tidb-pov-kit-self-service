import unittest
import re

from lib.industry_profiles import INDUSTRY_KEYS
from load.workload_definitions import (
    is_point_get_query,
    point_get_workload_for_cfg,
    transactional_workload_for_cfg,
)


class PointGetWorkloadTests(unittest.TestCase):
    def test_general_auto_classifier_excludes_range_and_count(self):
        workload = transactional_workload_for_cfg({"industry": {"selected": "general_auto"}}, {})
        range_like = []
        for w in workload:
            query_type = str(w.get("query_type", "")).strip().lower()
            tokens = {t for t in re.split(r"[^a-z0-9]+", query_type) if t}
            if "range" in tokens or "count" in tokens:
                range_like.append(w)
        self.assertTrue(range_like)
        self.assertTrue(all(not is_point_get_query(w) for w in range_like))

        point_like = [w for w in workload if str(w.get("query_type", "")) in {"select_account", "select_user"}]
        self.assertTrue(point_like)
        self.assertTrue(all(is_point_get_query(w) for w in point_like))

    def test_point_get_pool_exists_for_every_industry(self):
        for industry in INDUSTRY_KEYS:
            with self.subTest(industry=industry):
                cfg = {"industry": {"selected": industry}}
                pool = point_get_workload_for_cfg(cfg, {})
                self.assertTrue(pool, f"Expected non-empty point-get pool for {industry}")
                self.assertTrue(
                    all(str(w.get("sql", "")).strip().lower().startswith("select ") for w in pool),
                    f"Point-get pool should only contain read queries for {industry}",
                )
                self.assertTrue(
                    any(is_point_get_query(w) for w in pool),
                    f"Expected at least one strict point-get query for {industry}",
                )


if __name__ == "__main__":
    unittest.main()

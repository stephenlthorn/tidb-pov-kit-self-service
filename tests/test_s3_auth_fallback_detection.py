import os
import sys
import unittest
import importlib.util
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from setup.bootstrap_dataset import _is_s3_auth_required_error as bootstrap_auth_error  # noqa: E402

_DATA_IMPORT_PATH = Path(__file__).resolve().parents[1] / "tests" / "07_data_import" / "run.py"
_SPEC = importlib.util.spec_from_file_location("m7_import_run", _DATA_IMPORT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MOD = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MOD)
data_import_auth_error = getattr(_MOD, "_is_s3_auth_required_error")


class S3AuthFallbackDetectionTests(unittest.TestCase):
    def test_detects_tidb_import_into_auth_error(self):
        msg = (
            "Access to the data source has been denied. Reason: acesss key with secret access key "
            "or role arn with external id is required."
        )
        self.assertTrue(bootstrap_auth_error(msg))
        self.assertTrue(data_import_auth_error(msg))

    def test_ignores_unrelated_error(self):
        msg = "table does not exist"
        self.assertFalse(bootstrap_auth_error(msg))
        self.assertFalse(data_import_auth_error(msg))


if __name__ == "__main__":
    unittest.main()

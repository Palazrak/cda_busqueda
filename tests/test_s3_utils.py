import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


class S3UtilsTest(unittest.TestCase):
    def test_upload_bytes_is_noop_when_disabled(self):
        from utils import s3_utils

        with patch.dict(os.environ, {"S3_UPLOAD_ENABLED": "false"}, clear=False):
            self.assertIsNone(
                s3_utils.upload_bytes_if_enabled(
                    b"demo",
                    "test/demo.txt",
                    content_type="text/plain",
                )
            )

    def test_enabled_upload_requires_credentials(self):
        from utils import s3_utils

        env = {
            "S3_UPLOAD_ENABLED": "true",
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_REGION": "",
            "S3_BUCKET": "",
        }
        with patch.dict(os.environ, env, clear=False):
            with self.assertRaisesRegex(RuntimeError, "S3 upload enabled"):
                s3_utils.upload_bytes_if_enabled(b"demo", "test/demo.txt")


if __name__ == "__main__":
    unittest.main()

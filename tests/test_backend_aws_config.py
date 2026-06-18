import os
import unittest
from unittest.mock import patch

from backend.aws_config import AwsConfigError, get_rekognition_config


class BackendAwsConfigTest(unittest.TestCase):
    def test_missing_rekognition_credentials_raise_friendly_error(self):
        env = {
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_REGION": "",
        }

        with patch.dict(os.environ, env, clear=False):
            with self.assertRaisesRegex(
                AwsConfigError,
                "AWS Rekognition is not configured",
            ):
                get_rekognition_config()

    def test_rekognition_config_uses_region_default(self):
        env = {
            "AWS_ACCESS_KEY_ID": "access-key",
            "AWS_SECRET_ACCESS_KEY": "secret-key",
        }

        with patch.dict(os.environ, env, clear=True):
            config = get_rekognition_config()

        self.assertEqual(config["aws_access_key_id"], "access-key")
        self.assertEqual(config["aws_secret_access_key"], "secret-key")
        self.assertEqual(config["region_name"], "us-east-1")


if __name__ == "__main__":
    unittest.main()

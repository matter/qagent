import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.config import load_settings


class MacroDataConfigTests(unittest.TestCase):
    def test_fred_api_key_loads_from_yaml_and_env_can_override_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "external_data:",
                        "  fred:",
                        "    api_key: yaml_key",
                        "    request_timeout_seconds: 12",
                    ]
                ),
                encoding="utf-8",
            )

            yaml_settings = load_settings(config_path)
            self.assertEqual(yaml_settings.external_data.fred.api_key, "yaml_key")
            self.assertEqual(
                yaml_settings.external_data.fred.request_timeout_seconds,
                12,
            )

            with patch.dict(os.environ, {"FRED_API_KEY": "env_key"}):
                env_settings = load_settings(config_path)

            self.assertEqual(env_settings.external_data.fred.api_key, "env_key")


if __name__ == "__main__":
    unittest.main()

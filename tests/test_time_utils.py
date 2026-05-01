from datetime import UTC, datetime
from pathlib import Path
from unittest import TestCase

from backend.time_utils import utc_now_iso, utc_now_naive


class TimeUtilsTests(TestCase):
    def test_utc_now_naive_returns_naive_utc_datetime(self):
        before = datetime.now(UTC).replace(tzinfo=None)
        value = utc_now_naive()
        after = datetime.now(UTC).replace(tzinfo=None)

        self.assertIsNone(value.tzinfo)
        self.assertLessEqual(before, value)
        self.assertLessEqual(value, after)

    def test_utc_now_iso_returns_zulu_timestamp(self):
        value = utc_now_iso()

        self.assertTrue(value.endswith("Z"))
        self.assertNotIn("+00:00", value)

    def test_backend_code_does_not_call_deprecated_utcnow(self):
        project_root = Path(__file__).resolve().parents[1]
        offenders: list[str] = []
        for path in (project_root / "backend").rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            text = path.read_text()
            if ".utcnow(" in text:
                offenders.append(str(path.relative_to(project_root)))

        self.assertEqual([], offenders)

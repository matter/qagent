import unittest
from datetime import date

from backend.services.calendar_service import (
    get_trading_days,
    offset_trading_days,
    snap_to_trading_day,
)


class CalendarContractTests(unittest.TestCase):
    def test_old_get_trading_days_signature_defaults_to_us(self):
        old_style = get_trading_days(date(2024, 1, 2), date(2024, 1, 5))
        explicit_us = get_trading_days("US", "2024-01-02", "2024-01-05")

        self.assertEqual(old_style, explicit_us)
        self.assertIn(date(2024, 1, 2), old_style)

    def test_cn_calendar_is_independent_from_us_calendar(self):
        cn_days = get_trading_days("CN", "2024-02-12", "2024-02-16")
        us_days = get_trading_days("US", "2024-02-12", "2024-02-16")

        self.assertEqual(cn_days, [])
        self.assertGreater(len(us_days), 0)

    def test_offset_and_snap_accept_market_keyword(self):
        self.assertEqual(
            snap_to_trading_day(date(2024, 2, 12), market="CN", direction="forward"),
            date(2024, 2, 19),
        )
        self.assertEqual(
            offset_trading_days(date(2024, 2, 8), 1, market="CN"),
            date(2024, 2, 19),
        )


if __name__ == "__main__":
    unittest.main()

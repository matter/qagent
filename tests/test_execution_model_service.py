import unittest

from backend.services.execution_model_service import (
    DEFAULT_PLANNED_PRICE_BUFFER_BPS,
    evaluate_planned_price_fill,
    normalize_execution_model,
    normalize_planned_price_buffer_bps,
    normalize_planned_price_fallback,
)


class ExecutionModelServiceTests(unittest.TestCase):
    def test_planned_price_fills_inside_buffered_range(self):
        decision = evaluate_planned_price_fill(
            planned_price=100.0,
            high=102.0,
            low=98.0,
            buffer_bps=50,
        )

        self.assertTrue(decision.filled)
        self.assertEqual(decision.fill_price, 100.0)
        self.assertIsNone(decision.reason)
        self.assertAlmostEqual(decision.lower_bound, 98.49)
        self.assertAlmostEqual(decision.upper_bound, 101.49)

    def test_planned_price_rejects_outside_buffered_range(self):
        decision = evaluate_planned_price_fill(
            planned_price=98.1,
            high=102.0,
            low=98.0,
            buffer_bps=50,
        )

        self.assertFalse(decision.filled)
        self.assertIsNone(decision.fill_price)
        self.assertEqual(decision.reason, "planned_price_outside_buffered_range")
        self.assertEqual(decision.metadata["planned_price"], 98.1)

    def test_planned_price_rejects_missing_high_low(self):
        decision = evaluate_planned_price_fill(
            planned_price=100.0,
            high=None,
            low=98.0,
            buffer_bps=50,
        )

        self.assertFalse(decision.filled)
        self.assertEqual(decision.reason, "missing_high_low")

    def test_planned_price_rejects_invalid_planned_price(self):
        decision = evaluate_planned_price_fill(
            planned_price=0.0,
            high=102.0,
            low=98.0,
            buffer_bps=50,
        )

        self.assertFalse(decision.filled)
        self.assertEqual(decision.reason, "invalid_planned_price")

    def test_normalizes_execution_model_and_buffer_defaults(self):
        self.assertEqual(normalize_execution_model(None), "next_open")
        self.assertEqual(normalize_execution_model("planned_price"), "planned_price")
        self.assertEqual(
            normalize_planned_price_buffer_bps(None),
            DEFAULT_PLANNED_PRICE_BUFFER_BPS,
        )

        with self.assertRaises(ValueError):
            normalize_execution_model("close")
        with self.assertRaises(ValueError):
            normalize_planned_price_buffer_bps(-1)

    def test_normalizes_planned_price_fallback(self):
        self.assertEqual(normalize_planned_price_fallback(None), "cancel")
        self.assertEqual(normalize_planned_price_fallback("next_close"), "next_close")

        with self.assertRaises(ValueError):
            normalize_planned_price_fallback("same_day_close")


if __name__ == "__main__":
    unittest.main()

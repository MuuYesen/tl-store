import unittest
from decimal import Decimal
from tl_store.ctrader.execution import project_funds


class OfficialFunds(unittest.TestCase):
    def test_rounds_total_locked_then_derives_free_and_preserves_exact_audit(self):
        for equity, margin, total, locked, free in [
            ("100.015", "10.005", "100.02", "10.00", "90.02"),
            ("100.005", "0.005", "100.00", "0.00", "100.00"),
            ("100.015", "0.015", "100.02", "0.02", "100.00"),
            ("1.005", "2.005", "1.00", "2.00", "-1.00"),
            ("1.005", "2.015", "1.00", "2.02", "-1.02"),
        ]:
            with self.subTest(equity=equity, margin=margin):
                funds = {
                    "schema_version": 1,
                    "currency": "USD",
                    "epoch": 7,
                    "revision": 11,
                    "raw": {
                        "equity": equity,
                        "used_margin": margin,
                        "free_margin": str(Decimal(equity) - Decimal(margin)),
                    },
                }
                balance, audit = project_funds(funds)
                self.assertEqual(balance.total.as_decimal(), Decimal(total))
                self.assertEqual(balance.locked.as_decimal(), Decimal(locked))
                self.assertEqual(balance.free.as_decimal(), Decimal(free))
                self.assertEqual(
                    balance.total.raw, balance.locked.raw + balance.free.raw
                )
                self.assertEqual(audit["raw"], funds["raw"])

    def test_bad_exact_funds_reject_before_framework_projection(self):
        with self.assertRaises(ValueError):
            project_funds(
                {
                    "currency": "USD",
                    "raw": {"equity": "100", "used_margin": "2", "free_margin": "99"},
                }
            )

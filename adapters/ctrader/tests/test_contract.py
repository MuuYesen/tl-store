import unittest
from decimal import Decimal
from lq_adapter_ctrader.config import CTraderConfig
from lq_adapter_ctrader.provider import instrument_from_wire


class Contracts(unittest.TestCase):
    def test_rejects_unbound_identity(self):
        with self.assertRaises(ValueError):
            CTraderConfig(
                expected_login=0, symbols=("BTCUSD",), asset_classes=("CRYPTOCURRENCY",)
            )

    def test_standard_execution_requires_no_extra_intent_storage(self):
        cfg = CTraderConfig(expected_login=42, symbols=("BTCUSD",),
                            asset_classes=("CRYPTOCURRENCY",))
        self.assertFalse(hasattr(cfg, "state_directory"))
        self.assertFalse(hasattr(cfg, "intent_originator_id"))

    def test_exact_volume_and_price_contract(self):
        spec = instrument_from_wire(
            {
                "symbolId": 1,
                "symbolName": "BTCUSD",
                "baseAssetId": 1,
                "quoteAssetId": 2,
            },
            {
                "symbolId": 1,
                "digits": 2,
                "pipPosition": 1,
                "minVolume": 1,
                "maxVolume": 10000000,
                "stepVolume": 1,
                "lotSize": 100,
            },
            [{"assetId": 1, "name": "BTC"}, {"assetId": 2, "name": "USD"}],
            "CRYPTOCURRENCY",
            1,
        )
        self.assertEqual(spec.validate_quantity(Decimal("0.01")), 1)
        with self.assertRaises(ValueError):
            spec.validate_quantity(Decimal("0.001"))
        self.assertEqual(str(spec.instrument.price_increment), "0.01")

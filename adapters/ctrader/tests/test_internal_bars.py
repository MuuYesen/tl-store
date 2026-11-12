"""Official NA quote aggregation, without a Store callback or broker connection."""
from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
from nautilus_trader.trading import Strategy
from nautilus_trader.execution import FixedFeeModel
from nautilus_trader.model import (AccountType, OmsType, Money, Currency,
                                  BarType, QuoteTick, Price, Quantity)
from lq_adapter_ctrader.provider import instrument_from_wire


def test_official_internal_mid_bars_use_adapter_instrument_and_quotes():
    spec = instrument_from_wire(
        dict(symbolId=1, symbolName='BTCUSD', baseAssetId=1, quoteAssetId=2),
        dict(symbolId=1, digits=2, minVolume=1, maxVolume=1000000, stepVolume=1),
        [dict(assetId=1, name='BTC'), dict(assetId=2, name='USD')],
        'CRYPTOCURRENCY', 1)
    instrument = spec.instrument
    bar_type = BarType.from_str('BTCUSD.CTRADER-1-SECOND-MID-INTERNAL')
    bars = []
    class Subscriber(Strategy):
        def on_start(self): self.subscribe_bars(bar_type)
        def on_bar(self, bar): bars.append(bar)
    native = BacktestEngine(BacktestEngineConfig(bypass_logging=True, run_analysis=False))
    try:
        native.add_venue(instrument.id.venue, OmsType.HEDGING, AccountType.MARGIN,
                         [Money(10000, Currency.from_str('USD'))],
                         fee_model=FixedFeeModel(Money(0, Currency.from_str('USD'))))
        native.add_instrument(instrument)
        native.add_strategy(Subscriber())
        native.add_data([QuoteTick(instrument.id, Price.from_str('100.00'),
            Price.from_str('102.00'), Quantity.from_str('0.00'), Quantity.from_str('0.00'),
            ts, ts) for ts in (1_100_000_000, 2_100_000_000, 3_100_000_000)])
        native.run()
        assert len(bars) >= 2
        assert all(bar.close.as_decimal() == 101 and bar.bar_type == bar_type for bar in bars), [(str(b.close), str(b.bar_type)) for b in bars]
    finally:
        native.dispose()

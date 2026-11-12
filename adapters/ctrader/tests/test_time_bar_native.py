"""Pinned official NA owns calendar boundaries; synthetic quotes, no transport."""
from datetime import datetime, timezone
import pytest
pytest.importorskip('tradelearn')
from tradelearn import engine as bt
from tradelearn_runtime.execution import create_execution_host
from nautilus_trader.backtest import BacktestEngine, BacktestEngineConfig
from nautilus_trader.config import DataEngineConfig, StrategyConfig
from nautilus_trader.model import AccountType, OmsType, Money, Currency, BarType, QuoteTick, StrategyId
from nautilus_trader.execution import FixedFeeModel
from nautilus_trader.testkit.providers import TestInstrumentProvider


def ns(value):
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp())*1_000_000_000


@pytest.mark.parametrize('timeframe,dates,boundary', [
    (bt.TimeFrame.Weeks, ['2024-02-04T23:59:58','2024-02-04T23:59:59','2024-02-05T00:00:01'], '2024-02-05T00:00:00'),
    (bt.TimeFrame.Months,['2024-02-29T23:59:58','2024-02-29T23:59:59','2024-03-01T00:00:01'], '2024-03-01T00:00:00'),
    (bt.TimeFrame.Years, ['2024-12-31T23:59:58','2024-12-31T23:59:59','2025-01-01T00:00:01'], '2025-01-01T00:00:00'),
])
def test_compiled_calendar_bar_reaches_single_tl_runtime(timeframe, dates, boundary):
    store=__import__("tradelearn_runtime", fromlist=["NautilusStore"]).NautilusStore(adapter='ctrader',account=100001,
        client_id='synthetic-client',client_secret='synthetic-secret',
        access_token='synthetic-access',refresh_token='synthetic-refresh',practice=True)
    compiled=store.getdata(dataname='BTCUSDT',timeframe=timeframe).bar_type
    instrument=TestInstrumentProvider.btcusdt_perp_binance()
    bar_type=BarType.from_str(str(compiled).replace('BTCUSDT.CTRADER',str(instrument.id)))
    native=BacktestEngine(BacktestEngineConfig(bypass_logging=True,run_analysis=False,
        data_engine=DataEngineConfig(time_bars_build_with_no_updates=False,
            time_bars_skip_first_non_full_bar=False)))
    native.add_venue(instrument.id.venue,OmsType.NETTING,AccountType.MARGIN,
        [Money(1000000,Currency.from_str('USDT'))],
        fee_model=FixedFeeModel(Money(0,Currency.from_str('USDT'))))
    native.add_instrument(instrument)
    native.add_data([QuoteTick(instrument.id,instrument.make_price(bid),instrument.make_price(bid+2),
        instrument.make_qty(1),instrument.make_qty(1),ns(date),ns(date))
        for bid,date in zip((1000,1002,1004),dates)])
    seen=[]
    class Business(bt.Strategy):
        def next(self):seen.append(float(self.data.close[0]))
    host=create_execution_host(StrategyConfig(strategy_id=StrategyId('TL-001')),
        business_cls=Business,feeds={str(instrument.id):'bar'},
        instruments={str(instrument.id):instrument.id},subscriptions=[bar_type],
        account_id='BINANCE-001',offline_test=True)
    try:
        native.add_strategy(host);native.run()
        assert seen==[1003.0]
        bars=native.cache.bars(bar_type)
        assert len(bars)==1 and bars[0].ts_event==ns(boundary)
        assert float(bars[0].open)==1001.0 and float(bars[0].close)==1003.0
    finally:native.dispose()

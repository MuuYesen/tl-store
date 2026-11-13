"""Unified NautilusStore adapter assembly. No application strategy or deployment imports."""
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable
from nautilus_trader.live import LiveNodeConfig
from nautilus_trader.config import StrategyConfig
from .config import CTraderConfig, CTraderCredentials

@dataclass(frozen=True)
class CTraderStoreConfig:
    options: CTraderConfig
    node_config: LiveNodeConfig
    strategy_config: StrategyConfig | None = None
    state_dir: str | Path = '.tradelearn/ctrader'
    account_provider: Callable | None = None

    def __post_init__(self):
        if not isinstance(self.options, CTraderConfig):
            raise TypeError('CTraderConfig options required')
        if not isinstance(self.node_config, LiveNodeConfig):
            raise TypeError('official LiveNodeConfig required')
        if (set(self.node_config.data_clients) != {'CTRADER'}
                or set(self.node_config.exec_clients) != {'CTRADER'}):
            raise ValueError('exact CTRADER data/execution client configuration required')
        if self.strategy_config is not None and not isinstance(self.strategy_config, StrategyConfig):
            raise TypeError('official StrategyConfig required')
        if self.account_provider is not None and not callable(self.account_provider):
            raise TypeError('account_provider must be callable')


def build_adapter(config=None, *, account=None, client_id=None, client_secret=None,
                  access_token=None, refresh_token=None, practice=None,
                  token_state=None, token_role=None, grant_id=None,
                  trader_id=None, strategy_id=None, economics=None, require_flat_on_connect=None,
                  queue_capacity=None, max_history_records=None, history_lookback_ms=None,
                  request_timeout_ms=None):
    """Return versioned assembly data for the one TL NautilusStore; never connect."""
    simple = any(value is not None for value in
                 (account, client_id, client_secret, access_token, refresh_token, practice,
                  token_state, token_role, grant_id, trader_id, strategy_id, economics,
                  require_flat_on_connect, queue_capacity, max_history_records,
                  history_lookback_ms, request_timeout_ms))
    if simple:
        if config is not None:
            raise ValueError('simple and native adapter config are mutually exclusive')
        if type(practice) is not bool:
            raise TypeError('practice must explicitly select demo (True) or live (False)')
        from nautilus_trader.config import DataClientConfig, ExecutionClientConfig
        from nautilus_trader.live import LiveDataEngineConfig
        from nautilus_trader.common import Environment
        from nautilus_trader.model import TraderId, StrategyId, OmsType
        credentials = CTraderCredentials(client_id, client_secret, access_token, refresh_token,
            state_path=token_state, grant_id=grant_id,
            role='owner' if token_role is None else token_role)
        transport = {name: value for name, value in dict(
            economics=economics, require_flat_on_connect=require_flat_on_connect,
            queue_capacity=queue_capacity, max_history_records=max_history_records,
            history_lookback_ms=history_lookback_ms, request_timeout_ms=request_timeout_ms).items()
            if value is not None}
        config = CTraderStoreConfig(
            options=CTraderConfig(expected_login=account, symbols=(), credentials=credentials,
                                  resolve_asset_classes=True, live=not practice, **transport),
            node_config=LiveNodeConfig(trader_id=TraderId('TL-001' if trader_id is None else trader_id),
                environment=Environment.SANDBOX if practice else Environment.LIVE,
                data_engine=LiveDataEngineConfig(time_bars_build_with_no_updates=False),
                data_clients={'CTRADER': DataClientConfig()},
                exec_clients={'CTRADER': ExecutionClientConfig()}),
            strategy_config=StrategyConfig(strategy_id=StrategyId(
                'TL-001' if strategy_id is None else strategy_id),oms_type=OmsType.HEDGING))
    if not isinstance(config, CTraderStoreConfig):
        raise TypeError('CTraderStoreConfig required')
    try:
        from tradelearn_runtime.store import NautilusStore
    except ImportError:
        raise RuntimeError('TradeLearn unified NautilusStore adapter API 2 required') from None
    if getattr(NautilusStore, '_tradelearn_store_adapter_api', None) != 2:
        raise RuntimeError('TradeLearn unified NautilusStore adapter API 2 required')
    if getattr(NautilusStore, '_tradelearn_store_interface', None) != 1:
        raise RuntimeError('TradeLearn Backtrader Store interface 1 required')
    from nautilus_trader.model import BarType, BarAggregation, PriceType, OmsType, StrategyId
    from .session import CTraderAccountSession
    from .factories import CTraderDataClientFactory, CTraderExecutionClientFactory

    def validate_feed(value):
        if not isinstance(value, BarType):
            raise TypeError('official BarType required')
        if (str(value.instrument_id.venue) != 'CTRADER'
                or str(value.instrument_id.symbol) not in session.options.symbols):
            raise ValueError('subscription differs from configured cTrader symbols')
        if (not value.is_internally_aggregated()
                or value.spec.price_type not in (PriceType.BID, PriceType.ASK, PriceType.MID)
                or value.spec.aggregation not in (BarAggregation.SECOND, BarAggregation.MINUTE,
                                                 BarAggregation.HOUR, BarAggregation.DAY, BarAggregation.MILLISECOND,
                                                 BarAggregation.WEEK, BarAggregation.MONTH, BarAggregation.YEAR)):
            raise ValueError('cTrader requires NA internal time bars from BID/ASK/MID quotes')

    session = CTraderAccountSession(config.options)
    strategy_config = config.strategy_config or StrategyConfig(
        strategy_id=StrategyId('TL-001'), oms_type=OmsType.HEDGING)
    data_factory = CTraderDataClientFactory(config.options, session)
    exec_factory = CTraderExecutionClientFactory(config.options, session)

    def compile_feed(*, dataname, timeframe=3, compression=1, price='mid'):
        # TL TimeFrame integer values are converted only here, off the hot path.
        aggregations = {2: 'MILLISECOND', 3: 'SECOND', 4: 'MINUTE', 5: 'DAY',
                        6: 'WEEK', 7: 'MONTH', 8: 'YEAR'}
        if (not isinstance(dataname, str) or not dataname or dataname.strip() != dataname
                or any(c in dataname for c in ('.', '\n', '\r', '\0'))):
            raise ValueError('Exact nonempty cTrader dataname required')
        if type(timeframe) is not int or timeframe not in aggregations:
            raise ValueError('A TL time-based timeframe representable by NA is required')
        if type(compression) is not int or not 1 <= compression <= 2147483647:
            raise ValueError('Positive bounded integer compression required')
        if price not in ('bid', 'ask', 'mid'):
            raise ValueError('cTrader supports bid, ask or mid prices')
        step = compression
        if timeframe == 2:
            if compression % 1000:
                raise ValueError('NA time bars require exact whole milliseconds')
            step = compression // 1000
        bar = BarType.from_str(f'{dataname}.CTRADER-{step}-{aggregations[timeframe]}-{price.upper()}-INTERNAL')
        # Ask the pinned native model to check its own interval representation.
        # Calendar aggregation remains owned by the NA clock, never this compiler.
        bar.spec.get_interval_ns()
        if simple and dataname not in session.options.symbols:
            options = replace(session.options, symbols=(*session.options.symbols, dataname))
            session.options = data_factory.options = exec_factory.options = options
        elif not simple and dataname not in config.options.symbols:
            raise ValueError('subscription differs from configured cTrader symbols')
        return bar

    from .account_values import CTraderAccountValues
    account_provider=(config.account_provider if config.account_provider is not None else
                      CTraderAccountValues(session,config.options.expected_login))
    return dict(api_version=2, config=config.node_config,
        data_factories={'CTRADER': data_factory},
        exec_factories={'CTRADER': exec_factory},
        strategy_config=strategy_config, state_dir=config.state_dir,
        account_provider=account_provider,
        account_id=f'CTRADER-{config.options.expected_login}', validate_feed=validate_feed, compile_feed=compile_feed, close_adapter=session.close)

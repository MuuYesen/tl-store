"""Factories retain one explicitly shared account session and official node context."""

from nautilus_trader.live.clients import DataClientFactory, ExecutionClientFactory
from nautilus_trader.model import AccountId, AccountType, OmsType, Venue
from .session import CTraderAccountSession
from .data import CTraderDataClient
from .execution import CTraderExecutionClient


class CTraderDataClientFactory(DataClientFactory):
    def __init__(self, options, session=None):
        self.options = options
        self.session = (
            session if session is not None else CTraderAccountSession(options)
        )

    def create(self, *, name, config, cache, clock):
        return CTraderDataClient(
            name=name,
            config=config,
            cache=cache,
            clock=clock,
            venue=Venue("CTRADER"),
            options=self.options,
            session=self.session,
        )


class CTraderExecutionClientFactory(ExecutionClientFactory):
    # Current protocol admission is Market IOC / typed close; no partial bracket send.
    _tradelearn_order_groups = False

    def __init__(self, options, session=None):
        self.options = options
        self.session = (
            session if session is not None else CTraderAccountSession(options)
        )

    def create(self, *, name, config, cache, clock, trader_id):
        return CTraderExecutionClient(
            name=name,
            config=config,
            cache=cache,
            clock=clock,
            trader_id=trader_id,
            venue=Venue("CTRADER"),
            account_id=AccountId(f"CTRADER-{self.options.expected_login}"),
            account_type=AccountType.MARGIN,
            oms_type=OmsType.HEDGING,
            options=self.options,
            session=self.session,
        )

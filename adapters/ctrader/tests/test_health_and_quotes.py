import asyncio
from collections import deque
from types import SimpleNamespace
import unittest
from tl_store.ctrader.data import CTraderDataClient
from tl_store.ctrader.execution import CTraderExecutionClient
from tl_store.ctrader.errors import FundsRevisionChanged
from tl_store.ctrader.provider import instrument_from_wire
from tl_store.ctrader.session import CTraderAccountObserver


class ReplayEnd(Exception):
    pass


class Session:
    failure = None

    async def fail(self, error):
        self.failure = error


class Transport:
    def __init__(self, events=()):
        self.events = deque(events)
        self.halted = False
        self.closed = False

    async def next_event(self):
        if not self.events:
            raise ReplayEnd()
        return self.events.popleft()

    async def halt(self):
        self.halted = True

    async def close(self):
        self.closed = True


def spot(ts=200, account=42, **sides):
    payload = dict(ctidTraderAccountId=account, symbolId=1, **sides)
    if ts is not None:
        payload["timestamp"] = ts
    return dict(payload_type=2131, received_wall_ns=999000000, payload=payload)


def data_client(events):
    class Client(CTraderDataClient):
        def _handle_data(self, value):
            self.quotes.append(value)

    c = object.__new__(Client)
    c.session = Session()
    c.transport = Transport(events)
    c.quotes = []
    c._account_id = 42
    c._subscribed = {1}
    c._sides = {}
    c._last_source_ts = 0
    c._reader = None
    spec = instrument_from_wire(
        dict(symbolId=1, symbolName="BTCUSD", baseAssetId=1, quoteAssetId=2),
        dict(symbolId=1, digits=2, minVolume=1, maxVolume=1000000, stepVolume=1),
        [dict(assetId=1, name="BTC"), dict(assetId=2, name="USD")],
        "CRYPTOCURRENCY",
        1,
    )
    c.contracts = {1: spec}
    return c


class QuoteHealth(unittest.IsolatedAsyncioTestCase):
    async def test_missing_zero_and_backwards_source_clock_do_not_emit_or_pollute_sides(
        self,
    ):
        c = data_client(
            [
                spot(bid=10000000, ask=10001000),
                spot(None, bid=9000000),
                spot(0, bid=8000000),
                spot(199, bid=7000000),
                spot(201, ask=10002000),
            ]
        )
        with self.assertRaises(ReplayEnd):
            await c._events_inner()
        self.assertEqual(len(c.quotes), 2)
        self.assertEqual(str(c.quotes[-1].bid_price), "100.00")
        self.assertEqual(str(c.quotes[-1].ask_price), "100.02")
        self.assertEqual(c.quotes[-1].ts_event, 201000000)
        self.assertEqual(
            c.quotes[0].bid_price.precision, c.quotes[0].ask_price.precision
        )

    async def test_wrong_account_mapping_failure_closes_epoch_and_is_visible_to_observer(
        self,
    ):
        c = data_client([spot(account=43, bid=10000000, ask=10001000)])
        with self.assertRaises(ValueError):
            await c._events()
        self.assertTrue(c.transport.halted)
        self.assertIsInstance(c.session.failure, ValueError)
        observer = CTraderAccountObserver(c.session)
        with self.assertRaisesRegex(RuntimeError, "processing epoch failed"):
            await observer.check_epoch(1)

    async def test_failed_reader_is_preserved_and_disconnect_still_releases_lease(self):
        c = data_client([])
        lease = c.transport

        async def fail():
            raise ValueError("original source failure")

        c._reader = asyncio.create_task(fail())
        await asyncio.sleep(0)
        await c._disconnect()
        self.assertTrue(lease.closed)
        self.assertIsNone(c.transport)
        self.assertEqual(str(c.background_errors[0]), "original source failure")

    async def test_explicit_funds_query_propagates_failure_once_without_hidden_poller(self):
        self.assertFalse(hasattr(CTraderExecutionClient, "_funds_loop"))
        for error in (FundsRevisionChanged(), RuntimeError("connection ended")):
            c = object.__new__(CTraderExecutionClient)
            c._orders = {"pending": "preserved"}
            calls = []

            async def refresh():
                calls.append(True)
                raise error

            c._refresh_funds = refresh
            with self.assertRaises(type(error)) as raised:
                await c._query_account(None)
            self.assertIs(raised.exception, error)
            self.assertEqual(calls, [True])
            self.assertEqual(c._orders, {"pending": "preserved"})

"""Official Node + installed public TL bridge + target cTrader adapter, no network.

The transport is a deterministic broker replay. Native wire and unknown-result reconciliation are
verified separately by the Rust loopback tests, rather than asserted here.
"""

import asyncio
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
import tempfile
import time


def deny_network(event, args):
    if event in ("socket.connect", "socket.getaddrinfo", "socket.sendto"):
        raise RuntimeError("Network disabled by target adapter probe")


sys.addaudithook(deny_network)
from nautilus_trader.common import Environment, LoggerConfig
from nautilus_trader.config import (
    DataClientConfig,
    ExecutionClientConfig,
    LiveNodeConfig,
    StrategyConfig,
)
from nautilus_trader.live import LiveNode
from nautilus_trader.model import (
    TraderId,
    StrategyId,
    AccountId,
    InstrumentId,
    PositionId,
    OrderStatus,
)
from lq_adapter_ctrader import (
    CTraderConfig,
    VerifiedDealEconomics,
    CTraderDataClientFactory,
    CTraderExecutionClientFactory,
)
from tradelearn_runtime.execution import create_execution_host
from tradelearn.engine import Strategy
from tradelearn.backtest.models import Order

STAMP = time.time_ns() // 1000000
LIGHT = {"symbolId": 1, "symbolName": "BTCUSD", "baseAssetId": 1, "quoteAssetId": 2}
DETAIL = {
    "symbolId": 1,
    "digits": 2,
    "minVolume": 1,
    "maxVolume": 10000000,
    "stepVolume": 1,
    "lotSize": 100,
    "enableShortSelling": True,
}
ASSETS = [{"assetId": 1, "name": "BTC"}, {"assetId": 2, "name": "USD"}]


class ReplayTransport:
    account_id = 42

    def __init__(self, execution):
        self.execution = execution
        self.queue = asyncio.Queue()
        self.closed = False
        self.orders = {}
        self.deals = {}
        self.remaining = Decimal(0)
        self.calls = []
        self.closes = 0

    async def request(self, name, payload):
        self.calls.append((name, payload))
        if name == "symbols":
            return {"symbol": [LIGHT]}
        if name == "symbol_details":
            return {"symbol": [DETAIL]}
        if name == "assets":
            return {"asset": ASSETS}
        if name == "subscribe_spots":
            await self.queue.put(
                {
                    "payload_type": 2131,
                    "received_wall_ns": STAMP * 1000000,
                    "payload": {
                        "ctidTraderAccountId": 42,
                        "symbolId": 1,
                        "timestamp": STAMP,
                        "bid": 10000000,
                        "ask": 10001000,
                    },
                }
            )
            return {}
        if name == "unsubscribe_spots":
            return {}
        if name == "order_details":
            oid = int(payload["orderId"])
            return {
                "order": self.orders[oid],
                "deal": self.deals[oid],
                "hasMore": False,
            }
        raise AssertionError(name)

    async def snapshot(self, *args):
        return {
            "trader": {
                "trader": {
                    "ctidTraderAccountId": 42,
                    "traderLogin": 123,
                    "depositAssetId": 2,
                }
            },
            "orders": [],
            "deals": [],
            "reconcile": {"position": [], "order": []},
            "close_bindings": {},
        }

    async def funds(self):
        return {
            "funds": {
                "schema_version": 1,
                "currency": "USD",
                "observed_utc_ns": time.time_ns(),
                "raw": {"equity": "10000", "used_margin": "0", "free_margin": "10000"},
            },
            "ctrader_raw_snapshot": None,
            "execution_batch": None,
            "raw": {"reconcile_after": {"position": [], "order": []}},
        }

    async def next_event(self):
        return await self.queue.get()

    async def acknowledge_executions(self, through):
        pass

    async def close(self):
        self.closed = True

    async def halt(self):
        self.closed = True

    async def mutate(self, payload):
        self.calls.append(("mutate", payload))
        cid = payload["client_order_id"]
        if payload["kind"] == "market":
            assert (
                payload["position_id"] is None
                and payload["side"] == 1
                and Decimal(payload["quantity"]) == 4
            )
            self.remaining = Decimal(4)
            order = {
                "orderId": 20,
                "positionId": 30,
                "clientOrderId": cid,
                "orderType": 1,
                "orderStatus": 1,
                "timeInForce": 3,
                "executedVolume": 200,
                "executionPrice": "100.01",
                "utcLastUpdateTimestamp": STAMP + 1,
                "tradeData": {
                    "symbolId": 1,
                    "volume": 400,
                    "tradeSide": 1,
                    "openTimestamp": STAMP,
                },
            }
            deal = {
                "dealId": 10,
                "orderId": 20,
                "positionId": 30,
                "symbolId": 1,
                "volume": 400,
                "filledVolume": 200,
                "tradeSide": 1,
                "dealStatus": 3,
                "createTimestamp": STAMP,
                "executionTimestamp": STAMP + 1,
                "executionPrice": "100.01",
                "commission": 1,
                "moneyDigits": 2,
            }
            self.orders[20] = order
            self.deals[20] = [deal]

            async def finish_entry():
                await asyncio.sleep(0.03)
                await self.queue.put(
                    {
                        "payload_type": 2126,
                        "payload": {"ctidTraderAccountId": 42, "order": dict(order)},
                    }
                )
                await asyncio.sleep(0.03)
                self.orders[20] = dict(
                    order,
                    orderStatus=2,
                    executedVolume=400,
                    utcLastUpdateTimestamp=STAMP + 2,
                )
                self.deals[20].append(
                    dict(deal, dealId=11, dealStatus=2, executionTimestamp=STAMP + 2)
                )
                await self.queue.put(
                    {
                        "payload_type": 2126,
                        "payload": {
                            "ctidTraderAccountId": 42,
                            "order": self.orders[20],
                        },
                    }
                )

            asyncio.create_task(finish_entry())
            return {"state": "execution", "payload": {"order": order}}
        assert (
            payload["kind"] == "close"
        ), "EXIT must select the typed close mutation, never a plain SELL"
        assert (
            payload["position_id"] == 30
            and payload["side"] == 2
            and Decimal(payload["quantity"]) == 2
        )
        assert self.remaining >= 2
        self.closes += 1
        oid = 20 + self.closes
        did = 11 + self.closes
        self.remaining -= Decimal(2)
        order = {
            "orderId": oid,
            "positionId": 30,
            "orderType": 1,
            "orderStatus": 2,
            "timeInForce": 2,
            "closingOrder": True,
            "executedVolume": 200,
            "executionPrice": "100.03",
            "utcLastUpdateTimestamp": STAMP + 2 + self.closes,
            "tradeData": {
                "symbolId": 1,
                "volume": 200,
                "tradeSide": 2,
                "openTimestamp": STAMP + 2 + self.closes,
            },
        }
        deal = {
            "dealId": did,
            "orderId": oid,
            "positionId": 30,
            "symbolId": 1,
            "volume": 200,
            "filledVolume": 200,
            "tradeSide": 2,
            "dealStatus": 2,
            "createTimestamp": STAMP + 2 + self.closes,
            "executionTimestamp": STAMP + 2 + self.closes,
            "executionPrice": "100.03",
            "commission": 1,
            "moneyDigits": 2,
            "closePositionDetail": {
                "entryPrice": "100.01",
                "grossProfit": 4,
                "swap": 0,
                "commission": 1,
                "closedVolume": 200,
                "moneyDigits": 2,
                "pnlConversionFee": 0,
            },
        }
        assert "clientOrderId" not in order
        self.orders[oid] = order
        self.deals[oid] = [deal]
        await self.queue.put(
            {
                "payload_type": 2126,
                "local_client_order_id": cid,
                "payload": {"ctidTraderAccountId": 42, "order": order, "deal": deal},
            }
        )
        return {
            "state": "execution",
            "local_client_order_id": cid,
            "payload": {"order": order, "deal": deal},
        }


class ReplaySession:
    failure = None

    def __init__(self):
        self.transports = []

    async def acquire(self, execution=False, market=False):
        t = ReplayTransport(execution)
        self.transports.append(t)
        return t

    async def fail(self, error):
        self.failure = error


async def cycle(index):
    holder = {}
    notifications = []
    created = []

    class DF(CTraderDataClientFactory):
        def create(self, **kwargs):
            client = super().create(**kwargs)
            created.append(client)
            return client

    class EF(CTraderExecutionClientFactory):
        def create(self, **kwargs):
            client = super().create(**kwargs)
            created.append(client)
            return client

    class Business(Strategy):
        def __init__(self):
            super().__init__()
            self.phase = "entry"
            self.sent = False

        def next(self):
            if not self.sent:
                self.sent = True
                self.buy(size=4, tif="ioc", client_oid=f"entry-{index}")

        def notify_order(self, order):
            notifications.append((order.status, order.executed.size))
            if order.status != Order.Completed:
                return
            if self.phase == "entry":
                self.phase = "partial-position-close"
                self.sell(
                    size=2,
                    tif="ioc",
                    client_oid=f"close-a-{index}",
                    reduce_only=True,
                    position_id=PositionId("30"),
                )
            elif self.phase == "partial-position-close":
                self.phase = "final-position-close"
                self.sell(
                    size=2,
                    tif="ioc",
                    client_oid=f"close-b-{index}",
                    reduce_only=True,
                    position_id=PositionId("30"),
                )
            else:

                async def stop_after_core_drains():
                    await asyncio.sleep(0.05)
                    holder["host"].shutdown_system("target replay reached flat")

                asyncio.create_task(stop_after_core_drains())

    with tempfile.TemporaryDirectory() as directory:
        state = Path(directory).resolve()
        options = CTraderConfig(
            expected_login=123,
            symbols=("BTCUSD",),
            asset_classes=("CRYPTOCURRENCY",),
            economics=VerifiedDealEconomics(
                42,
                123,
                2,
                "USD",
                "charge_positive",
                "offline-synthetic-broker-contract",
            ),
        )
        session = ReplaySession()
        node = LiveNode.build(
            "CTRADER-TARGET-REPLAY",
            LiveNodeConfig(
                environment=Environment.SANDBOX,
                trader_id=TraderId("CTRADER-001"),
                logging=LoggerConfig(bypass_logging=True),
                timeout_connection_secs=2,
                timeout_reconciliation_secs=2,
                timeout_portfolio_secs=2,
                timeout_disconnection_secs=1,
                delay_post_stop_secs=0,
                data_clients={"CTRADER": DataClientConfig()},
                exec_clients={"CTRADER": ExecutionClientConfig()},
            ),
            data_factories={"CTRADER": DF(options, session)},
            exec_factories={"CTRADER": EF(options, session)},
        )
        host = create_execution_host(
            StrategyConfig(strategy_id=StrategyId("TL-CTRADER-001")),
            business_cls=Business,
            feeds={"BTCUSD": "tick"},
            instruments={"BTCUSD": InstrumentId.from_str("BTCUSD.CTRADER")},
            account_id=AccountId("CTRADER-123"),
            intent_identity="offline/CTRADER/123",
            intent_path=state / "intent.sqlite",
        )
        holder["host"] = host
        node.add_strategy(host)
        try:
            try:
                async with asyncio.timeout(8):
                    await node.run_async()
            except TimeoutError as error:
                raise AssertionError({"notifications": notifications,
                    "session_failure": str(session.failure),
                    "clients": [{"ready": getattr(c, "_ready", None),
                                 "errors": [str(e) for e in getattr(c, "background_errors", [])]}
                                for c in created],
                    "orders": [(str(o.client_order_id), str(o.status)) for o in node.cache.orders()]}) from error
            await host.drain()
            orders = node.cache.orders()
            assert len(orders) == 3, [
                (str(o.client_order_id), str(o.status)) for o in orders
            ]
            assert all(o.status == OrderStatus.FILLED for o in orders)
            assert notifications.count((Order.Partial, 2.0)) == 1, notifications
            assert notifications.count((Order.Completed, 4.0)) == 1, notifications
            assert notifications.count((Order.Completed, -2.0)) == 2, notifications
            assert sum(len(o.trade_ids) for o in orders) == 4
            positions = node.cache.positions()
            assert (
                len(positions) == 1
                and positions[0].is_closed
                and positions[0].quantity.as_decimal() == 0
            )
            backend = next(t for t in session.transports if t.execution)
            assert backend.closes == 2 and backend.remaining == 0
            assert all(
                "clientOrderId" not in raw
                for oid, raw in backend.orders.items()
                if oid != 20
            )
            assert session.failure is None
            assert all(t.closed for t in session.transports)
            assert all(not c.background_errors for c in created)
            return {
                "cycle": index,
                "entry_quantity": "4",
                "close_quantities": ["2", "2"],
                "close_mutation_kind": "close",
                "position_id": "30",
                "position_flat": True,
                "trade_ids": 4,
                "partial_entry_and_duplicate_deal_idempotent": True,
                "raw_close_client_order_id_absent": True,
                "transports_released": True,
            }
        finally:
            await host.drain()
            node.dispose()


async def main():
    return [await cycle(1), await cycle(2)]


if __name__ == "__main__":
    result = {
        "official_version": "2.0.0rc7",
        "target_adapter": str(Path(__import__("lq_adapter_ctrader").__file__)),
        "network": "socket audit denied",
        "transport": "deterministic broker replay; native WS/unknown-result reconciliation tested separately",
        "results": asyncio.run(main()),
    }
    output = Path(
        os.environ.get(
            "LEAFQUANT_CTRADER_NODE_RECEIPT",
            Path(__file__).resolve().parents[1]
            / "artifacts/official-node-receipt.json",
        )
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))

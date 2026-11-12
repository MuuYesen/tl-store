"""Authoritative Open API orders, fills, hedged positions and raw account evidence."""

import asyncio
import uuid
from decimal import Decimal
from nautilus_trader import model as m
from nautilus_trader.live.clients import ExecutionClient
from .provider import integer, load_contracts
from .reports import (
    EvidencePending,
    decode,
    fee,
    fill_report,
    order_report,
    position_report,
    price,
    quantity,
    side,
    timestamp,
)


def project_funds(funds):
    raw = funds["raw"]
    currency = m.Currency.from_str(funds["currency"])
    equity, used, available = (
        Decimal(raw[n]) for n in ("equity", "used_margin", "free_margin")
    )
    if equity - used != available:
        raise ValueError("Exact broker funds do not reconcile")
    total = m.Money.from_decimal(equity, currency)
    locked = m.Money.from_decimal(used, currency)
    free = m.Money.from_raw(total.raw - locked.raw, currency)
    audit = dict(funds)
    audit.update(
        currency_precision=currency.precision,
        amount_encoding="decimal_deposit_currency_units",
        projection="sdk_bankers_total_locked_free_difference",
        locked_interpretation="broker_used_margin",
        projected={
            "total": str(total.as_decimal()),
            "locked": str(locked.as_decimal()),
            "free": str(free.as_decimal()),
        },
    )
    return m.AccountBalance(total, locked, free), audit


class CTraderExecutionClient(ExecutionClient):
    def __init__(self, *, options, session, **kwargs):
        super().__init__(**kwargs)
        self.options, self.session = options, session
        self.transport = None
        self.contracts = {}
        self._tasks = []
        self._orders = {}
        self._deals = {}
        self._deals_by_order = {}
        self._positions = {}
        self._venue_clients = {}
        self._local = {}
        self._accepted = set()
        self._filled = {}
        self._terminal = {}
        self._projection = asyncio.Lock()
        self._account = None
        self._ready = False
        self._pending_recovery = set()

    async def _connect(self):
        try:
            self.transport = await self.session.acquire(execution=True)
            self.contracts = await load_contracts(
                self.transport, self.options, self.clock.timestamp_ns()
            )
            for spec in self.contracts.values():
                self._handle_instrument(spec.instrument)
            snapshot = await self.transport.snapshot(
                self.options.history_lookback_ms, self.options.require_flat_on_connect
            )
            account = snapshot["trader"]["trader"]
            self._account = {
                "ctid_trader_account_id": str(account["ctidTraderAccountId"]),
                "trader_login": str(account["traderLogin"]),
                "deposit_asset_id": str(account["depositAssetId"]),
                "is_live": self.options.live,
            }
            if integer(account["traderLogin"]) != self.options.expected_login:
                raise ValueError("Unexpected authenticated account")
            e = self.options.economics
            if e and (
                e.ctid_trader_account_id != integer(account["ctidTraderAccountId"])
                or e.trader_login != integer(account["traderLogin"])
                or e.deposit_asset_id != integer(account["depositAssetId"])
            ):
                raise ValueError("Commission evidence/account mismatch")
            self._venue_clients = {
                int(oid): cid for oid, cid in snapshot.get("close_bindings", {}).items()
            }
            self._orders = {integer(r["orderId"]): r for r in snapshot["orders"]}
            self._deals = {integer(r["dealId"]): r for r in snapshot["deals"]}
            self._deals_by_order = {}
            for did, raw in self._deals.items():
                self._deals_by_order.setdefault(integer(raw["orderId"]), {})[did] = raw
            self._positions = {
                integer(r["positionId"]): r
                for r in snapshot["reconcile"].get("position", [])
            }
            await self._refresh_funds()
            self._ready = True
            self._tasks = [
                self.create_task(self._events(), "ctrader-openapi-execution"),
                self.create_task(self._recovery_loop(), "ctrader-openapi-recovery"),
            ]
        except BaseException:
            await self._disconnect()
            raise

    async def _disconnect(self):
        self._ready = False
        self.background_errors = []
        try:
            for task in self._tasks:
                task.cancel()
            outcomes = await asyncio.gather(*self._tasks, return_exceptions=True)
            self.background_errors = [
                e
                for e in outcomes
                if isinstance(e, BaseException)
                and not isinstance(e, asyncio.CancelledError)
            ]
        finally:
            self._tasks = []
            if self.transport:
                await self.transport.close()
            self.transport = None

    async def _fail_closed(self, error):
        self._ready = False
        # The session owns invalidation and captures the first cause before close.
        await self.session.fail(error)

    async def _refresh_funds(self):
        observation = await self.transport.funds()
        funds = observation["funds"]
        balance, info_funds = project_funds(funds)
        currency = balance.total.currency
        if (
            self.options.economics
            and currency.code != self.options.economics.commission_currency
        ):
            raise ValueError(
                "Deposit currency differs from verified commission currency"
            )
        info = {
            "ctrader_funds": info_funds,
            "ctrader_raw_snapshot": observation.get("ctrader_raw_snapshot"),
        }
        batch = observation.get("execution_batch")
        if batch:
            e = self.options.economics
            economics = (
                {
                    "commission_sign": e.commission_sign,
                    "commission_currency": e.commission_currency,
                    "evidence_reference": e.evidence_reference,
                }
                if e
                else None
            )
            account = dict(self._account, deposit_currency=currency.code)
            info["ctrader_executions"] = {
                "schema_version": 1,
                "account": account,
                "economics": economics,
                "events": batch["events"],
            }
        self.generate_account_state(
            [balance], [], True, integer(funds["observed_utc_ns"]), info
        )
        if batch:
            await self.transport.acknowledge_executions(integer(batch["through"]))
        reconcile = observation.get("raw", {}).get("reconcile_after")
        if reconcile is not None:
            self._positions = {
                integer(r["positionId"]): r for r in reconcile.get("position", [])
            }

    async def _recovery_loop(self):
        try:
            await self._recovery_loop_inner()
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            await self._fail_closed(error)
            raise

    async def _recovery_loop_inner(self):
        # Order recovery can wait on network/projection; it must not block explicit account queries.
        while True:
            await asyncio.sleep(1)
            for oid in tuple(self._pending_recovery):
                await self._recover_order(oid)

    async def _events(self):
        try:
            await self._events_inner()
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            await self._fail_closed(error)
            raise

    async def _events_inner(self):
        while True:
            event = await self.transport.next_event()
            if event["payload_type"] != 2126:
                continue
            payload = event["payload"]
            if (
                str(payload["ctidTraderAccountId"])
                != self._account["ctid_trader_account_id"]
            ):
                raise ValueError("Execution account mismatch")
            recover = False
            async with self._projection:
                order = payload.get("order")
                if order:
                    oid = integer(order["orderId"])
                    if event.get("local_client_order_id"):
                        self._venue_clients[oid] = event["local_client_order_id"]
                    recover = await self._process_execution_payload(oid, payload)
                if payload.get("position"):
                    p = payload["position"]
                    position = decode("position", p)
                    if (
                        Decimal(position["quantity"]) == 0
                        or position["position_status"] == 2
                    ):
                        self._positions.pop(position["position_id"], None)
                    else:
                        self._positions[position["position_id"]] = p

            if recover:
                await self._recover_order(oid)

    async def _process_execution_payload(self, oid, payload):
        # Authenticated execution events can already contain a complete lifecycle.
        # Validate the same reports before committing any evidence or callbacks.
        raw = payload["order"]
        order = decode("order", raw)
        if order["order_id"] != oid:
            raise ValueError("Execution order identity mismatch")
        if order["symbol_id"] not in self.contracts:
            return
        old = self._orders.get(oid)
        if old:
            previous = decode("order", old)
            if (
                order["update_timestamp_ms"] < previous["update_timestamp_ms"]
                or Decimal(order["filled_quantity"]) < Decimal(previous["filled_quantity"])
                or (oid in self._terminal and order["order_status"] != previous["order_status"])
            ):
                return True
        detail = {"order": raw, "deal": [payload["deal"]] if payload.get("deal") else []}
        prepared = self._prepare_order_details(oid, detail)
        if prepared[2] is None:
            # Missing broker clocks, commission, or any part of the executed volume
            # retain the canonical recovery path. Never manufacture missing facts.
            return True
        await self._apply_order_details(oid, detail, prepared=prepared)

    async def _recover_order(self, oid):
        detail = await self.transport.request("order_details", {"orderId": oid})
        # Network waits never own the projection lock. A normal execution may
        # advance retained facts while this request is in flight; recheck it.
        async with self._projection:
            incoming = decode("order", detail["order"])
            if incoming["order_id"] != oid:
                raise ValueError("OrderDetails identity mismatch")
            old = self._orders.get(oid)
            if old is not None:
                previous = decode("order", old)
                if (incoming["update_timestamp_ms"] < previous["update_timestamp_ms"]
                        or Decimal(incoming["filled_quantity"]) < Decimal(previous["filled_quantity"])
                        or (oid in self._terminal and incoming["order_status"] != previous["order_status"])):
                    self._pending_recovery.add(oid)
                    if len(self._pending_recovery) > self.options.queue_capacity:
                        raise RuntimeError("Unresolved lifecycle evidence bound exceeded")
                    return
            await self._apply_order_details(oid, detail)

    def _prepare_order_details(self, oid, detail):
        """Validate one lifecycle without publishing or changing retained facts."""
        if detail.get("hasMore", False):
            raise ValueError("Truncated authoritative order lifecycle")
        raw = detail["order"]
        order = decode("order", raw)
        if order["order_id"] != oid:
            raise ValueError("OrderDetails identity mismatch")
        if order["symbol_id"] not in self.contracts:
            return
        raws = dict(self._deals_by_order.get(oid, {}))
        for d in detail.get("deal", []):
            deal = decode("deal", d)
            if deal["order_id"] != oid:
                raise ValueError("OrderDetails contains another order's deal")
            did = deal["deal_id"]
            old = self._deals.get(did)
            if old:
                a = decode("deal", old)
                for field in (
                    "order_id",
                    "position_id",
                    "symbol_id",
                    "filled_volume",
                    "trade_side",
                    "execution_timestamp_ms",
                    "execution_price",
                ):
                    if a[field] != deal[field]:
                        raise ValueError("Conflicting authoritative deal replay")
            raws[did] = d
        try:
            for value in raws.values():
                deal = decode("deal", value)
                if deal["deal_status"] in (2, 3) and deal["execution_price"] is None:
                    raise EvidencePending("Missing broker execution price")
            report = order_report(
                raw,
                list(raws.values()),
                self.contracts[order["symbol_id"]],
                self.account_id,
                self.options.economics,
                self.clock.timestamp_ns(),
                (
                    m.ClientOrderId(self._venue_clients[oid])
                    if oid in self._venue_clients
                    else None
                ),
            )
        except EvidencePending:
            report = None
        return order, raws, report

    async def _apply_order_details(self, oid, detail, *, prepared=None):
        if prepared is None:
            prepared = self._prepare_order_details(oid, detail)
        if prepared is None:
            return
        order, by_id, report = prepared
        self._orders[oid] = detail["order"]
        self._deals.update(by_id)
        self._deals_by_order[oid] = by_id
        raws = list(by_id.values())
        if report is None:
            self._pending_recovery.add(oid)
            if len(self._pending_recovery) > self.options.queue_capacity:
                raise RuntimeError("Unresolved lifecycle evidence bound exceeded")
            self._prune_evidence()
            return
        self._pending_recovery.discard(oid)
        cid = report.client_order_id
        local = (self.cache.order(cid) if cid else None) or self._local.get(str(cid))
        if local is None:
            fills = [
                fill_report(
                    d,
                    self.contracts[order["symbol_id"]],
                    self.account_id,
                    self.options.economics,
                    self.clock.timestamp_ns(),
                    cid,
                )
                for d in raws
                if decode("deal", d)["deal_status"] in (2, 3)
            ]
            self._handle_report(report, fills)
            self._prune_evidence()
            return
        if (
            str(local.instrument_id) != str(report.instrument_id)
            or local.side != report.order_side
            or local.quantity != report.quantity
        ):
            raise ValueError("Broker execution differs from admitted order")
        if (
            report.order_status != m.OrderStatus.REJECTED
            and str(cid) not in self._accepted
            and local.status in (m.OrderStatus.INITIALIZED, m.OrderStatus.SUBMITTED)
        ):
            self.generate_order_accepted(
                local, report.venue_order_id, report.ts_accepted
            )
            self._accepted.add(str(cid))
        for d in sorted(
            raws, key=lambda r: (integer(r["executionTimestamp"]), integer(r["dealId"]))
        ):
            deal = decode("deal", d)
            if deal["deal_status"] not in (2, 3):
                continue
            identity = (deal["order_id"], deal["deal_id"])
            if (
                identity in self._filled
                or m.TradeId(str(deal["deal_id"])) in local.trade_ids
            ):
                continue
            fill = fill_report(
                d,
                self.contracts[order["symbol_id"]],
                self.account_id,
                self.options.economics,
                self.clock.timestamp_ns(),
                cid,
            )
            self.generate_order_filled(
                local,
                fill.venue_order_id,
                fill.venue_position_id,
                fill.trade_id,
                fill.last_qty,
                fill.last_px,
                self.contracts[order["symbol_id"]].instrument.quote_currency,
                fill.commission,
                m.LiquiditySide.NO_LIQUIDITY_SIDE,
                fill.ts_event,
            )
            self._filled[identity] = deal
        terminal = report.order_status in (
            m.OrderStatus.REJECTED,
            m.OrderStatus.CANCELED,
            m.OrderStatus.EXPIRED,
            m.OrderStatus.FILLED,
        )
        if terminal:
            old = self._terminal.get(oid)
            if old and old != report.order_status:
                raise ValueError("Conflicting terminal order state")
            if old is None:
                if report.order_status == m.OrderStatus.REJECTED:
                    self.generate_order_rejected(
                        local, "Broker native rejection", report.ts_last, False
                    )
                elif report.order_status == m.OrderStatus.CANCELED:
                    self.generate_order_canceled(
                        local, report.venue_order_id, report.ts_last
                    )
                elif report.order_status == m.OrderStatus.EXPIRED:
                    self.generate_order_expired(
                        local, report.venue_order_id, report.ts_last
                    )
                self._terminal[oid] = report.order_status
        self._prune_evidence()

    def _prune_evidence(self):
        bound = self.options.max_history_records
        if len(self._orders) > bound:
            removable = [
                oid
                for oid, row in self._orders.items()
                if decode("order", row)["order_status"] in (2, 3, 4, 5)
                and oid not in self._pending_recovery
            ]
            for oid in removable[: len(self._orders) - bound]:
                cid = self._orders[oid].get("clientOrderId") or self._venue_clients.get(
                    oid
                )
                self._venue_clients.pop(oid, None)
                self._orders.pop(oid, None)
                self._terminal.pop(oid, None)
                if cid:
                    self._local.pop(cid, None)
                    self._accepted.discard(cid)
                for did in self._deals_by_order.pop(oid, {}):
                    self._deals.pop(did, None)
                self._filled = {
                    key: d for key, d in self._filled.items() if key[0] != oid
                }
        if len(self._orders) > bound or len(self._deals) > bound:
            raise RuntimeError(
                "Active execution evidence exceeds configured memory bound"
            )

    async def _submit_order(self, command):
        order = command.order
        if str(order.client_order_id) in self._local:
            raise RuntimeError(
                "CID already admitted; never deny or resend the original order"
            )
        try:
            if not self._ready:
                raise ValueError("Open API execution not reconciled")
            if (
                order.order_type != m.OrderType.MARKET
                or order.time_in_force != m.TimeInForce.IOC
                or getattr(order, "is_post_only", False)
                or getattr(order, "is_quote_quantity", False)
            ):
                raise ValueError("Only plain market IOC is supported")
            for name in ("display_qty", "expire_time"):
                if getattr(order, name, None) is not None:
                    raise ValueError("Unsupported order option: " + name)
            if getattr(order, "emulation_trigger", None) not in (
                None,
                m.TriggerType.NO_TRIGGER,
            ):
                raise ValueError("Emulated orders are unsupported")
            if getattr(order, "contingency_type", None) not in (
                None,
                m.ContingencyType.NO_CONTINGENCY,
            ):
                raise ValueError("Contingent orders are unsupported")
            if getattr(command, "exec_algorithm_id", None) is not None or getattr(
                command, "params", None
            ):
                raise ValueError("Unsupported execution parameters")
            matches = [
                s
                for s in self.contracts.values()
                if s.instrument.id == order.instrument_id
            ]
            if len(matches) != 1:
                raise ValueError("Unknown broker contract")
            spec = matches[0]
            reducing = getattr(order, "is_reduce_only", False)
            if not reducing:
                spec.validate_quantity(order.quantity)
            if self.options.economics is None:
                raise ValueError("Verified fee evidence required before trading")
            position = getattr(command, "position_id", None)
            if reducing:
                if position is None:
                    raise ValueError("Reduce-only requires an exact broker position")
                owned = self.cache.position(position)
                if (
                    owned is None
                    or owned.account_id != self.account_id
                    or owned.strategy_id != order.strategy_id
                    or owned.instrument_id != order.instrument_id
                ):
                    raise ValueError(
                        "Reduce-only target does not belong to this account/strategy/instrument"
                    )
                if (
                    owned.side not in (m.PositionSide.LONG, m.PositionSide.SHORT)
                    or owned.quantity.as_decimal() < order.quantity.as_decimal()
                    or (owned.side == m.PositionSide.LONG)
                    == (order.side == m.OrderSide.BUY)
                ):
                    raise ValueError(
                        "Reduce-only side or quantity conflicts with owned remaining position"
                    )
                exact = order.quantity.as_decimal() * 100
                if exact <= 0 or exact != exact.to_integral_value():
                    raise ValueError("Close requires exact positive centi-units")
            payload = {
                "kind": "close" if reducing else "market",
                "client_order_id": str(order.client_order_id),
                "envelope_id": "N" + uuid.uuid4().hex,
                "quantity": str(order.quantity),
                "side": 1 if order.side == m.OrderSide.BUY else 2,
                "position_id": integer(str(position)) if position is not None else None,
                "light": spec.raw_light,
                "detail": spec.raw_detail,
                "assets": spec.assets,
            }
        except (ValueError, KeyError) as error:
            self.generate_order_denied(order, str(error))
            return
        self._local[str(order.client_order_id)] = order
        self.generate_order_submitted(order)
        try:
            result = await self.transport.mutate(payload)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            await self._fail_closed(error)
            raise
        if result["state"] == "not_sent":
            self.generate_order_rejected(
                order, result["reason"], self.clock.timestamp_ns(), False
            )
            return
        if result["state"] == "unknown":
            # Keep the real submitted state. Na owns subsequent status queries
            # and reconciliation; absence of a reply is not a rejection or retry.
            import logging
            logging.getLogger(__name__).warning(
                "cTrader request outcome unknown; awaiting venue reconciliation"
            )
            return
        if result.get("payload", {}).get("order"):
            oid = integer(result["payload"]["order"]["orderId"])
            if reducing:
                self._venue_clients[oid] = str(order.client_order_id)
            async with self._projection:
                recover = await self._process_execution_payload(oid, result["payload"])
            if recover:
                await self._recover_order(oid)

    async def _cancel_order(self, command):
        try:
            await self._cancel_order_inner(command)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            await self._fail_closed(error)
            raise

    async def _cancel_order_inner(self, command):
        local = self.cache.order(command.client_order_id)
        venue = command.venue_order_id or self.cache.venue_order_id(
            command.client_order_id
        )
        if not self._ready or local is None or venue is None:
            raise ValueError("Cancellation lacks reconciled native identity")
        result = await self.transport.mutate(
            {
                "kind": "cancel",
                "client_order_id": "C" + uuid.uuid4().hex,
                "envelope_id": "N" + uuid.uuid4().hex,
                "venue_order_id": integer(str(venue)),
                "original_client_order_id": str(command.client_order_id),
            }
        )
        if result["state"] == "not_sent":
            self.generate_order_cancel_rejected(
                local, venue, result["reason"], self.clock.timestamp_ns()
            )
        elif result["state"] == "unknown":
            error = RuntimeError("Cancel outcome unknown; never resend")
            await self._fail_closed(error)
            raise error
        else:
            await self._recover_order(integer(str(venue)))

    async def _cancel_all_orders(self, command):
        from nautilus_trader.live import CancelOrder

        for order in self.cache.orders_open(
            venue=self.venue, instrument_id=command.instrument_id
        ):
            await self._cancel_order(
                CancelOrder(
                    self.trader_id,
                    order.strategy_id,
                    order.instrument_id,
                    order.client_order_id,
                    order.venue_order_id,
                )
            )

    async def _query_account(self, command):
        await self._refresh_funds()

    async def _query_order(self, command):
        venue = command.venue_order_id or self.cache.venue_order_id(
            command.client_order_id
        )
        if venue is None:
            return None
        await self._recover_order(integer(str(venue)))

    async def _generate_order_status_report(self, command):
        venue = command.venue_order_id or self.cache.venue_order_id(
            command.client_order_id
        )
        if venue is None:
            return None
        detail = await self.transport.request(
            "order_details", {"orderId": integer(str(venue))}
        )
        if detail.get("hasMore", False):
            raise ValueError("Truncated order lifecycle")
        raw = detail["order"]
        d = decode("order", raw)
        if d["symbol_id"] not in self.contracts:
            return None
        return order_report(
            raw,
            detail.get("deal", []),
            self.contracts[d["symbol_id"]],
            self.account_id,
            self.options.economics,
            self.clock.timestamp_ns(),
            (
                m.ClientOrderId(self._venue_clients[d["order_id"]])
                if d["order_id"] in self._venue_clients
                else None
            ),
        )

    async def _generate_order_status_reports(self, command):
        reports = []
        for raw in self._orders.values():
            d = decode("order", raw)
            if d["symbol_id"] not in self.contracts:
                continue
            spec = self.contracts[d["symbol_id"]]
            if (
                getattr(command, "instrument_id", None)
                and command.instrument_id != spec.instrument.id
            ):
                continue
            deals = [
                v
                for v in self._deals.values()
                if integer(v["orderId"]) == d["order_id"]
            ]
            reports.append(
                order_report(
                    raw,
                    deals,
                    spec,
                    self.account_id,
                    self.options.economics,
                    self.clock.timestamp_ns(),
                    (
                        m.ClientOrderId(self._venue_clients[d["order_id"]])
                        if d["order_id"] in self._venue_clients
                        else None
                    ),
                )
            )
        return reports

    async def _generate_fill_reports(self, command):
        reports = []
        by_order = {integer(o["orderId"]): o for o in self._orders.values()}
        for raw in self._deals.values():
            d = decode("deal", raw)
            if d["symbol_id"] not in self.contracts or d["deal_status"] not in (2, 3):
                continue
            spec = self.contracts[d["symbol_id"]]
            if (
                getattr(command, "instrument_id", None)
                and command.instrument_id != spec.instrument.id
            ):
                continue
            if getattr(command, "venue_order_id", None) and str(
                command.venue_order_id
            ) != str(d["order_id"]):
                continue
            cid = by_order.get(d["order_id"], {}).get(
                "clientOrderId"
            ) or self._venue_clients.get(d["order_id"])
            reports.append(
                fill_report(
                    raw,
                    spec,
                    self.account_id,
                    self.options.economics,
                    self.clock.timestamp_ns(),
                    m.ClientOrderId(cid) if cid else None,
                )
            )
        return reports

    async def _generate_position_status_reports(self, command):
        reports = []
        for raw in self._positions.values():
            d = decode("position", raw)
            if d["symbol_id"] not in self.contracts:
                continue
            spec = self.contracts[d["symbol_id"]]
            if (
                getattr(command, "instrument_id", None)
                and command.instrument_id != spec.instrument.id
            ):
                continue
            reports.append(
                position_report(raw, spec, self.account_id, self.clock.timestamp_ns())
            )
        return reports

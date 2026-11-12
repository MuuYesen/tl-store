"""Canonical Rust broker DTOs mapped to official report objects without float conversion."""

import json
from decimal import Decimal
from nautilus_trader import model as m


class EvidencePending(ValueError):
    """Canonical broker lifecycle is incomplete and must be queried again."""


def decode(kind, raw):
    from ._native import decode as native_decode

    return json.loads(native_decode(kind, json.dumps(raw, separators=(",", ":"))))


def timestamp(ms):
    value = int(ms)
    if value <= 0:
        raise EvidencePending("Missing canonical broker timestamp")
    return value * 1000000


def side(value):
    return {1: m.OrderSide.BUY, 2: m.OrderSide.SELL}[int(value)]


def quantity(value):
    result = m.Quantity.from_str(format(Decimal(str(value)), "f"))
    if result.as_decimal() != Decimal(str(value)):
        raise ValueError("Broker quantity loses precision")
    return result


def price(value, spec):
    value = Decimal(str(value))
    result = m.Price.from_str(format(value, f".{spec.instrument.price_precision}f"))
    if result.as_decimal() != value:
        raise ValueError("Broker price loses instrument precision")
    return result


def fee(deal, economics):
    if economics is None or deal.get("commission") is None:
        raise EvidencePending("Broker commission sign/currency evidence is incomplete")
    amount = Decimal(str(deal["commission"]))
    if economics.commission_sign == "charge_negative":
        amount = -amount
    money = m.Money.from_decimal(
        amount, m.Currency.from_str(economics.commission_currency)
    )
    if money.as_decimal() != amount:
        raise ValueError("Broker commission loses currency precision")
    return money


def fill_report(raw, spec, account_id, economics, ts_init, client_order_id=None):
    deal = decode("deal", raw)
    if (
        deal["deal_status"] not in (2, 3)
        or Decimal(deal["filled_volume"]) <= 0
        or deal["execution_timestamp_ms"] < deal["create_timestamp_ms"]
    ):
        raise ValueError("Invalid authoritative broker fill")
    if deal["symbol_id"] != spec.symbol_id:
        raise ValueError("Deal symbol mismatch")
    return m.FillReport(
        account_id,
        spec.instrument.id,
        m.VenueOrderId(str(deal["order_id"])),
        m.TradeId(str(deal["deal_id"])),
        side(deal["trade_side"]),
        quantity(deal["filled_volume"]),
        price(deal["execution_price"], spec),
        fee(deal, economics),
        m.LiquiditySide.NO_LIQUIDITY_SIDE,
        timestamp(deal["execution_timestamp_ms"]),
        ts_init,
        client_order_id=client_order_id,
        venue_position_id=m.PositionId(str(deal["position_id"])),
    )


def order_report(
    raw, raw_deals, spec, account_id, economics, ts_init, client_order_id=None
):
    order = decode("order", raw)
    unique = {}
    total = Decimal(0)
    for raw_deal in raw_deals:
        d = decode("deal", raw_deal)
        if d["order_id"] != order["order_id"]:
            continue
        if d["deal_status"] not in (2, 3):
            if Decimal(d["filled_volume"]):
                raise ValueError("Rejected deal contains exposure")
            continue
        if (
            d["symbol_id"] != order["symbol_id"]
            or d["trade_side"] != order["trade_side"]
            or (order["position_id"] and d["position_id"] != order["position_id"])
        ):
            raise ValueError("Order/deal identity mismatch")
        if d["deal_id"] in unique:
            if unique[d["deal_id"]] != d:
                raise ValueError("Conflicting duplicate deal")
            continue
        fill_report(raw_deal, spec, account_id, economics, ts_init)
        unique[d["deal_id"]] = d
        total += Decimal(d["filled_volume"])
    if total != Decimal(order["filled_quantity"]):
        raise EvidencePending(
            "Executed order quantity lacks complete authoritative deals"
        )
    status = {
        1: m.OrderStatus.PARTIALLY_FILLED if total else m.OrderStatus.ACCEPTED,
        2: m.OrderStatus.FILLED,
        3: m.OrderStatus.REJECTED,
        4: m.OrderStatus.EXPIRED,
        5: m.OrderStatus.CANCELED,
    }[order["order_status"]]
    if status == m.OrderStatus.FILLED and total != Decimal(order["quantity"]):
        raise ValueError("Filled order quantity incomplete")
    return m.OrderStatusReport(
        account_id,
        spec.instrument.id,
        m.VenueOrderId(str(order["order_id"])),
        side(order["trade_side"]),
        {
            1: m.OrderType.MARKET,
            2: m.OrderType.LIMIT,
            3: m.OrderType.STOP_MARKET,
            6: m.OrderType.STOP_LIMIT,
        }[order["order_type"]],
        {
            1: m.TimeInForce.GTD,
            2: m.TimeInForce.GTC,
            3: m.TimeInForce.IOC,
            4: m.TimeInForce.FOK,
        }[order["time_in_force"]],
        status,
        quantity(order["quantity"]),
        quantity(total),
        timestamp(order["open_timestamp_ms"]),
        timestamp(order["update_timestamp_ms"]),
        ts_init,
        client_order_id=client_order_id
        or (
            m.ClientOrderId(order["client_order_id"])
            if order["client_order_id"]
            else None
        ),
        venue_position_id=(
            m.PositionId(str(order["position_id"])) if order["position_id"] else None
        ),
        avg_px=Decimal(order["execution_price"]) if order["execution_price"] else None,
        price=price(order["limit_price"], spec) if order["limit_price"] else None,
        trigger_price=price(order["stop_price"], spec) if order["stop_price"] else None,
        expire_time=(
            timestamp(order["expiration_timestamp_ms"])
            if order["expiration_timestamp_ms"]
            else None
        ),
        reduce_only=order["closing_order"] or False,
    )


def position_report(raw, spec, account_id, ts_init):
    p = decode("position", raw)
    if p["symbol_id"] != spec.symbol_id:
        raise ValueError("Position symbol mismatch")
    return m.PositionStatusReport(
        account_id,
        spec.instrument.id,
        (
            m.PositionSide.FLAT
            if Decimal(p["quantity"]) == 0
            else {1: m.PositionSide.LONG, 2: m.PositionSide.SHORT}[p["trade_side"]]
        ),
        quantity(p["quantity"]),
        timestamp(p["update_timestamp_ms"] or p["open_timestamp_ms"]),
        ts_init,
        venue_position_id=m.PositionId(str(p["position_id"])),
        avg_px_open=Decimal(p["price"]),
    )

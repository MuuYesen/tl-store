"""Exact centi-unit contracts mapped into official Nautilus instruments."""

from dataclasses import dataclass
from decimal import Decimal
from nautilus_trader.model import (
    AssetClass,
    Cfd,
    Currency,
    InstrumentId,
    Price,
    Quantity,
    Symbol,
)


def integer(value):
    if isinstance(value, bool):
        raise ValueError("Boolean cannot encode an integer")
    result = int(value)
    if str(result) != str(value):
        raise ValueError("Noninteger wire value")
    return result


@dataclass(frozen=True)
class Contract:
    symbol_id: int
    name: str
    instrument: object
    minimum: Decimal
    maximum: Decimal
    step: Decimal
    raw_light: dict
    raw_detail: dict
    assets: list

    def validate_quantity(self, quantity):
        value = Decimal(str(quantity))
        if (
            not value.is_finite()
            or value <= 0
            or value < self.minimum
            or value > self.maximum
            or value % self.step
            or value * 100 != (value * 100).to_integral_value()
        ):
            raise ValueError("Quantity outside exact broker centi-unit contract")
        return int(value * 100)


def instrument_from_wire(light, detail, assets, asset_class, ts):
    sid = integer(light["symbolId"])
    if sid <= 0 or sid != integer(detail["symbolId"]):
        raise ValueError("Symbol identity mismatch")

    def currency(aid):
        matches = [a["name"] for a in assets if integer(a["assetId"]) == integer(aid)]
        if len(matches) != 1:
            raise ValueError("Missing or duplicate asset identity")
        return Currency.from_str(matches[0])

    digits = integer(detail["digits"])
    if not 0 <= digits <= 16:
        raise ValueError("Unsupported price precision")
    minimum, maximum, step = (
        Decimal(integer(detail[n])) / 100
        for n in ("minVolume", "maxVolume", "stepVolume")
    )
    if minimum <= 0 or maximum < minimum or step <= 0:
        raise ValueError("Invalid broker volume contract")
    sp = max(2, -step.normalize().as_tuple().exponent)
    name = light["symbolName"]
    inst = Cfd(
        instrument_id=InstrumentId.from_str(name + ".CTRADER"),
        raw_symbol=Symbol(name),
        asset_class=getattr(AssetClass, asset_class),
        base_currency=currency(light["baseAssetId"]),
        quote_currency=currency(light["quoteAssetId"]),
        price_precision=digits,
        size_precision=sp,
        price_increment=Price.from_str(format(Decimal(1).scaleb(-digits), "f")),
        size_increment=Quantity.from_str(format(step, f".{sp}f")),
        min_quantity=Quantity.from_str(format(minimum, f".{sp}f")),
        max_quantity=Quantity.from_str(format(maximum, f".{sp}f")),
        lot_size=(
            Quantity.from_str(
                format(Decimal(integer(detail["lotSize"])) / 100, f".{sp}f")
            )
            if detail.get("lotSize")
            else None
        ),
        ts_event=ts,
        ts_init=ts,
        info={"ctrader_symbol_id": sid, "raw_light": light, "raw_detail": detail},
    )
    return Contract(sid, name, inst, minimum, maximum, step, light, detail, assets)


async def load_contracts(transport, options, ts):
    if not options.symbols:
        raise ValueError('At least one configured cTrader symbol required')
    lights = (await transport.request("symbols", {"includeArchivedSymbols": False}))[
        "symbol"
    ]
    chosen = []
    for name in options.symbols:
        rows = [r for r in lights if r["symbolName"] == name]
        if len(rows) != 1:
            raise ValueError("Missing or ambiguous configured symbol")
        chosen.append(rows[0])
    details = (
        await transport.request(
            "symbol_details", {"symbolId": [integer(r["symbolId"]) for r in chosen]}
        )
    )["symbol"]
    assets = (await transport.request("assets", {}))["asset"]
    categories, classes = [], []
    if options.resolve_asset_classes:
        categories = (await transport.request('symbol_categories', {}))['symbolCategory']
        classes = (await transport.request('asset_classes', {}))['assetClass']
    result = {}
    for i, light in enumerate(chosen):
        rows = [
            r for r in details if integer(r["symbolId"]) == integer(light["symbolId"])
        ]
        if len(rows) != 1:
            raise ValueError("Missing or ambiguous full symbol")
        asset = (asset_class_from_metadata(light, categories, classes)
                 if options.resolve_asset_classes else options.asset_classes[i])
        spec = instrument_from_wire(light, rows[0], assets, asset, ts)
        result[spec.symbol_id] = spec
    return result


def asset_class_from_metadata(light, categories, classes):
    """Resolve broker IDs, then an explicit supported class name; never guess symbol."""
    category_id = integer(light.get('symbolCategoryId', 0))
    category = [row for row in categories if integer(row['id']) == category_id]
    if len(category) != 1:
        raise ValueError('Missing or ambiguous broker symbol category')
    class_id = integer(category[0]['assetClassId'])
    matches = [row for row in classes if integer(row['id']) == class_id]
    if len(matches) != 1:
        raise ValueError('Missing or ambiguous broker asset class')
    names = {'forex': 'FX', 'fx': 'FX', 'currencies': 'FX',
             'cryptocurrency': 'CRYPTOCURRENCY', 'cryptocurrencies': 'CRYPTOCURRENCY',
             'crypto': 'CRYPTOCURRENCY', 'indices': 'INDEX', 'index': 'INDEX',
             'stocks': 'EQUITY', 'equities': 'EQUITY', 'shares': 'EQUITY',
             'commodities': 'COMMODITY', 'metals': 'COMMODITY', 'energies': 'COMMODITY',
             'bonds': 'DEBT'}
    name = matches[0].get('name', '')
    if not isinstance(name, str) or name.casefold() not in names:
        raise ValueError('Unsupported authoritative broker asset class')
    return names[name.casefold()]

"""Authoritative broker symbol/category/class joins, never name heuristics."""
import asyncio
import pytest
from tl_store.ctrader import CTraderConfig
from lq_adapter_ctrader.provider import load_contracts


class Metadata:
    def __init__(self, broken=None):
        self.calls = []
        self.rows = {
            'symbols': {'symbol': [dict(symbolId=7, symbolName='BTCUSD', baseAssetId=1,
                quoteAssetId=2, symbolCategoryId=30)]},
            'symbol_details': {'symbol':[dict(symbolId=7, digits=2,minVolume=1,
                maxVolume=10000000,stepVolume=1,lotSize=100)]},
            'assets': {'asset':[dict(assetId=1,name='BTC'),dict(assetId=2,name='USD')]},
            'symbol_categories': {'symbolCategory':[dict(id=30,assetClassId=8,name='Broker group')]},
            'asset_classes': {'assetClass':[dict(id=8,name='Cryptocurrencies')]},
        }
        if broken == 'ambiguous':self.rows['symbols']['symbol'] *= 2
        if broken == 'missing_category':self.rows['symbol_categories']['symbolCategory'] = []
        if broken == 'unsupported':self.rows['asset_classes']['assetClass'][0]['name'] = 'Unspecified exotic'
    async def request(self, name, payload):
        self.calls.append(name)
        return self.rows[name]


def test_metadata_identity_and_class():
    transport = Metadata()
    contracts = asyncio.run(load_contracts(transport, CTraderConfig(
        expected_login=100001, symbols=('BTCUSD',),resolve_asset_classes=True), 1))
    assert str(contracts[7].instrument.id) == 'BTCUSD.CTRADER'
    assert contracts[7].instrument.asset_class.name == 'CRYPTOCURRENCY'
    assert {'asset_classes','symbol_categories'} <= set(transport.calls)


@pytest.mark.parametrize('broken',['ambiguous','missing_category','unsupported'])
def test_metadata_fails_closed(broken):
    with pytest.raises(ValueError):
        asyncio.run(load_contracts(Metadata(broken), CTraderConfig(
            expected_login=100001,symbols=('BTCUSD',),resolve_asset_classes=True),1))

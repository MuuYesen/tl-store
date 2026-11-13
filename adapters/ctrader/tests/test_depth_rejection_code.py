import asyncio
import pytest
from tl_store.ctrader.session import NativeTransport
from tl_store.ctrader.errors import CTraderRequestRejected


@pytest.mark.parametrize('code',[None,'NOT_AUTHORIZED','CH_DEPTH_QUOTES_NOT_SUPPORTED'])
def test_readonly_rejection_code_is_typed_and_never_manufactured(code):
    text='Open API rejected request'+('' if code is None else f' [errorCode={code}]')
    class Connection:
        async def request(self,*a):raise RuntimeError(text)
    with pytest.raises(CTraderRequestRejected) as raised:
        asyncio.run(NativeTransport(Connection()).request('subscribe_depth',{'symbolId':[1]}))
    assert raised.value.error_code==code
    assert str(raised.value)==text


def test_unrelated_native_failure_remains_original_not_broker_rejection():
    original=RuntimeError('epoch ended')
    class Connection:
        async def request(self,*a):raise original
    with pytest.raises(RuntimeError) as raised:
        asyncio.run(NativeTransport(Connection()).request('subscribe_depth',{'symbolId':[1]}))
    assert raised.value is original

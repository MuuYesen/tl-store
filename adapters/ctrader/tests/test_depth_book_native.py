"""The installed thin export uses the existing single native DepthBook authority."""
import json
import pytest
from tl_store.ctrader import _native


def book(capacity=4):return _native.DepthBook(42,7,3,capacity)
def event(**changes):return {**dict(ctidTraderAccountId='42',symbolId='7'),**changes}
def apply(b,payload,epoch=3):return json.loads(b.apply(json.dumps(payload),epoch))


def test_native_depth_update_replace_delete_same_price_and_repeat_are_exact():
    b=book()
    p=event(newQuotes=[dict(id='1',bid='123456',size='100'),dict(id='2',bid='123456',size='50'),dict(id='3',ask='123500',size='200')])
    view=apply(b,p)
    assert view['bids']==[['123456','150']]
    assert view['asks']==[['123500','200']]
    assert view['complete'] is False and view['source_ts_ns'] is None
    assert apply(b,p)==view
    assert apply(b,event(newQuotes=[dict(id='2',bid='123400',size='75')]))['bids']==[['123456','100'],['123400','75']]
    assert apply(b,event(deletedQuotes=['1']))['bids']==[['123400','75']]
    assert json.loads(b.levels())['bids']==[['123400','75']]


@pytest.mark.parametrize('payload',[
    event(ctidTraderAccountId='43'),event(symbolId='8'),
    event(newQuotes=[dict(id='1',bid='100',ask='101',size='1')]),
    event(newQuotes=[dict(id=True,bid='100',size='1')]),
    event(newQuotes=[dict(id='1',bid='0',size='1')]),
    event(newQuotes=[dict(id='1',bid='100',size='-1')]),
])
def test_native_malformed_depth_permanently_invalidates_current_epoch(payload):
    b=book()
    apply(b,event(newQuotes=[dict(id='1',bid='100',size='1')]))
    with pytest.raises(ValueError):apply(b,payload)
    with pytest.raises(ValueError):b.levels()
    with pytest.raises(ValueError):apply(b,event())


def test_native_epoch_capacity_crossed_and_aggregate_overflow_fail_closed():
    for capacity,payload,epoch in [
        (1,event(newQuotes=[dict(id='1',bid='100',size='1'),dict(id='2',ask='101',size='1')]),3),
        (4,event(newQuotes=[dict(id='1',bid='102',size='1'),dict(id='2',ask='101',size='1')]),3),
        (4,event(newQuotes=[dict(id='1',bid='100',size=str(2**64-1)),dict(id='2',bid='100',size='1')]),3),
        (4,event(),4),
    ]:
        b=book(capacity)
        with pytest.raises(ValueError):apply(b,payload,epoch)
        with pytest.raises(ValueError):b.levels()


@pytest.mark.parametrize('values',[(0,7,3,4),(42,0,3,4),(42,7,0,4),(42,7,3,0),(42,7,3,100001),(True,7,3,4),(42,True,3,4),(42,7,True,4),(42,7,3,True)])
def test_native_depth_constructor_requires_typed_positive_bounded_scope(values):
    with pytest.raises((ValueError,TypeError)):_native.DepthBook(*values)


def test_partial_native_book_unknown_and_repeated_delete_are_idempotent():
    b=book()
    initial=apply(b,event(newQuotes=[dict(id='1',bid='100',size='20'),dict(id='2',ask='102',size='30')]))
    assert apply(b,event(deletedQuotes=['999']))==initial
    deleted=apply(b,event(deletedQuotes=['1']))
    assert deleted['bids']==[] and deleted['asks']==[['102','30']]
    assert apply(b,event(deletedQuotes=['1']))==deleted
    assert deleted['complete'] is False and deleted['source_ts_ns'] is None


def test_unknown_delete_never_creates_capacity_and_other_conflicts_still_invalidate():
    b=book(1)
    apply(b,event(newQuotes=[dict(id='1',bid='100',size='20')]))
    with pytest.raises(ValueError,match='capacity'):
        apply(b,event(newQuotes=[dict(id='2',bid='99',size='20')],deletedQuotes=['999']))
    with pytest.raises(ValueError):b.levels()
    for payload in [event(deletedQuotes=['0']),event(deletedQuotes=['1','1']),event(newQuotes=[dict(id='1',bid='100',size='20')],deletedQuotes=['1'])]:
        b=book()
        apply(b,event(newQuotes=[dict(id='1',bid='100',size='20')]))
        with pytest.raises(ValueError):apply(b,payload)
        with pytest.raises(ValueError):b.levels()


def test_unsigned_zero_size_retains_identity_without_zero_liquidity():
    b=book(2)
    initial=apply(b,event(newQuotes=[dict(id='1',bid='100',size='20'),dict(id='2',ask='101',size=0)]))
    assert initial['bids']==[['100','20']] and initial['asks']==[]
    assert initial['complete'] is False and initial['source_ts_ns'] is None
    assert apply(b,event(newQuotes=[dict(id='1',bid='100',size='0')]))['bids']==[]
    assert apply(b,event(newQuotes=[dict(id='2',ask='101',size='25')]))['asks']==[['101','25']]
    assert apply(b,event(deletedQuotes=['1','2']))['asks']==[]
    b=book(1)
    apply(b,event(newQuotes=[dict(id='1',bid='100',size=0)]))
    with pytest.raises(ValueError,match='capacity'):
        apply(b,event(newQuotes=[dict(id='2',bid='99',size=1)]))

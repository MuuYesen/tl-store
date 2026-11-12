"""AccountState.info is raw framework data; transport encoding belongs to logs."""
import unittest
from types import SimpleNamespace
from tl_store.ctrader.execution import CTraderExecutionClient

class StandardAccountInfo(unittest.IsolatedAsyncioTestCase):
    async def test_large_original_facts_are_published_without_private_carrier(self):
        original = {'facts': [{'id': i, 'original': 'x' * 100} for i in range(800)]}
        funds = dict(currency='USD', observed_utc_ns=1700000000000000000,
                     raw=dict(equity='100', used_margin='2', free_margin='98'))
        class Transport:
            async def funds(self):
                return dict(funds=funds, ctrader_raw_snapshot=original)
        published = []
        client = SimpleNamespace(transport=Transport(), options=SimpleNamespace(economics=None),
                                 generate_account_state=lambda *args: published.append(args))
        await CTraderExecutionClient._refresh_funds(client)
        self.assertEqual(len(published), 1)
        info = published[0][-1]
        self.assertEqual(info['ctrader_raw_snapshot'], original)
        self.assertEqual(info['ctrader_funds']['raw'], funds['raw'])
        self.assertNotIn('leafquant_account_info_encoding_v1', info)

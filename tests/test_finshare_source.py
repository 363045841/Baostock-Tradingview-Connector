"""finshare 期货适配器的无网络单元测试。"""

from datetime import date
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from data.finshare.source import fetch_bars, fetch_snapshot, search, source_capabilities


class FinshareSourceTest(unittest.TestCase):
    """验证 finshare 适配器的领域转换。"""

    def test_source_capabilities_declare_future_instrument_type(self):
        """验证源级能力只声明期货类别和实际 K 线能力。"""
        capabilities = source_capabilities()
        self.assertEqual(capabilities, {
            "instrumentTypes": ["future"],
            "bars": {"periods": ["daily"], "adjustments": ["none"]},
        })

    def test_search_maps_future_symbol_to_exchange(self):
        """验证期货代码搜索返回稳定的 Provider 身份。"""
        result = search("cu0", 10)
        self.assertEqual(result[0]["sourceId"], "finshare")
        self.assertEqual(result[0]["symbol"], "CU0")
        self.assertEqual(result[0]["exchange"], "SHFE")
        self.assertEqual(result[0]["providerRef"], {"futureCode": "CU0"})


    def test_fetch_bars_converts_finshare_model(self):
        """验证 finshare 历史模型字段转换为 V1 K 线字段。"""
        sdk = SimpleNamespace(
            get_future_kline=lambda *args, **kwargs: [
                SimpleNamespace(
                    trade_date=date(2024, 1, 2),
                    open_price=100,
                    high_price=110,
                    low_price=90,
                    close_price=105,
                    volume=20,
                )
            ]
        )
        with patch("data.finshare.source._import_sdk", return_value=sdk):
            result = fetch_bars("CU0", "2024-01-01", "2024-01-31")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["date"], "2024-01-01")
        self.assertEqual(result[0]["open"], 100)
        self.assertEqual(result[0]["high"], 110)
        self.assertEqual(result[0]["low"], 90)
        self.assertEqual(result[0]["close"], 105)
        self.assertEqual(result[0]["volume"], 20)


    def test_fetch_snapshot_converts_finshare_model(self):
        """验证 finshare 实时快照字段转换为 V1 字段。"""
        sdk = SimpleNamespace(
            get_future_snapshot=lambda code: SimpleNamespace(
                last_price=7000,
                volume=12,
                open_interest=300,
                bid1_price=6999,
                ask1_price=7001,
                bid1_volume=5,
                ask1_volume=6,
            )
        )
        with patch("data.finshare.source._import_sdk", return_value=sdk):
            result = fetch_snapshot("IF2409")
        self.assertEqual(result["symbol"], "IF2409")
        self.assertEqual(result["lastPrice"], 7000)
        self.assertEqual(result["openInterest"], 300)
        self.assertEqual(result["bidPrice"], 6999)

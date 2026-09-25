"""V1 K 线游标分页的无网络单元测试。"""

import json
from unittest.mock import patch
import unittest

from api.market_data_v1 import (
    BarRequest,
    _fetch_baostock_bars,
    _fetch_finshare_bars,
    _fetch_tradingview_bars,
    _probe_baostock,
    _probe_tradingview,
    fetch_bars,
)
from data.base import KlineBar


def _request(source_id: str, **kwargs) -> BarRequest:
    """构造统一的 V1 K 线请求。"""
    return BarRequest.model_validate({
        "sourceId": source_id,
        "instrument": {"id": "id", "symbol": "sh.600000", "exchange": "SH"},
        "period": "daily",
        "adjustment": "none",
        "limit": 2,
        **kwargs,
    })


class MarketDataV1BarsTest(unittest.TestCase):
    """验证各 V1 K 线源的游标语义一致。"""

    def test_baostock_initial_page_returns_latest_items_in_ascending_order(self):
        """首屏选取最新 N 根并按时间正序返回。"""
        rows = [
            {"date": "2024-01-03", "open": "3", "high": "3", "low": "3", "close": "3", "volume": "3"},
            {"date": "2024-01-01", "open": "1", "high": "1", "low": "1", "close": "1", "volume": "1"},
            {"date": "2024-01-02", "open": "2", "high": "2", "low": "2", "close": "2", "volume": "2"},
        ]
        with patch("api.market_data_v1.get_stock_k_data", return_value={"success": True, "data": rows}) as fetch:
            result = _fetch_baostock_bars(_request("baostock"))
        self.assertEqual([item["date"] for item in result["items"]], ["2024-01-02", "2024-01-03"])
        self.assertEqual(fetch.call_args.kwargs["start_date"], "1990-01-01")

    def test_finshare_cursor_is_exclusive_and_returns_previous_page_ascending(self):
        """游标不包含自身，上一页仍保持时间正序。"""
        items = [
            {"timestamp": 3_000, "date": "1970-01-01"},
            {"timestamp": 1_000, "date": "1970-01-01"},
            {"timestamp": 2_000, "date": "1970-01-01"},
        ]
        request = _request(
            "finshare",
            beforeTimestamp=3_000,
            instrument={"id": "id", "symbol": "CU0", "exchange": "SHFE"},
        )
        with patch("api.market_data_v1.fetch_finshare_bars", return_value=items):
            result = _fetch_finshare_bars(request)
        self.assertEqual([item["timestamp"] for item in result["items"]], [1_000, 2_000])

    def test_tradingview_uses_latest_snapshot_and_applies_cursor(self):
        """TradingView 的 latest-n-bars SDK 结果在服务层做游标筛选。"""
        bars = [
            KlineBar(seq=1, ts_open=1_000, open=1, high=1, low=1, close=1, volume=1, closed=True),
            KlineBar(seq=2, ts_open=2_000, open=2, high=2, low=2, close=2, volume=2, closed=True),
            KlineBar(seq=3, ts_open=3_000, open=3, high=3, low=3, close=3, volume=3, closed=True),
        ]
        source = unittest.mock.MagicMock()
        source.latest_snapshot.return_value = bars
        with patch("api.market_data_v1.TradingViewSource", return_value=source):
            result = _fetch_tradingview_bars(_request("tradingview", beforeTimestamp=3_000))
        self.assertEqual([item["timestamp"] for item in result["items"]], [1_000, 2_000])
        source.latest_snapshot.assert_called_once()

    def test_invalid_limit_returns_v1_invalid_request_response(self):
        """请求模型校验失败也返回 V1 标准错误码。"""
        response = fetch_bars({
            "sourceId": "baostock",
            "instrument": {"id": "id", "symbol": "sh.600000", "exchange": "SH"},
            "period": "daily",
            "adjustment": "none",
            "limit": 0,
        })
        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.body)["error"]["code"], "INVALID_REQUEST")

    def test_legacy_date_range_fields_are_rejected(self):
        """已移除的日期范围参数不能被静默兼容。"""
        response = fetch_bars({
            "sourceId": "baostock",
            "instrument": {"id": "id", "symbol": "sh.600000", "exchange": "SH"},
            "period": "daily",
            "adjustment": "none",
            "limit": 2,
            "from": 1_000,
            "to": 2_000,
        })
        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.body)["error"]["code"], "INVALID_REQUEST")

    def test_client_payload_with_bar_aggregation_and_cursor_is_accepted(self):
        """前端固定携带 barAggregation / beforeTimestamp，必须被接受并回显聚合口径。"""
        rows = [
            {"date": "2024-01-01", "open": "1", "high": "1", "low": "1", "close": "1", "volume": "1"},
        ]
        with patch("api.market_data_v1.get_stock_k_data", return_value={"success": True, "data": rows}):
            result = fetch_bars({
                "sourceId": "baostock",
                "instrument": {"id": "id", "symbol": "sh.600000", "exchange": "SH"},
                "period": "daily",
                "adjustment": "none",
                "barAggregation": "original",
                "limit": 2,
                "beforeTimestamp": 1_735_603_200_000,
            })
        self.assertEqual(result["data"]["barAggregation"], "original")
        self.assertEqual(result["data"]["olderData"], "exhausted")


class MarketDataV1ProbeTest(unittest.TestCase):
    """验证源级能力与实际数据源能力保持一致。"""

    def test_baostock_probe_declares_bar_capabilities(self):
        """Baostock probe 必须声明 stock K 线能力，否则会被前端路由剔除。"""
        login = unittest.mock.MagicMock(error_code="0", error_msg="")
        with patch("api.market_data_v1.bs") as baostock:
            baostock.login.return_value = login
            result = _probe_baostock()
        self.assertEqual(result["status"], "online")
        self.assertIn("stock", result["capabilities"]["assetClasses"])
        self.assertIn("daily", result["capabilities"]["bars"]["periods"])
        self.assertIn("qfq", result["capabilities"]["bars"]["adjustments"])

    def test_tradingview_probe_declares_bar_capabilities(self):
        """TradingView probe 必须声明 K 线能力，供前端路由筛选 Provider。"""
        with patch.dict("sys.modules", {"tvDatafeed": unittest.mock.MagicMock()}):
            result = _probe_tradingview()
        self.assertEqual(result["status"], "online")
        self.assertIn("index", result["capabilities"]["assetClasses"])
        self.assertIn("daily", result["capabilities"]["bars"]["periods"])
        self.assertIn("none", result["capabilities"]["bars"]["adjustments"])

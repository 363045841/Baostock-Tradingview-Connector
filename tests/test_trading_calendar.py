"""验证日历能力、时间戳口径和转发异常，不连接行情或日历上游。"""

from io import BytesIO
import json
import os
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

from api.market_data_v1 import _instrument, _probe_baostock, fetch_trading_calendar
from data.calendar_client import CALENDAR_URL_ENV


def request(**changes):
    """构造包含完整行情品种引用的 KCQ 日历请求。"""
    return {
        "sourceId": "baostock",
        "instrument": {
            "id": "baostock:stock:sh:600000", "symbol": "sh.600000", "exchange": "SH",
            "providerRef": {"stockCode": "sh.600000"},
        },
        "period": "daily", "adjustment": "none", "barAggregation": "original",
        "anchorTimestamp": 1790697600000, "count": 2, **changes,
    }


def upstream(body):
    """模拟 HTTP 响应体，仅供协议及故障测试使用。"""
    response = MagicMock()
    response.__enter__.return_value = response
    response.read.return_value = json.dumps(body).encode()
    return response


class TradingCalendarTest(unittest.TestCase):
    """验证可选接入不会影响未配置实例或不受支持的品种。"""

    def setUp(self):
        self.env = patch.dict(os.environ, {CALENDAR_URL_ENV: "http://127.0.0.1:8094/"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def assert_error(self, payload, status, code):
        """检查 V1 错误响应和请求标识。"""
        result = fetch_trading_calendar(payload)
        self.assertEqual(result.status_code, status)
        envelope = json.loads(result.body)
        self.assertEqual(envelope["error"]["code"], code)
        self.assertTrue(envelope["requestId"])

    def test_probe_and_instrument_capabilities(self):
        """配置启用源级与沪深品种级能力，北交所不声明支持。"""
        with patch("api.market_data_v1.bs") as bs:
            bs.login.return_value = MagicMock(error_code="0")
            self.assertTrue(_probe_baostock()["capabilities"]["tradingCalendar"])
        for code, expected in [("sh.600000", True), ("sz.000001", True), ("bj.430047", False)]:
            self.assertEqual(_instrument(code, code)["capabilities"]["tradingCalendar"], expected)

    def test_disabled_config_does_not_advertise_or_call_upstream(self):
        """未配置服务时能力为 false，请求明确拒绝且不联网。"""
        with patch.dict(os.environ, {CALENDAR_URL_ENV: ""}), patch("data.calendar_client.urlopen") as send:
            with patch("api.market_data_v1.bs") as bs:
                bs.login.return_value = MagicMock(error_code="0")
                self.assertFalse(_probe_baostock()["capabilities"]["tradingCalendar"])
            self.assertFalse(_instrument("sh.600000", "浦发银行")["capabilities"]["tradingCalendar"])
            self.assert_error(request(), 400, "UNSUPPORTED_CAPABILITY")
            send.assert_not_called()

    def test_forwarding_preserves_identity_and_forces_baostock_date_convention(self):
        """沪深别名映射正确，保留标识和复权；调用方不能改成美股或 UTC 日期。"""
        for exchange, market, symbol in [("SH", "SSE", "sh.600000"), ("SZ", "SZSE", "sz.000001")]:
            payload = request(adjustment="qfq", instrument={
                "id": "original-id", "symbol": symbol, "exchange": exchange,
                "providerRef": {"stockCode": symbol, "calendarExchange": "XNYS", "calendarTimeZone": "UTC"},
            })
            expected = {"anchorTimestamp": payload["anchorTimestamp"], "futureTimestamps": [1791388800000, 1791475200000]}
            with patch("data.calendar_client.urlopen", return_value=upstream({"data": expected})) as send:
                result = fetch_trading_calendar(payload)
            self.assertEqual(result["data"], expected)
            forwarded = json.loads(send.call_args.args[0].data)
            self.assertEqual(send.call_args.args[0].full_url, "http://127.0.0.1:8094/api/v1/market-data/trading-calendar")
            self.assertEqual(send.call_args.kwargs["timeout"], 5)
            self.assertEqual(forwarded["sourceId"], "baostock")
            self.assertEqual(forwarded["adjustment"], "qfq")
            self.assertEqual(forwarded["instrument"]["symbol"], symbol)
            self.assertEqual(forwarded["instrument"]["id"], "original-id")
            self.assertEqual(forwarded["instrument"]["providerRef"], {
                "stockCode": symbol, "calendarExchange": market, "calendarTimeZone": "Asia/Shanghai",
            })

    def test_unsupported_requests_never_reach_upstream(self):
        """分钟、周线、对齐聚合、其他源和北交所不能误用沪深日历。"""
        variants = [
            {"period": "5min"}, {"period": "weekly"}, {"barAggregation": "aligned"},
            {"sourceId": "tradingview"}, {"sourceId": "finshare"},
            {"instrument": {"id": "bj", "symbol": "bj.430047", "exchange": "BJ"}},
        ]
        with patch("data.calendar_client.urlopen") as send:
            for variant in variants:
                with self.subTest(variant=variant):
                    self.assert_error(request(**variant), 400, "UNSUPPORTED_CAPABILITY")
            send.assert_not_called()

    def test_invalid_requests_return_v1_error(self):
        """非法计数、锚点和 sourceId 按协议报错，不发送上游请求。"""
        with patch("data.calendar_client.urlopen") as send:
            for variant in [{"count": 0}, {"count": 4097}, {"count": True}, {"anchorTimestamp": -1}, {"sourceId": "unknown"}]:
                self.assert_error(request(**variant), 400, "INVALID_REQUEST")
            send.assert_not_called()

    def test_coverage_error_is_preserved(self):
        """未覆盖日期的能力错误不转换为空列表或网络故障。"""
        body = {"error": {"code": "UNSUPPORTED_CAPABILITY", "message": "outside coverage"}}
        error = HTTPError("http://localhost", 400, "Bad Request", {}, BytesIO(json.dumps(body).encode()))
        with patch("data.calendar_client.urlopen", side_effect=error):
            self.assert_error(request(), 400, "UNSUPPORTED_CAPABILITY")

    def test_unavailable_timeout_and_malformed_upstream(self):
        """断连、超时、非 JSON 和错误形状返回上游故障，不能伪装有效日历。"""
        for error in [URLError("connection refused"), TimeoutError("timed out")]:
            with patch("data.calendar_client.urlopen", side_effect=error):
                self.assert_error(request(), 502, "UPSTREAM_UNAVAILABLE")
        for data in [None, {"anchorTimestamp": 1, "futureTimestamps": []},
                     {"anchorTimestamp": request()["anchorTimestamp"], "futureTimestamps": [1]}]:
            with patch("data.calendar_client.urlopen", return_value=upstream({"data": data})):
                self.assert_error(request(), 502, "UPSTREAM_UNAVAILABLE")
        response = upstream({})
        response.read.return_value = b"<html>bad gateway</html>"
        with patch("data.calendar_client.urlopen", return_value=response):
            self.assert_error(request(), 502, "UPSTREAM_UNAVAILABLE")

    def test_short_prefix_is_not_filled_with_guessed_dates(self):
        """覆盖末尾保留实际可用前缀，不按自然日补齐。"""
        data = {"anchorTimestamp": request()["anchorTimestamp"], "futureTimestamps": []}
        with patch("data.calendar_client.urlopen", return_value=upstream({"data": data})):
            self.assertEqual(fetch_trading_calendar(request())["data"], data)


if __name__ == "__main__":
    unittest.main()

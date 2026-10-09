"""实时 K 线 SSE 的无网络单元测试：帧编码、扇出去重、收线、断开清理、路由。"""

import asyncio
import json
import unittest
from unittest.mock import patch

from api import market_data_v1
from api.live_bars import (
    HEARTBEAT,
    LiveBarsHub,
    LiveCapacityError,
    LiveFeed,
    Subscriber,
    encode_frame,
    sse_stream,
)

MINUTE = 60_000
T0 = 1_760_000_000_000 - (1_760_000_000_000 % (5 * MINUTE))


def _bar(ts: int, close: float, volume: float = 1.0) -> dict:
    """构造 LiveBar。"""
    return {"timestamp": ts, "open": 1.0, "high": max(1.0, close), "low": min(1.0, close), "close": close, "volume": volume}


def _drain(sub: Subscriber) -> list[dict]:
    """取出订阅者队列中的全部帧并解析为 JSON。"""
    frames = []
    while not sub.queue.empty():
        raw = sub.queue.get_nowait()
        assert raw.startswith("data: ") and raw.endswith("\n\n"), raw
        frames.append(json.loads(raw[len("data: "):]))
    return frames


class _ScriptedFetcher:
    """按脚本依次返回结果（异常则抛出），记录调用次数与请求根数。"""

    def __init__(self, script):
        self.script = list(script)
        self.counts: list[int] = []
        self.closed = False

    def __call__(self, count: int) -> list[dict]:
        self.counts.append(count)
        item = self.script.pop(0) if len(self.script) > 1 else self.script[0]
        if isinstance(item, Exception):
            raise item
        return item

    def close(self) -> None:
        self.closed = True


def _hub(fetchers: dict, **kwargs) -> LiveBarsHub:
    """以键 → 脚本化 fetcher 的映射构造 hub。"""
    created = []

    def factory(key, period):
        fetcher = fetchers[key]
        created.append(key)
        return fetcher, fetcher.close

    options = {
        "poll_interval": 0.01,
        "idle_interval": 0.01,
        "snapshot_size": 50,
        "clock": lambda: (T0 + 2 * MINUTE) / 1000,
    }
    options.update(kwargs)
    hub = LiveBarsHub(factory, **options)
    hub.created = created
    return hub


class FrameEncodingTest(unittest.TestCase):
    def test_encodes_single_unnamed_data_line(self):
        """未命名 data 帧：单行 JSON + 空行结尾，保留非 ASCII。"""
        raw = encode_frame({"type": "status", "symbol": "小米集团", "period": "5min", "status": "live"})
        self.assertTrue(raw.startswith("data: "))
        self.assertTrue(raw.endswith("\n\n"))
        self.assertEqual(raw.count("\n"), 2)
        self.assertNotIn("event:", raw)
        payload = json.loads(raw[len("data: "):])
        self.assertEqual(payload["symbol"], "小米集团")

    def test_heartbeat_is_comment_line(self):
        self.assertEqual(HEARTBEAT, ": ping\n\n")


class LiveFeedApplyTest(unittest.IsolatedAsyncioTestCase):
    def _feed(self, now_ms: int = T0 + 2 * MINUTE) -> tuple[LiveFeed, Subscriber]:
        feed = LiveFeed(
            ("", "AAPL", "5min", "splits"), "5min", lambda n: [],
            poll_interval=5, idle_interval=30, snapshot_size=3, clock=lambda: now_ms / 1000,
        )
        sub = Subscriber(symbol="AAPL", period="5min")
        feed.subscribers.add(sub)
        return feed, sub

    async def test_first_apply_sends_snapshot_then_status(self):
        feed, sub = self._feed()
        feed.apply([_bar(T0 - 10 * MINUTE, 1), _bar(T0 - 5 * MINUTE, 2), _bar(T0, 3), _bar(T0 - 15 * MINUTE, 0.5)])
        frames = _drain(sub)
        self.assertEqual([f["type"] for f in frames], ["snapshot", "status"])
        self.assertEqual([b["timestamp"] for b in frames[0]["bars"]], [T0 - 10 * MINUTE, T0 - 5 * MINUTE, T0])
        self.assertEqual(frames[0]["symbol"], "AAPL")
        self.assertEqual(frames[0]["period"], "5min")
        self.assertEqual(frames[1]["status"], "live")

    async def test_unchanged_bar_is_deduped_and_change_sends_forming(self):
        feed, sub = self._feed()
        feed.apply([_bar(T0, 3)])
        _drain(sub)
        feed.apply([_bar(T0 - 5 * MINUTE, 2), _bar(T0, 3)])
        self.assertEqual(_drain(sub), [])
        feed.apply([_bar(T0, 3.5, volume=9)])
        frames = _drain(sub)
        self.assertEqual([f["type"] for f in frames], ["forming"])
        self.assertEqual(frames[0]["bar"]["close"], 3.5)

    async def test_new_bar_closes_previous_with_final_values_then_forms(self):
        feed, sub = self._feed()
        feed.apply([_bar(T0, 3)])
        _drain(sub)
        feed.apply([_bar(T0, 3.2), _bar(T0 + 5 * MINUTE, 3.3)])
        frames = _drain(sub)
        self.assertEqual([f["type"] for f in frames], ["closed", "forming"])
        self.assertEqual(frames[0]["bar"], _bar(T0, 3.2))
        self.assertEqual(frames[1]["bar"]["timestamp"], T0 + 5 * MINUTE)

    async def test_skipped_bars_are_closed_in_order(self):
        feed, sub = self._feed(now_ms=T0 + 12 * MINUTE)
        feed.apply([_bar(T0, 3)])
        _drain(sub)
        feed.apply([_bar(T0, 3), _bar(T0 + 5 * MINUTE, 4), _bar(T0 + 10 * MINUTE, 5)])
        frames = [f for f in _drain(sub) if f["type"] != "status"]
        self.assertEqual(
            [(f["type"], f["bar"]["timestamp"]) for f in frames],
            [("closed", T0), ("closed", T0 + 5 * MINUTE), ("forming", T0 + 10 * MINUTE)],
        )
        self.assertEqual([b["timestamp"] for b in feed.tail], [T0, T0 + 5 * MINUTE, T0 + 10 * MINUTE])

    async def test_stale_last_bar_reports_market_closed(self):
        feed, sub = self._feed(now_ms=T0 + 3 * 3_600_000)
        feed.apply([_bar(T0, 3)])
        frames = _drain(sub)
        self.assertEqual(frames[-1], {"type": "status", "symbol": "AAPL", "period": "5min", "status": "market-closed"})

    async def test_slow_consumer_is_resynced_with_snapshot(self):
        feed, _ = self._feed()
        slow = Subscriber(symbol="AAPL", period="5min", queue=asyncio.Queue(maxsize=2))
        feed.subscribers.add(slow)
        feed.apply([_bar(T0, 3)])  # snapshot + status 填满队列
        feed.apply([_bar(T0, 4)])  # 溢出 → 清空并等待快照
        self.assertTrue(slow.needs_snapshot)
        feed.apply([_bar(T0, 5)])
        frames = _drain(slow)
        self.assertEqual(frames[0]["type"], "snapshot")
        self.assertEqual(frames[0]["bars"][-1]["close"], 5)


class LiveBarsHubTest(unittest.IsolatedAsyncioTestCase):
    async def _wait_for(self, sub: Subscriber, count: int) -> list[dict]:
        frames = []
        while len(frames) < count:
            raw = await asyncio.wait_for(sub.queue.get(), timeout=2)
            frames.append(json.loads(raw[len("data: "):]))
        return frames

    async def test_subscribers_of_same_series_share_one_upstream_feed(self):
        key = ("", "AAPL", "5min", "splits")
        fetcher = _ScriptedFetcher([[_bar(T0, 1)], [_bar(T0, 2)], [_bar(T0, 2), _bar(T0 + 5 * MINUTE, 3)]])
        hub = _hub({key: fetcher})
        feed_a, sub_a = hub.subscribe(key, "5min", "AAPL")
        feed_b, sub_b = hub.subscribe(key, "5min", "aapl")
        self.assertIs(feed_a, feed_b)
        self.assertEqual(hub.created, [key])

        frames_a = await self._wait_for(sub_a, 5)
        frames_b = await self._wait_for(sub_b, 5)
        expected = ["snapshot", "status", "forming", "closed", "forming"]
        self.assertEqual([f["type"] for f in frames_a], expected)
        self.assertEqual([f["type"] for f in frames_b], expected)
        # 每个连接回显自己请求中的 symbol 字符串
        self.assertEqual({f["symbol"] for f in frames_a}, {"AAPL"})
        self.assertEqual({f["symbol"] for f in frames_b}, {"aapl"})
        # 首次拉快照根数，之后只拉尾部
        self.assertEqual(fetcher.counts[0], 50)
        self.assertEqual(set(fetcher.counts[1:3]), {3})
        hub.close()

    async def test_late_subscriber_gets_cached_snapshot_immediately(self):
        key = ("", "AAPL", "5min", "splits")
        fetcher = _ScriptedFetcher([[_bar(T0, 1)]])
        hub = _hub({key: fetcher})
        _, first = hub.subscribe(key, "5min", "AAPL")
        await self._wait_for(first, 2)
        _, late = hub.subscribe(key, "5min", "AAPL")
        frames = _drain(late)
        self.assertEqual([f["type"] for f in frames], ["snapshot", "status"])
        self.assertEqual(len(hub.created), 1)
        hub.close()

    async def test_upstream_error_is_reported_and_recovers(self):
        key = ("", "AAPL", "5min", "splits")
        fetcher = _ScriptedFetcher([RuntimeError("tv down"), [_bar(T0, 1)]])
        hub = _hub({key: fetcher})
        _, sub = hub.subscribe(key, "5min", "AAPL")
        frames = await self._wait_for(sub, 3)
        self.assertEqual(frames[0], {"type": "status", "symbol": "AAPL", "period": "5min", "status": "upstream-error", "detail": "tv down"})
        self.assertEqual([f["type"] for f in frames[1:]], ["snapshot", "status"])
        self.assertEqual(frames[2]["status"], "live")
        hub.close()

    async def test_single_transient_error_with_data_is_not_reported(self):
        key = ("", "AAPL", "5min", "splits")
        fetcher = _ScriptedFetcher([
            [_bar(T0, 1)], RuntimeError("empty"), [_bar(T0, 1)],
            RuntimeError("down"), RuntimeError("down"), [_bar(T0, 1)],
        ])
        hub = _hub({key: fetcher})
        _, sub = hub.subscribe(key, "5min", "AAPL")
        frames = await self._wait_for(sub, 4)
        self.assertEqual(
            [(f["type"], f.get("status")) for f in frames],
            [("snapshot", None), ("status", "live"), ("status", "upstream-error"), ("status", "live")],
        )
        self.assertEqual(frames[2]["detail"], "down")
        hub.close()

    async def test_capacity_limits(self):
        keys = [("", "AAPL", "5min", "splits"), ("", "MSFT", "5min", "splits")]
        hub = _hub({k: _ScriptedFetcher([[_bar(T0, 1)]]) for k in keys}, max_feeds=1, max_subscribers_per_feed=1)
        hub.subscribe(keys[0], "5min", "AAPL")
        with self.assertRaises(LiveCapacityError):
            hub.subscribe(keys[1], "5min", "MSFT")
        with self.assertRaises(LiveCapacityError):
            hub.subscribe(keys[0], "5min", "AAPL")
        hub.close()

    async def test_disconnect_unsubscribes_and_stops_last_feed(self):
        key = ("", "AAPL", "5min", "splits")
        fetcher = _ScriptedFetcher([[_bar(T0, 1)]])
        hub = _hub({key: fetcher})
        feed, sub_a = hub.subscribe(key, "5min", "AAPL")
        _, sub_b = hub.subscribe(key, "5min", "AAPL")

        async def connected():
            return False

        stream_a = sse_stream(hub, feed, sub_a, connected)
        self.assertEqual(await stream_a.__anext__(), ": connected\n\n")
        self.assertTrue((await stream_a.__anext__()).startswith("data: "))
        await stream_a.aclose()  # 客户端断开：Starlette 关闭/取消生成器
        self.assertNotIn(sub_a, feed.subscribers)
        self.assertIn(key, hub.feeds)

        disconnected = asyncio.Event()

        async def is_disconnected():
            return disconnected.is_set()

        _drain(sub_b)
        stream_b = sse_stream(hub, feed, sub_b, is_disconnected, heartbeat_interval=0.05)
        await stream_b.__anext__()
        hub.feeds[key]._task.cancel()  # 停止轮询，只剩心跳
        self.assertEqual(await stream_b.__anext__(), HEARTBEAT)
        disconnected.set()
        with self.assertRaises(StopAsyncIteration):
            await stream_b.__anext__()
        self.assertEqual(hub.feeds, {})
        await asyncio.sleep(0.05)  # on_close 在线程池中执行
        self.assertTrue(fetcher.closed)


class _FakeRequest:
    async def is_disconnected(self):
        return False


class StreamRouteTest(unittest.IsolatedAsyncioTestCase):
    async def _call(self, source_id="tradingview", **params):
        values = {
            "symbol": "AAPL", "period": "5min", "bar_aggregation": "original",
            "adjustment": "splits", "exchange": "", "instrument_id": "",
        }
        values.update(params)
        return await market_data_v1.stream_live_bars(_FakeRequest(), source_id=source_id, **values)

    async def test_probe_and_instruments_declare_live_bars(self):
        self.assertIs(market_data_v1._TV_SOURCE_CAPABILITIES["liveBars"], True)
        self.assertIs(market_data_v1._tv_instrument("AAPL")["capabilities"]["liveBars"], True)
        self.assertNotIn("liveBars", market_data_v1._BAOSTOCK_SOURCE_CAPABILITIES)

    async def test_rejects_unsupported_source_and_invalid_params(self):
        cases = [
            ({"source_id": "baostock"}, "UNSUPPORTED_CAPABILITY"),
            ({"source_id": "nope"}, "INVALID_REQUEST"),
            ({"symbol": " "}, "INVALID_REQUEST"),
            ({"period": "2min"}, "INVALID_REQUEST"),
            ({"bar_aggregation": "weird"}, "INVALID_REQUEST"),
            ({"adjustment": "hfq"}, "INVALID_REQUEST"),
            ({"instrument_id": "gotdx:stock:sh:600000"}, "INVALID_REQUEST"),
            ({"instrument_id": "tradingview:NASDAQ"}, "INVALID_REQUEST"),
        ]
        for params, code in cases:
            response = await self._call(**params)
            self.assertEqual(response.status_code, 400, params)
            self.assertEqual(json.loads(response.body)["error"]["code"], code, params)

    async def test_streams_event_stream_with_sse_headers(self):
        captured = {}

        def factory(key, period):
            captured["key"] = key
            fetcher = _ScriptedFetcher([[_bar(T0, 1)]])
            return fetcher, fetcher.close

        hub = LiveBarsHub(factory, poll_interval=0.01, idle_interval=0.01, snapshot_size=10, clock=lambda: T0 / 1000)
        with patch.object(market_data_v1, "live_bars_hub", hub):
            response = await self._call(symbol="nasdaq:AAPL", period="5min", adjustment="qfq")
            self.assertEqual(response.media_type, "text/event-stream")
            self.assertEqual(response.headers["cache-control"], "no-cache")
            self.assertEqual(response.headers["x-accel-buffering"], "no")
            self.assertEqual(captured["key"], ("NASDAQ", "AAPL", "5min", "dividends"))
            body = response.body_iterator
            self.assertEqual(await body.__anext__(), ": connected\n\n")
            snapshot = json.loads((await body.__anext__())[len("data: "):])
            self.assertEqual(snapshot["type"], "snapshot")
            self.assertEqual(snapshot["symbol"], "nasdaq:AAPL")
            await body.aclose()
        self.assertEqual(hub.feeds, {})

    def _recording_hub(self, keys: list):
        def factory(key, period):
            keys.append(key)
            fetcher = _ScriptedFetcher([[_bar(T0, 1)]])
            return fetcher, fetcher.close

        return LiveBarsHub(factory, poll_interval=0.01, idle_interval=0.01, snapshot_size=10, clock=lambda: T0 / 1000)

    async def test_instrument_id_matches_search_and_bars_resolution(self):
        """instrumentId 用 _tv_instrument 的 id 格式，解析出的交易所与 /bars 一致。"""
        for exchange, expected in [("NASDAQ", ("NASDAQ", "AAPL")), ("", ("", "AAPL"))]:
            instrument = market_data_v1._tv_instrument("AAPL", exchange)
            self.assertEqual(market_data_v1._tv_ref_from_instrument_id(instrument["id"]), expected)
            source = unittest.mock.MagicMock()
            source.latest_snapshot.return_value = []
            request = market_data_v1.BarRequest.model_validate({
                "sourceId": "tradingview",
                "instrument": {k: instrument[k] for k in ("id", "symbol", "exchange", "providerRef")},
                "period": "5min", "adjustment": "splits", "limit": 2,
            })
            with patch("api.market_data_v1.TradingViewSource", return_value=source):
                market_data_v1._fetch_tradingview_bars(request)
            bars_exchange = source.set_exchange.call_args.args[0]
            self.assertEqual("" if bars_exchange.upper() in ("", "AUTO") else bars_exchange, expected[0])

    async def test_instrument_id_takes_precedence_over_exchange_and_symbol(self):
        keys = []
        hub = self._recording_hub(keys)
        with patch.object(market_data_v1, "live_bars_hub", hub):
            response = await self._call(symbol="AAPL", exchange="CAPITALCOM", instrument_id="tradingview:NASDAQ:AAPL")
            await response.body_iterator.aclose()
            response = await self._call(symbol="AAPL", exchange="CAPITALCOM")
            await response.body_iterator.aclose()
        self.assertEqual(keys, [("NASDAQ", "AAPL", "5min", "splits"), ("CAPITALCOM", "AAPL", "5min", "splits")])
        hub.close()

    async def test_same_symbol_different_instrument_ids_get_separate_pollers(self):
        keys = []
        hub = self._recording_hub(keys)
        with patch.object(market_data_v1, "live_bars_hub", hub):
            nasdaq = await self._call(symbol="AAPL", instrument_id="tradingview:NASDAQ:AAPL")
            auto = await self._call(symbol="AAPL", instrument_id="tradingview:auto:AAPL")
            nasdaq_again = await self._call(symbol="AAPL", instrument_id="tradingview:NASDAQ:AAPL")
            self.assertEqual(len(hub.feeds), 2)
            self.assertEqual(keys, [("NASDAQ", "AAPL", "5min", "splits"), ("", "AAPL", "5min", "splits")])
            for response in (nasdaq, auto, nasdaq_again):
                body = response.body_iterator
                await body.__anext__()
                frame = json.loads((await body.__anext__())[len("data: "):])
                self.assertEqual((frame["type"], frame["symbol"]), ("snapshot", "AAPL"))
                await body.aclose()
        self.assertEqual(hub.feeds, {})

    async def test_capacity_error_returns_503(self):
        hub = _hub({}, max_feeds=1)
        hub.feeds[("x",)] = object()  # 占满
        with patch.object(market_data_v1, "live_bars_hub", hub):
            response = await self._call()
        self.assertEqual(response.status_code, 503)

    async def test_tv_fetcher_pins_resolved_exchange_after_auto_probe(self):
        from data.base import KlineBar

        source = unittest.mock.MagicMock()
        source.resolved_pair = ("NASDAQ", "AAPL")
        source.latest_snapshot.return_value = [
            KlineBar(seq=1, ts_open=T0, open=1, high=2, low=0.5, close=1.5, volume=10, closed=False)
        ]
        with patch("api.market_data_v1.TradingViewSource", return_value=source):
            fetcher = market_data_v1._TvLiveFetcher("", "AAPL", "5m", "splits")
            bars = fetcher(3)
            fetcher(3)
        self.assertEqual(bars, [{"timestamp": T0, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10}])
        source.set_exchange.assert_any_call("")
        source.set_exchange.assert_called_with("NASDAQ")
        self.assertEqual(source.subscribe.call_count, 2)  # 初次订阅 + 固定交易所后重订阅一次


if __name__ == "__main__":
    unittest.main()

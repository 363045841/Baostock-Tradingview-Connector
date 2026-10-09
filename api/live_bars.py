"""实时 K 线 SSE：共享上游轮询 + 订阅者扇出。

每个 (exchange, symbol, timeframe, adjustment) 只保留一个上游轮询任务（LiveFeed），
所有订阅该序列的 SSE 连接共享其结果。同步的 TradingView 客户端在线程中运行，
不阻塞事件循环。帧协议（KCQ V1 实时 K 线契约）：

- ``{type: 'snapshot', symbol, period, bars: LiveBar[]}``
- ``{type: 'forming',  symbol, period, bar}``
- ``{type: 'closed',   symbol, period, bar}``
- ``{type: 'status',   symbol, period, status, detail?}``

``LiveBar = {timestamp(UTC ms), open, high, low, close, volume?}``，时间戳与
``POST /api/v1/market-data/bars`` 返回的 ``timestamp`` 同源（``int(KlineBar.ts_open)``）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import AsyncIterator, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

HEARTBEAT = ": ping\n\n"
HEARTBEAT_INTERVAL_S = 15.0

STATUS_LIVE = "live"
STATUS_MARKET_CLOSED = "market-closed"
STATUS_UPSTREAM_ERROR = "upstream-error"

_PERIOD_MS = {
    "1min": 60_000,
    "5min": 5 * 60_000,
    "15min": 15 * 60_000,
    "30min": 30 * 60_000,
    "60min": 60 * 60_000,
    "daily": 24 * 3_600_000,
    "weekly": 7 * 24 * 3_600_000,
    "monthly": 31 * 24 * 3_600_000,
}


def _env_number(name: str, default: float) -> float:
    """读取数值型环境变量，非法值回退默认。"""
    raw = os.environ.get(name, "")
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def encode_frame(frame: dict) -> str:
    """把一帧编码为未命名 SSE 消息：单行 ``data: <json>`` + 空行。"""
    return f"data: {json.dumps(frame, ensure_ascii=False, separators=(',', ':'))}\n\n"


def period_ms(period: str) -> int:
    """返回 V1 周期的名义时长（毫秒）。"""
    return _PERIOD_MS[period]


class LiveCapacityError(Exception):
    """订阅数量超出上限。"""


Fetcher = Callable[[int], list[dict]]
"""同步拉取最近 n 根 LiveBar（时间正序），在线程中执行。"""


@dataclass(eq=False)
class Subscriber:
    """单个 SSE 连接：按自己请求里的 symbol/period 回显帧。"""

    symbol: str
    period: str
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=256))
    needs_snapshot: bool = True

    def push(self, frame_type: str, **payload: object) -> None:
        """入队一帧；慢消费者队列满时清空并改为等待下一次快照重同步。"""
        frame = {"type": frame_type, "symbol": self.symbol, "period": self.period, **payload}
        try:
            self.queue.put_nowait(encode_frame(frame))
        except asyncio.QueueFull:
            while not self.queue.empty():
                self.queue.get_nowait()
            self.needs_snapshot = True


class LiveFeed:
    """一个序列的共享上游轮询：去重、收线检测、市场状态，并扇出给订阅者。"""

    def __init__(
        self,
        key: tuple,
        period: str,
        fetcher: Fetcher,
        *,
        poll_interval: float,
        idle_interval: float,
        snapshot_size: int,
        tail_size: int = 3,
        clock: Callable[[], float] = time.time,
        on_close: Optional[Callable[[], None]] = None,
    ) -> None:
        self.key = key
        self.period = period
        self.subscribers: set[Subscriber] = set()
        self.tail: list[dict] = []
        self.status: Optional[str] = None
        self.status_detail: Optional[str] = None
        self._fetcher = fetcher
        self._poll_interval = poll_interval
        self._idle_interval = idle_interval
        self._snapshot_size = snapshot_size
        self._tail_size = tail_size
        self._clock = clock
        self._on_close = on_close
        self._task: Optional[asyncio.Task] = None
        self._errors = 0

    # ---------- 订阅管理 ----------

    def add(self, sub: Subscriber) -> None:
        """加入订阅者；已有缓存尾部时立即补发快照与当前状态。"""
        self.subscribers.add(sub)
        if self.tail:
            self._send_snapshot(sub)
        elif self.status == STATUS_UPSTREAM_ERROR:
            sub.push("status", status=self.status, **self._detail())
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run())

    def remove(self, sub: Subscriber) -> None:
        """移除订阅者。"""
        self.subscribers.discard(sub)

    def stop(self) -> None:
        """取消轮询任务并释放上游连接。"""
        if self._task is not None:
            self._task.cancel()
            self._task = None
        if self._on_close is not None:
            on_close, self._on_close = self._on_close, None
            # 关闭 socket 可能短暂阻塞；放到线程里，同时打断仍在进行的拉取
            asyncio.get_running_loop().run_in_executor(None, on_close)

    # ---------- 轮询 ----------

    async def _run(self) -> None:
        """轮询上游直到被取消；失败指数退避，休市时降频。"""
        while True:
            need_snapshot = not self.tail or any(s.needs_snapshot for s in self.subscribers)
            count = self._snapshot_size if need_snapshot else self._tail_size
            try:
                bars = await asyncio.to_thread(self._fetcher, count)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 上游任何异常都转为状态帧，不终止流
                self._errors += 1
                logger.warning("live bars fetch failed for %s: %s", self.key, exc)
                # TradingView 偶发空响应：已有数据时单次失败不打扰客户端，连续失败才上报
                if not self.tail or self._errors >= 2:
                    self._set_status(STATUS_UPSTREAM_ERROR, str(exc) or exc.__class__.__name__)
                delay = min(self._poll_interval * (2 ** self._errors), 60.0)
            else:
                self._errors = 0
                self.apply(bars)
                delay = self._poll_interval if self.status == STATUS_LIVE else self._idle_interval
            await asyncio.sleep(delay)

    def apply(self, bars: list[dict]) -> None:
        """合并一次拉取结果：首帧发快照，此后只发变化的 forming / closed 帧。"""
        incoming: dict[int, dict] = {}
        for bar in bars:
            incoming[int(bar["timestamp"])] = bar
        ordered = [incoming[ts] for ts in sorted(incoming)]
        if not ordered:
            return

        events: list[tuple[str, dict]] = []
        if self.tail:
            last = self.tail[-1]
            last_ts = last["timestamp"]
            newer = [bar for bar in ordered if bar["timestamp"] > last_ts]
            final_last = incoming.get(last_ts, last)
            if newer:
                # 新 K 线出现即视为上一根收线；中间被跳过的完整 K 线同样以 closed 补发
                events.append(("closed", final_last))
                events.extend(("closed", bar) for bar in newer[:-1])
                events.append(("forming", newer[-1]))
            elif final_last != last:
                events.append(("forming", final_last))
            merged = {bar["timestamp"]: bar for bar in self.tail}
            merged.update({ts: bar for ts, bar in incoming.items() if ts >= self.tail[0]["timestamp"]})
            self.tail = [merged[ts] for ts in sorted(merged)][-self._snapshot_size:]
        else:
            self.tail = ordered[-self._snapshot_size:]

        status = self._market_status()
        status_changed = (status, None) != (self.status, self.status_detail)
        self.status, self.status_detail = status, None

        for sub in list(self.subscribers):
            if sub.needs_snapshot:
                self._send_snapshot(sub)
                continue
            for frame_type, bar in events:
                sub.push(frame_type, bar=bar)
            if status_changed:
                sub.push("status", status=status)

    def _market_status(self) -> str:
        """末根 K 线的名义区间已过去仍无新 K 线时判定为休市。"""
        last_ts = self.tail[-1]["timestamp"]
        grace_ms = max(60_000, int(self._poll_interval * 3_000))
        now_ms = int(self._clock() * 1000)
        if now_ms < last_ts + period_ms(self.period) + grace_ms:
            return STATUS_LIVE
        return STATUS_MARKET_CLOSED

    def _set_status(self, status: str, detail: Optional[str]) -> None:
        """状态变化时广播 status 帧。"""
        if status == self.status and detail == self.status_detail:
            return
        self.status = status
        self.status_detail = detail
        for sub in list(self.subscribers):
            sub.push("status", status=status, **self._detail())

    def _send_snapshot(self, sub: Subscriber) -> None:
        """向单个订阅者补发快照（以及已知的当前状态）。"""
        sub.needs_snapshot = False
        sub.push("snapshot", bars=list(self.tail))
        if self.status is not None:
            sub.push("status", status=self.status, **self._detail())

    def _detail(self) -> dict:
        return {"detail": self.status_detail} if self.status_detail else {}


FeedFactory = Callable[[tuple, str], tuple[Fetcher, Optional[Callable[[], None]]]]


class LiveBarsHub:
    """按序列键管理共享 LiveFeed，限制总序列数与单序列订阅数。"""

    def __init__(
        self,
        feed_factory: FeedFactory,
        *,
        max_feeds: Optional[int] = None,
        max_subscribers_per_feed: Optional[int] = None,
        poll_interval: Optional[float] = None,
        idle_interval: Optional[float] = None,
        snapshot_size: Optional[int] = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._feed_factory = feed_factory
        self.max_feeds = max_feeds or int(_env_number("LIVE_BARS_MAX_FEEDS", 32))
        self.max_subscribers_per_feed = max_subscribers_per_feed or int(
            _env_number("LIVE_BARS_MAX_SUBSCRIBERS_PER_FEED", 200)
        )
        self.poll_interval = poll_interval or _env_number("LIVE_BARS_POLL_INTERVAL_S", 5.0)
        self.idle_interval = idle_interval or _env_number("LIVE_BARS_IDLE_INTERVAL_S", 30.0)
        self.snapshot_size = snapshot_size or int(_env_number("LIVE_BARS_SNAPSHOT_SIZE", 50))
        self._clock = clock
        self.feeds: dict[tuple, LiveFeed] = {}

    def subscribe(self, key: tuple, period: str, symbol: str) -> tuple[LiveFeed, Subscriber]:
        """加入（必要时创建）共享序列；超限抛 LiveCapacityError。"""
        feed = self.feeds.get(key)
        if feed is None:
            if len(self.feeds) >= self.max_feeds:
                raise LiveCapacityError(f"too many live series (max {self.max_feeds})")
            fetcher, on_close = self._feed_factory(key, period)
            feed = LiveFeed(
                key,
                period,
                fetcher,
                poll_interval=self.poll_interval,
                idle_interval=self.idle_interval,
                snapshot_size=self.snapshot_size,
                clock=self._clock,
                on_close=on_close,
            )
            self.feeds[key] = feed
        elif len(feed.subscribers) >= self.max_subscribers_per_feed:
            raise LiveCapacityError(
                f"too many subscribers for this series (max {self.max_subscribers_per_feed})"
            )
        sub = Subscriber(symbol=symbol, period=period)
        feed.add(sub)
        return feed, sub

    def unsubscribe(self, feed: LiveFeed, sub: Subscriber) -> None:
        """移除订阅者；最后一个订阅者离开时停止并移除序列。"""
        feed.remove(sub)
        if not feed.subscribers and self.feeds.get(feed.key) is feed:
            del self.feeds[feed.key]
            feed.stop()

    def close(self) -> None:
        """停止全部序列（服务关闭时调用）。"""
        for feed in list(self.feeds.values()):
            feed.stop()
        self.feeds.clear()


async def sse_stream(
    hub: LiveBarsHub,
    feed: LiveFeed,
    sub: Subscriber,
    is_disconnected: Callable[[], Awaitable[bool]],
    heartbeat_interval: float = HEARTBEAT_INTERVAL_S,
) -> AsyncIterator[str]:
    """把订阅者队列转为 SSE 文本流；空闲时发心跳，断开或取消时退订。"""
    try:
        yield ": connected\n\n"
        while True:
            try:
                chunk = await asyncio.wait_for(sub.queue.get(), timeout=heartbeat_interval)
            except asyncio.TimeoutError:
                if await is_disconnected():
                    return
                chunk = HEARTBEAT
            yield chunk
    finally:
        hub.unsubscribe(feed, sub)

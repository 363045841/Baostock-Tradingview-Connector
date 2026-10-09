from __future__ import annotations

import logging
import os
import platform
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import datetime, timedelta

from data.base import (
    DataSource,
    DataSourceTransientError,
    KlineBar,
    normalize_kline_bar,
)
from data.datetime_ts import datetime_to_ts_ms
from data.tradingview.bar_close_wait import (
    seconds_until_bar_closes,
)
from data.tradingview.config import load_proxy_config
from data.tradingview.market_defaults import (
    is_tv_exchange_auto,
    resolve_tv_fetch_pair,
    tv_auto_probe_plan,
)
from data.tradingview.symbol_lookup import TvSymbolNotFoundError, is_tv_name_input
from data.tradingview.errors import format_tradingview_fetch_error

logger = logging.getLogger(__name__)

_TV_FETCH_RETRIES = 1
_TV_FETCH_RETRY_SLEEP_S = 0.5
_TV_WS_TIMEOUT_S = 10.0
_TV_WS_TIMEOUT_ATTR = "_TvDatafeed__ws_timeout"
# 单次 get_hist 总超时：需早于前端 15s 的 Effect.timeout，让业务错误先返回
_TV_FETCH_TIMEOUT_S = 12.0

def _ensure_proxy() -> None:
    """Set HTTP_PROXY / HTTPS_PROXY env vars for tvDatafeed WebSocket connections.

    Priority (first wins):
      1. Env vars ``HTTP_PROXY`` / ``HTTPS_PROXY`` — already set by user
      2. ``config/proxy.json`` — explicit static config
      3. Windows registry (WinINET proxy) — auto-detect
    """
    if os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY"):
        return

    from data.tradingview.config import load_proxy_config

    cfg = load_proxy_config()
    if cfg.get("http"):
        os.environ.setdefault("HTTP_PROXY", cfg["http"])
    if cfg.get("https"):
        os.environ.setdefault("HTTPS_PROXY", cfg["https"])
    if cfg.get("no_proxy"):
        os.environ.setdefault("NO_PROXY", cfg["no_proxy"])
    if cfg:
        proxy = cfg.get("http") or cfg.get("https") or ""
        logger.info("Using proxy from config/proxy.json: %s", proxy)
        return

    # Fallback: Windows WinINET registry
    if platform.system() != "Windows":
        return
    try:
        import winreg  # type: ignore[import]

        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Internet Settings",
        ) as key:
            enabled = winreg.QueryValueEx(key, "ProxyEnable")[0]
            server = winreg.QueryValueEx(key, "ProxyServer")[0]
            if enabled and server:
                proxy_url = f"http://{server}"
                os.environ.setdefault("HTTP_PROXY", proxy_url)
                os.environ.setdefault("HTTPS_PROXY", proxy_url)
                logger.info("Using Windows proxy (auto-detected): %s", proxy_url)
    except Exception:
        logger.debug("Could not read Windows proxy settings", exc_info=True)


_TF_MAP: dict[str, str] = {
    "1m":  "in_1_minute",
    "3m":  "in_3_minute",
    "5m":  "in_5_minute",
    "15m": "in_15_minute",
    "30m": "in_30_minute",
    "45m": "in_45_minute",
    "1h":  "in_1_hour",
    "2h":  "in_2_hour",
    "3h":  "in_3_hour",
    "4h":  "in_4_hour",
    "1d":  "in_daily",
    "1w":  "in_weekly",
    "1M":  "in_monthly",
}

_BARS_PER_DAY: dict[str, float] = {
    "1m": 390,
    "3m": 130,
    "5m": 78,
    "15m": 26,
    "30m": 13,
    "45m": 8.7,
    "1h": 6.5,
    "2h": 3.25,
    "3h": 2.2,
    "4h": 1.6,
    "1d": 1,
    "1w": 1 / 7,
    "1M": 1 / 30,
}


class TradingViewSource(DataSource):
    def __init__(self, username: str = "", password: str = "", adjust: str = "splits") -> None:
        """TradingView data source.

        Args:
            adjust: 复权类型。``"splits"`` 仅拆股调整，``"dividends"`` 前复权（含分红），``"none"`` 不复权。
        """
        self._username = username
        self._password = password
        self._adjust = adjust
        self._tv = None
        self._connected: bool = False
        self._symbol: str = ""
        self._timeframe: str = ""
        self._exchange: str = ""
        self._resolved_pair: tuple[str, str] | None = None
        self._snapshot_lock = threading.Lock()

    @property
    def exchange(self) -> str:
        return self._exchange

    @property
    def resolved_pair(self) -> tuple[str, str] | None:
        """最近一次成功拉取实际使用的 (exchange, symbol)，供轮询方固定交易所、跳过重复探测。"""
        return self._resolved_pair

    def set_exchange(self, exchange: str) -> None:
        self._exchange = (exchange or "").strip().upper()

    def connect(self) -> None:
        _ensure_proxy()
        try:
            from tvDatafeed import TvDatafeed
            if self._username and self._password:
                self._tv = TvDatafeed(self._username, self._password)
            else:
                self._tv = TvDatafeed()
            try:
                setattr(self._tv, _TV_WS_TIMEOUT_ATTR, _TV_WS_TIMEOUT_S)
            except Exception:
                logger.debug("Could not override tvDatafeed ws timeout", exc_info=True)
            self._connected = True
            logger.info("TradingViewSource connected (anonymous=%s)", not self._username)
        except Exception as exc:
            self._connected = False
            raise DataSourceTransientError(
                f"TradingView 连接失败：{exc}（若未安装请执行 "
                "pip install git+https://github.com/rongardF/tvdatafeed.git）"
            ) from exc

    def disconnect(self) -> None:
        self._close_tv_socket()
        self._tv = None
        self._connected = False
        logger.info("TradingViewSource disconnected")

    def _close_tv_socket(self) -> None:
        tv = self._tv
        if tv is None:
            return
        ws = getattr(tv, "ws", None)
        if ws is None:
            return
        try:
            ws.close()
        except Exception:
            logger.debug("tvDatafeed socket close failed", exc_info=True)
        finally:
            try:
                tv.ws = None
            except Exception:
                pass

    def list_symbols(self) -> list[str]:
        return [
            "XAUUSD", "GOLD", "600519", "000001", "1810", "700",
            "小米集团", "腾讯控股", "EURUSD", "GBPUSD",
        ]

    def supported_timeframes(self) -> list[str]:
        return list(_TF_MAP.keys())

    def subscribe(self, symbol: str, timeframe: str) -> None:
        if timeframe not in _TF_MAP:
            raise ValueError(f"Unsupported timeframe: {timeframe!r}. Use one of {list(_TF_MAP)}")
        self._timeframe = timeframe
        self._symbol = symbol.strip()
        self._close_tv_socket()
        logger.info(
            "TradingViewSource subscribed: %s %s exchange=%s",
            self._symbol, timeframe, self._exchange or "(auto)",
        )

    def unsubscribe(self) -> None:
        self._symbol = ""
        self._timeframe = ""
        logger.info("TradingViewSource unsubscribed")

    def _fetch_hist_with_retry(
        self,
        *,
        symbol: str,
        exchange: str,
        interval: object,
        n_bars: int,
        adjustment: str = "",
    ):
        logger.debug(
            "TradingView get_hist: symbol=%s, exchange=%s, interval=%s, n_bars=%d, adjustment=%s",
            symbol, exchange, interval, n_bars, adjustment or "(default)",
        )
        adjustment = adjustment or self._adjust
        last_exc: BaseException | None = None
        for attempt in range(1, _TV_FETCH_RETRIES + 1):
            try:
                df = self._get_hist_with_timeout(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    n_bars=n_bars,
                    adjustment=adjustment,
                )
                if df is not None and not df.empty:
                    return df
                logger.warning(
                    "TradingView get_hist attempt %s/%s returned empty data: symbol=%s, exchange=%s, interval=%s",
                    attempt, _TV_FETCH_RETRIES, symbol, exchange, interval,
                )
                last_exc = None
            except Exception as exc:
                last_exc = exc
                logger.debug(
                    "TradingView get_hist attempt %s/%s failed: %s",
                    attempt, _TV_FETCH_RETRIES, exc,
                )
            finally:
                self._close_tv_socket()
            if attempt < _TV_FETCH_RETRIES:
                time.sleep(_TV_FETCH_RETRY_SLEEP_S)
        if last_exc is not None:
            raise last_exc
        return None

    def _get_hist_with_timeout(
        self,
        *,
        symbol: str,
        exchange: str,
        interval: object,
        n_bars: int,
        adjustment: str,
    ):
        """带总超时地调用 tvDatafeed.get_hist，避免 recv 无限阻塞。

        get_hist 的 recv 循环对无效品种不设硬超时，可能远超前端 15s 等待，
        这里用独立线程 + future.result(timeout) 兜底，超时即抛可识别错误。
        """
        pool = ThreadPoolExecutor(max_workers=1)
        future = pool.submit(
            self._tv.get_hist,
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            n_bars=n_bars,
            adjustment=adjustment,
        )
        try:
            return future.result(timeout=_TV_FETCH_TIMEOUT_S)
        except FutureTimeout:
            self._close_tv_socket()
            raise DataSourceTransientError(
                f"TradingView 拉取超时（{exchange or ''} / {symbol}）："
                f"超过 {int(_TV_FETCH_TIMEOUT_S)}s 未返回，请检查交易所与品种是否有效"
            )
        finally:
            # 不等待后台 recv 线程结束，避免再次阻塞；socket 已由超时路径或调用方关闭
            pool.shutdown(wait=False, cancel_futures=True)

    def _fetch_tv_auto_probe(
        self,
        *,
        symbol: str,
        plan: list[tuple[str, str]],
        interval: object,
        n_bars: int,
        adjustment: str = "",
    ) -> tuple[object, str]:
        if not plan:
            raise DataSourceTransientError(
                f"TradingView 无法识别品种「{symbol}」；"
                "请用 A 股 6 位代码、港股代码（如 1810）、"
                "指数代码（如 SPX、NDX、VIX）、"
                "外汇/黄金代码或已支持的股票名称"
            )
        last_exc: BaseException | None = None
        tried: list[str] = []
        for exchange, code in plan:
            label = f"{exchange}:{code}"
            tried.append(label)
            try:
                df = self._fetch_hist_with_retry(
                    symbol=code,
                    exchange=exchange,
                    interval=interval,
                    n_bars=n_bars,
                    adjustment=adjustment,
                )
            except Exception as exc:
                last_exc = exc
                logger.info("TradingView auto probe %s failed: %s", label, exc)
                continue
            if df is not None and not df.empty:
                logger.info(
                    "TradingView auto probe picked %s (tried %s)",
                    label, ", ".join(tried),
                )
                self._resolved_pair = (exchange, code)
                return df, exchange
        if last_exc is not None:
            raise last_exc
        raise DataSourceTransientError(
            f"TradingView 自动探测失败（{symbol}）：已尝试 {', '.join(tried)} 均无 K 线"
        )

    def latest_snapshot(self, n: int) -> list[KlineBar]:
        with self._snapshot_lock:
            return self._latest_snapshot_inner(n)

    def _latest_snapshot_inner(self, n: int) -> list[KlineBar]:
        if self._tv is None:
            raise DataSourceTransientError("TradingView 未连接，请先选择数据来源 TradingView")
        if not self._symbol or not self._timeframe:
            raise DataSourceTransientError("TradingView 未订阅品种/周期")

        user_symbol = self._symbol
        req_exchange = self._exchange
        exchange = req_exchange or ""
        fetch_symbol = user_symbol
        auto_probe = is_tv_exchange_auto(req_exchange)
        probe_plan = tv_auto_probe_plan(user_symbol) if auto_probe else []
        try:
            from tvDatafeed import Interval
            interval = getattr(Interval, _TF_MAP[self._timeframe])
            if auto_probe and probe_plan:
                df, exchange = self._fetch_tv_auto_probe(
                    symbol=user_symbol,
                    plan=probe_plan,
                    interval=interval,
                    n_bars=n + 1,
                    adjustment=self._adjust,
                )
            else:
                try:
                    exchange, fetch_symbol = resolve_tv_fetch_pair(
                        req_exchange, user_symbol
                    )
                except TvSymbolNotFoundError as exc:
                    raise DataSourceTransientError(str(exc)) from exc
                df = self._fetch_hist_with_retry(
                    symbol=fetch_symbol,
                    exchange=exchange,
                    interval=interval,
                    n_bars=n + 1,
                    adjustment=self._adjust,
                )
                if df is not None and not df.empty:
                    self._resolved_pair = (exchange, fetch_symbol)
        except DataSourceTransientError:
            raise
        except Exception as exc:
            msg = format_tradingview_fetch_error(
                user_symbol, exchange or req_exchange or "自动", cause=exc,
            )
            logger.warning("TradingView fetch failed: %s", exc)
            raise DataSourceTransientError(msg) from exc

        if df is None or df.empty:
            msg = format_tradingview_fetch_error(
                user_symbol, exchange or req_exchange or "自动", empty_data=True,
            )
            logger.debug(
                "TradingView empty data for %s exchange=%s",
                user_symbol, exchange or req_exchange or "(auto)",
            )
            raise DataSourceTransientError(msg)

        bars = self._df_to_bars(df.reset_index(), n)

        bars.reverse()
        for i, b in enumerate(bars):
            if b.seq != i + 1:
                bars[i] = KlineBar(
                    seq=i + 1, ts_open=b.ts_open, open=b.open,
                    high=b.high, low=b.low, close=b.close,
                    volume=b.volume, closed=b.closed,
                )

        return bars

    def fetch_range(self, start_date: str, end_date: str) -> tuple[list[KlineBar], str | None]:
        with self._snapshot_lock:
            return self._fetch_range_inner(start_date, end_date)

    def _fetch_range_inner(self, start_date: str, end_date: str) -> tuple[list[KlineBar], str | None]:
        if self._tv is None:
            raise DataSourceTransientError("TradingView 未连接，请先选择数据来源 TradingView")
        if not self._symbol or not self._timeframe:
            raise DataSourceTransientError("TradingView 未订阅品种/周期")

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        if end_dt < start_dt:
            raise DataSourceTransientError("end_date 不能早于 start_date")

        days = (datetime.now() - start_dt).days + 1
        bpd = _BARS_PER_DAY.get(self._timeframe, 1)
        n_bars = int(days * bpd * 1.2) + 50
        warning = None
        if n_bars > 5000:
            warning = (
                f"日期范围需 {n_bars} 根 K 线，超过 TradingView 单次上限 5000，"
                "已截断为最近 5000 根"
            )
            n_bars = 5000

        user_symbol = self._symbol
        req_exchange = self._exchange
        exchange = req_exchange or ""
        fetch_symbol = user_symbol
        auto_probe = is_tv_exchange_auto(req_exchange)
        probe_plan = tv_auto_probe_plan(user_symbol) if auto_probe else []
        try:
            from tvDatafeed import Interval
            interval = getattr(Interval, _TF_MAP[self._timeframe])
            if auto_probe and probe_plan:
                df, exchange = self._fetch_tv_auto_probe(
                    symbol=user_symbol,
                    plan=probe_plan,
                    interval=interval,
                    n_bars=n_bars,
                    adjustment=self._adjust,
                )
            else:
                try:
                    exchange, fetch_symbol = resolve_tv_fetch_pair(
                        req_exchange, user_symbol
                    )
                except TvSymbolNotFoundError as exc:
                    raise DataSourceTransientError(str(exc)) from exc
                df = self._fetch_hist_with_retry(
                    symbol=fetch_symbol,
                    exchange=exchange,
                    interval=interval,
                    n_bars=n_bars,
                    adjustment=self._adjust,
                )
        except DataSourceTransientError:
            raise
        except Exception as exc:
            msg = format_tradingview_fetch_error(
                user_symbol, exchange or req_exchange or "自动", cause=exc,
            )
            logger.warning("TradingView fetch failed: %s", exc)
            raise DataSourceTransientError(msg) from exc

        if df is None or df.empty:
            msg = format_tradingview_fetch_error(
                user_symbol, exchange or req_exchange or "自动", empty_data=True,
            )
            raise DataSourceTransientError(msg)

        df = df.reset_index()

        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int((end_dt + timedelta(days=1)).timestamp() * 1000)

        filtered_rows = []
        for row in df.itertuples(index=False):
            ts_ms = _row_ts_ms(row)
            if start_ts <= ts_ms < end_ts:
                filtered_rows.append(row)

        filtered_rows.reverse()

        bars: list[KlineBar] = []
        for i, row in enumerate(filtered_rows):
            ts_ms = _row_ts_ms(row)
            bar = KlineBar(
                seq=i + 1,
                ts_open=ts_ms,
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(getattr(row, "volume", 0.0)),
                closed=True,
            )
            bars.append(normalize_kline_bar(bar))

        if bars:
            from data.tradingview.bar_close_wait import seconds_until_bar_closes
            last = bars[-1]
            secs_left = seconds_until_bar_closes(last.ts_open, self._timeframe, now_ms=None)
            still_forming = secs_left is not None and secs_left > 0
            if still_forming:
                bars[-1] = KlineBar(
                    seq=last.seq,
                    ts_open=last.ts_open,
                    open=last.open,
                    high=last.high,
                    low=last.low,
                    close=last.close,
                    volume=last.volume,
                    closed=False,
                )

        bars.reverse()
        for i, b in enumerate(bars):
            if b.seq != i + 1:
                bars[i] = KlineBar(
                    seq=i + 1, ts_open=b.ts_open, open=b.open,
                    high=b.high, low=b.low, close=b.close,
                    volume=b.volume, closed=b.closed,
                )

        return bars, warning

    def _df_to_bars(self, df, n: int) -> list[KlineBar]:
        rows = list(df.itertuples(index=False))
        rows.reverse()

        bars: list[KlineBar] = []
        for i, row in enumerate(rows):
            ts_ms = _row_ts_ms(row)
            bar = KlineBar(
                seq=i + 1,
                ts_open=ts_ms,
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(getattr(row, "volume", 0.0)),
                closed=True,
            )
            if i == len(rows) - 1:
                from data.tradingview.bar_close_wait import seconds_until_bar_closes
                secs_left = seconds_until_bar_closes(ts_ms, self._timeframe, now_ms=None)
                still_forming = secs_left is not None and secs_left > 0
                bar = KlineBar(
                    seq=bar.seq,
                    ts_open=bar.ts_open,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    closed=not still_forming,
                )
            bars.append(normalize_kline_bar(bar))
            if len(bars) >= n:
                break

        return bars


def _row_ts_ms(row) -> int:
    return datetime_to_ts_ms(getattr(row, "datetime", None))

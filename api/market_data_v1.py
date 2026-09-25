"""Baostock / TradingView V1 行情协议路由与字段转换。"""

from datetime import datetime, timedelta, timezone
from typing import Literal, Optional
from uuid import uuid4
import time
from zoneinfo import ZoneInfo

import baostock as bs
from fastapi import APIRouter, Body, Path
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError

from stock_service import get_stock_k_data
from stock_directory import StockDirectoryError, search_stocks
from data.tradingview.source import TradingViewSource
from data.tradingview.market_defaults import tv_auto_probe_plan
from data.tradingview.symbol_lookup import lookup_tv_symbol_by_name
from data.datetime_ts import epoch_to_date_str
from data.finshare.source import fetch_bars as fetch_finshare_bars
from data.finshare.source import fetch_snapshot as fetch_finshare_snapshot
from data.finshare.source import search as search_finshare
from data.finshare.source import source_capabilities as finshare_capabilities

router = APIRouter()
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

Period = Literal["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"]
Adjustment = Literal["qfq", "hfq", "none", "splits"]

# 允许的 V1 数据源 ID
_SUPPORTED_SOURCES = frozenset({"baostock", "tradingview", "finshare"})

# TradingView 周期/复权到 tvDatafeed 的映射
_TV_PERIODS = ["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"]
_TV_PERIOD_TO_TF = {
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
    "30min": "30m",
    "60min": "1h",
    "daily": "1d",
    "weekly": "1w",
    "monthly": "1M",
}
_TV_ADJUST_TO_TF = {"qfq": "dividends", "splits": "splits", "none": "none"}
_TV_SOURCE_CAPABILITIES = {
    "assetClasses": ["stock", "index", "forex", "crypto", "unknown"],
    "bars": {
        "periods": _TV_PERIODS,
        "adjustments": ["qfq", "splits", "none"],
    },
}
_MAX_BAR_LIMIT = 5000
_TV_BARS_PER_DAY = {
    "1min": 390,
    "5min": 78,
    "15min": 26,
    "30min": 13,
    "60min": 6.5,
    "daily": 1,
    "weekly": 1 / 7,
    "monthly": 1 / 30,
}

# 会话/币种/时区按品种类型推断
_TV_SESSION_TZ = {
    "CN": "Asia/Shanghai",
    "HK": "Asia/Hong_Kong",
    "US": "America/New_York",
}


class InstrumentReference(BaseModel):
    """V1 请求中的品种引用。"""

    id: str
    symbol: str
    exchange: str
    providerRef: dict[str, str | int | float | bool] | None = None


class InstrumentSearchRequest(BaseModel):
    """V1 品种搜索请求。"""

    sourceId: str = Field(min_length=1)
    keyword: str = Field(min_length=1, max_length=128)
    limit: int = Field(ge=1, le=100)
    assetClasses: list[str] | None = None


class BarRequest(BaseModel):
    """V1 K 线查询请求。"""

    sourceId: str = Field(min_length=1)
    instrument: InstrumentReference
    period: Period
    adjustment: Adjustment
    limit: int = Field(ge=1, le=_MAX_BAR_LIMIT)
    before: int | None = Field(default=None, ge=0)

    model_config = {"extra": "forbid"}


class SnapshotRequest(BaseModel):
    """V1 期货实时快照请求。"""

    sourceId: str = Field(min_length=1)
    instrument: InstrumentReference


def _request_id() -> str:
    """生成请求追踪 ID。"""
    return str(uuid4())


def _success(data: object) -> dict[str, object]:
    """包装 V1 成功响应。"""
    return {"data": data, "requestId": _request_id()}


def _failure(status: int, code: str, message: str, details: Optional[dict] = None) -> JSONResponse:
    """构造 V1 标准错误响应。"""
    error: dict[str, object] = {"code": code, "message": message}
    if details:
        error["details"] = details
    return JSONResponse(status_code=status, content={"error": error, "requestId": _request_id()})


def _source_error(source_id: str) -> Optional[JSONResponse]:
    """校验请求是否属于受支持的数据源。"""
    if source_id not in _SUPPORTED_SOURCES:
        return _failure(
            400, "INVALID_REQUEST", f"sourceId must be one of {sorted(_SUPPORTED_SOURCES)}"
        )
    return None


def _date_from_ms(value: int) -> str:
    """将 UTC 毫秒时间戳转换为上海时区日期。"""
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).astimezone(SHANGHAI_TZ).date().isoformat()


def _bar_window_start(before: int | None, limit: int, period: str) -> str:
    """按周期估算满足分页请求所需的日期窗口起点。"""
    if period in {"daily", "weekly", "monthly"}:
        # 日期型 SDK 没有 latest/offset 参数；读取完整日线历史才能覆盖到期或退市品种。
        return "1990-01-01"
    end = (
        datetime.fromtimestamp(before / 1000, tz=timezone.utc).astimezone(SHANGHAI_TZ)
        if before is not None
        else datetime.now(SHANGHAI_TZ)
    )
    days_per_bar = {
        "daily": 3,
        "weekly": 21,
        "monthly": 92,
        "5min": 1 / 16,
        "15min": 1 / 5,
        "30min": 1 / 2,
        "60min": 1,
    }[period]
    return (end - timedelta(days=int(limit * days_per_bar) + 30)).date().isoformat()


def _bar_window_end(before: int | None) -> str:
    """返回 SDK 日期范围的结束日期，实际游标排除在结果筛选时处理。"""
    if before is None:
        return datetime.now(SHANGHAI_TZ).date().isoformat()
    return _date_from_ms(before)


def _paginate_bars(items: list[dict], limit: int, before: int | None) -> list[dict]:
    """排除游标及其后的 K 线，返回时间正序的最近一页。"""
    eligible = [item for item in items if before is None or item["timestamp"] < before]
    eligible.sort(key=lambda item: item["timestamp"])
    return eligible[-limit:]


def _tv_fetch_count(request: BarRequest) -> int:
    """按 TradingView 的仅 latest-n-bars 接口估算回溯所需根数。"""
    if request.before is None:
        return request.limit
    cursor = datetime.fromtimestamp(request.before / 1000, tz=timezone.utc)
    elapsed_days = max(0, (datetime.now(timezone.utc) - cursor).total_seconds() / 86400)
    estimated = int(elapsed_days * _TV_BARS_PER_DAY[request.period] * 1.2) + request.limit + 2
    return min(_MAX_BAR_LIMIT, max(request.limit, estimated))


# ---------- Baostock ----------

# Baostock 源级与品种级共用能力声明，与 _fetch_baostock_bars 的周期/复权实现保持一致
_BAOSTOCK_BAR_PERIODS = ["5min", "15min", "30min", "60min", "daily", "weekly", "monthly"]
_BAOSTOCK_BAR_ADJUSTMENTS = ["qfq", "hfq", "none"]
_BAOSTOCK_SOURCE_CAPABILITIES = {
    "assetClasses": ["stock"],
    "bars": {
        "periods": _BAOSTOCK_BAR_PERIODS,
        "adjustments": _BAOSTOCK_BAR_ADJUSTMENTS,
    },
}


def _instrument(code: str, name: str) -> dict[str, object]:
    """将 Baostock 股票记录转换为 V1 品种描述。"""
    market, symbol = code.split(".", 1) if "." in code else ("", code)
    exchange = {"sh": "SH", "sz": "SZ", "bj": "BJ"}.get(market.lower(), market.upper())
    return {
        "id": f"baostock:stock:{market.lower()}:{symbol}",
        "sourceId": "baostock",
        "symbol": code,
        "name": name or symbol,
        "assetClass": "stock",
        "exchange": exchange,
        "sessionId": "CN",
        "currency": "CNY",
        "providerRef": {"stockCode": code},
        "capabilities": {
            "bars": {
                "periods": _BAOSTOCK_BAR_PERIODS,
                "adjustments": _BAOSTOCK_BAR_ADJUSTMENTS,
            }
        },
    }


def _timestamp_ms(item: dict[str, object]) -> int:
    """将 Baostock 日期或分钟时间转换为 UTC 毫秒时间戳。"""
    raw_time = str(item.get("time") or "")
    digits = "".join(char for char in raw_time if char.isdigit())
    if len(digits) >= 14:
        local = datetime.strptime(digits[:14], "%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI_TZ)
    else:
        local = datetime.strptime(str(item["date"])[:10], "%Y-%m-%d").replace(tzinfo=SHANGHAI_TZ)
    return int(local.timestamp() * 1000)


def _number(value: object) -> Optional[float]:
    """将 Baostock 空字符串或数字转换为可选浮点数。"""
    if value is None or value == "":
        return None
    return float(value)


def _probe_baostock() -> dict:
    """探测 Baostock 登录与上游可用性，并声明股票 K 线能力。"""
    started = time.perf_counter()
    login = None
    try:
        login = bs.login()
        online = login.error_code == "0"
        data = {
            "status": "online" if online else "offline",
            "checkedAt": int(datetime.now(timezone.utc).timestamp() * 1000),
            "latencyMs": round((time.perf_counter() - started) * 1000, 2),
            "capabilities": _BAOSTOCK_SOURCE_CAPABILITIES,
        }
        if not online:
            data["message"] = login.error_msg
        return data
    except Exception as exc:
        return {
            "status": "offline",
            "checkedAt": int(datetime.now(timezone.utc).timestamp() * 1000),
            "latencyMs": round((time.perf_counter() - started) * 1000, 2),
            "message": str(exc),
            "capabilities": _BAOSTOCK_SOURCE_CAPABILITIES,
        }
    finally:
        if login is not None and login.error_code == "0":
            bs.logout()


def _search_baostock(request: InstrumentSearchRequest) -> dict:
    """搜索本地股票目录快照并转换为 V1 品种描述。"""
    if request.assetClasses is not None and "stock" not in request.assetClasses:
        return {"items": []}
    try:
        entries = search_stocks(request.keyword, request.limit)
    except StockDirectoryError as exc:
        raise _UpstreamError(str(exc)) from exc
    return {"items": [_instrument(entry.code, entry.code_name) for entry in entries]}


def _fetch_baostock_bars(request: BarRequest) -> dict:
    """按 V1 请求读取 Baostock 历史 K 线。"""
    if request.adjustment == "splits":
        raise _CapabilityError("Baostock does not support splits adjustment")
    period_map = {"daily": "d", "weekly": "w", "monthly": "m", "5min": "5", "15min": "15", "30min": "30", "60min": "60"}
    if request.period not in period_map:
        raise _CapabilityError(f"Baostock does not support {request.period}")
    stock_code = str((request.instrument.providerRef or {}).get("stockCode") or request.instrument.symbol)
    result = get_stock_k_data(
        stock_code=stock_code,
        start_date=_bar_window_start(request.before, request.limit, request.period),
        end_date=_bar_window_end(request.before),
        frequency=period_map[request.period],
        adjustflag={"qfq": "2", "hfq": "1", "none": "3"}[request.adjustment],
    )
    if not result.get("success"):
        raise _UpstreamError(result.get("error_msg", "Baostock query failed"))
    items = []
    for row in result.get("data", []):
        timestamp = _timestamp_ms(row)
        items.append({
            "timestamp": timestamp,
            "date": str(row.get("date", "")),
            "open": _number(row.get("open")),
            "high": _number(row.get("high")),
            "low": _number(row.get("low")),
            "close": _number(row.get("close")),
            "volume": _number(row.get("volume")),
            "turnover": _number(row.get("amount")),
            "changePercent": _number(row.get("pctChg")),
            "turnoverRate": _number(row.get("turn")),
        })
    items = _paginate_bars(items, request.limit, request.before)
    return {
        "instrumentId": request.instrument.id,
        "period": request.period,
        "adjustment": request.adjustment,
        "timezone": "Asia/Shanghai",
        "volumeUnit": "share",
        "items": items,
    }


# ---------- finshare 期货 ----------

def _probe_finshare() -> dict:
    """探测 finshare 是否可导入，并声明期货数据能力。"""
    started = time.perf_counter()
    try:
        import finshare  # noqa: F401

        return {
            "status": "online",
            "checkedAt": int(datetime.now(timezone.utc).timestamp() * 1000),
            "latencyMs": round((time.perf_counter() - started) * 1000, 2),
            "capabilities": finshare_capabilities(),
        }
    except Exception as exc:
        return {
            "status": "offline",
            "checkedAt": int(datetime.now(timezone.utc).timestamp() * 1000),
            "latencyMs": round((time.perf_counter() - started) * 1000, 2),
            "message": str(exc),
            "capabilities": finshare_capabilities(),
        }


def _search_finshare(request: InstrumentSearchRequest) -> dict:
    """搜索 finshare 期货代码并转换为 V1 品种描述。"""
    if request.assetClasses is not None and "future" not in request.assetClasses:
        return {"items": []}
    return {"items": search_finshare(request.keyword, request.limit)}


def _fetch_finshare_bars(request: BarRequest) -> dict:
    """按 V1 请求读取 finshare 期货日线。"""
    if request.period != "daily" or request.adjustment != "none":
        raise _CapabilityError("finshare supports daily bars with none adjustment only")
    code = str((request.instrument.providerRef or {}).get("futureCode") or request.instrument.symbol)
    try:
        items = fetch_finshare_bars(
            code,
            _bar_window_start(request.before, request.limit, request.period),
            _bar_window_end(request.before),
        )
    except Exception as exc:
        raise _UpstreamError(str(exc)) from exc
    items = _paginate_bars(items, request.limit, request.before)
    return {
        "instrumentId": request.instrument.id,
        "period": request.period,
        "adjustment": request.adjustment,
        "timezone": "Asia/Shanghai",
        "volumeUnit": "contract",
        "items": items,
    }


def _fetch_finshare_snapshot(request: SnapshotRequest) -> dict:
    """按 V1 请求读取 finshare 期货实时快照。"""
    code = str((request.instrument.providerRef or {}).get("futureCode") or request.instrument.symbol)
    try:
        return {"instrumentId": request.instrument.id, **fetch_finshare_snapshot(code)}
    except Exception as exc:
        raise _UpstreamError(str(exc)) from exc


# ---------- TradingView ----------

def _tv_session(symbol: str, exchange: str) -> str:
    """按品种/交易所推断交易时段标识。"""
    from data.tradingview.market_defaults import (
        is_ashare_tv_request,
        is_hk_tv_request,
    )

    if is_ashare_tv_request(exchange, symbol):
        return "CN"
    if is_hk_tv_request(exchange, symbol):
        return "HK"
    return "US"


def _tv_asset_class(symbol: str, exchange: str) -> str:
    """按品种/交易所推断资产类别。"""
    from data.tradingview.market_defaults import (
        _KNOWN_INDEX_TICKERS,
        is_ashare_tv_request,
        is_hk_tv_request,
        is_likely_crypto_symbol,
    )

    upper = symbol.upper()
    if upper in _KNOWN_INDEX_TICKERS:
        return "index"
    if upper in ("XAUUSD", "GOLD", "XAGUSD") or "/" in upper:
        return "forex"
    if is_likely_crypto_symbol(upper):
        return "crypto"
    if is_ashare_tv_request(exchange, symbol) or is_hk_tv_request(exchange, symbol):
        return "stock"
    return "unknown"


def _tv_instrument(symbol: str, exchange: str = "") -> dict[str, object]:
    """将 TradingView 代码转换为 V1 品种描述。"""
    sym = (symbol or "").strip()
    ex = (exchange or "").strip().upper()
    session = _tv_session(sym, ex)
    currency = {"CN": "CNY", "HK": "HKD"}.get(session, "USD")
    return {
        "id": f"tradingview:{ex or 'auto'}:{sym}",
        "sourceId": "tradingview",
        "symbol": sym,
        "name": sym,
        "assetClass": _tv_asset_class(sym, ex),
        "exchange": ex or "AUTO",
        "sessionId": session,
        "currency": currency,
        "providerRef": {"exchange": ex, "symbol": sym},
        "capabilities": {
            "bars": {
                "periods": _TV_PERIODS,
                "adjustments": ["qfq", "splits", "none"],
            }
        },
    }


def _probe_tradingview() -> dict:
    """探测 tvDatafeed 是否可导入（TradingView 无本地登录状态）。"""
    started = time.perf_counter()
    try:
        import tvDatafeed  # noqa: F401

        return {
            "status": "online",
            "checkedAt": int(datetime.now(timezone.utc).timestamp() * 1000),
            "latencyMs": round((time.perf_counter() - started) * 1000, 2),
            "capabilities": _TV_SOURCE_CAPABILITIES,
        }
    except Exception as exc:
        return {
            "status": "offline",
            "checkedAt": int(datetime.now(timezone.utc).timestamp() * 1000),
            "latencyMs": round((time.perf_counter() - started) * 1000, 2),
            "message": str(exc),
        }


def _search_tradingview(request: InstrumentSearchRequest) -> dict:
    """按名称映射或自动探测推断 TradingView 品种。"""
    keyword = request.keyword.strip()
    items = []
    seen = set()

    def add(symbol: str, exchange: str) -> None:
        key = f"{exchange}:{symbol}"
        if key in seen:
            return
        seen.add(key)
        items.append(_tv_instrument(symbol, exchange))

    hit = lookup_tv_symbol_by_name(keyword)
    if hit is not None:
        add(hit[1], hit[0])
    for ex, sym in tv_auto_probe_plan(keyword):
        add(sym, ex)
    if not items:
        add(keyword, "")
    if request.assetClasses:
        allowed = set(request.assetClasses)
        items = [item for item in items if item.get("assetClass") in allowed]
    return {"items": items[: request.limit]}


def _fetch_tradingview_bars(request: BarRequest) -> dict:
    """按 V1 请求通过 TradingView 拉取历史 K 线。"""
    if request.adjustment == "hfq":
        raise _CapabilityError("TradingView does not support hfq adjustment")
    timeframe = _TV_PERIOD_TO_TF.get(request.period)
    if timeframe is None:
        raise _CapabilityError(f"TradingView does not support {request.period}")
    adjust = _TV_ADJUST_TO_TF.get(request.adjustment, "none")
    ref = request.instrument.providerRef or {}
    exchange = str(ref.get("exchange") or request.instrument.exchange or "")
    symbol = str(ref.get("symbol") or request.instrument.symbol)
    try:
        src = TradingViewSource(adjust=adjust)
        src.connect()
        try:
            src.set_exchange(exchange)
            src.subscribe(symbol, timeframe)
            bars = src.latest_snapshot(_tv_fetch_count(request))
        finally:
            src.disconnect()
    except Exception as exc:
        raise _UpstreamError(str(exc))
    session = _tv_session(symbol, exchange)
    items = []
    for bar in bars:
        ts_ms = int(bar.ts_open)
        items.append({
            "timestamp": ts_ms,
            "date": epoch_to_date_str(bar.ts_open),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
        })
    items = _paginate_bars(items, request.limit, request.before)
    return {
        "instrumentId": request.instrument.id,
        "period": request.period,
        "adjustment": request.adjustment,
        "timezone": _TV_SESSION_TZ.get(session, "UTC"),
        "items": items,
    }


# ---------- 统一异常到错误响应 ----------

class _RequestError(Exception):
    """请求不合法。"""

    code = "INVALID_REQUEST"


class _CapabilityError(Exception):
    """请求能力不受支持。"""

    code = "UNSUPPORTED_CAPABILITY"


class _UpstreamError(Exception):
    """上游查询失败。"""

    code = "UPSTREAM_UNAVAILABLE"


def _dispatch(func, *args, **kwargs) -> dict | JSONResponse:
    """执行数据源处理函数，把领域异常转为 V1 错误响应。"""
    try:
        return _success(func(*args, **kwargs))
    except _RequestError as exc:
        return _failure(400, exc.code, str(exc))
    except _CapabilityError as exc:
        return _failure(400, exc.code, str(exc))
    except _UpstreamError as exc:
        return _failure(502, exc.code, str(exc))


_HANDLERS = {
    "probe": {"baostock": _probe_baostock, "tradingview": _probe_tradingview, "finshare": _probe_finshare},
    "search": {"baostock": _search_baostock, "tradingview": _search_tradingview, "finshare": _search_finshare},
    "bars": {"baostock": _fetch_baostock_bars, "tradingview": _fetch_tradingview_bars, "finshare": _fetch_finshare_bars},
}


@router.get("/api/v1/market-data/sources/{sourceId}/probe", response_model=None)
def probe_market_data_source(source_id: str = Path(alias="sourceId")) -> dict | JSONResponse:
    """探测数据源可用性。"""
    error = _source_error(source_id)
    if error:
        return error
    return _dispatch(_HANDLERS["probe"][source_id])


@router.post("/api/v1/market-data/instruments/search", response_model=None)
def search_instruments(request: InstrumentSearchRequest = Body(...)) -> dict | JSONResponse:
    """搜索数据源标准品种目录。"""
    error = _source_error(request.sourceId)
    if error:
        return error
    return _dispatch(_HANDLERS["search"][request.sourceId], request)


@router.post("/api/v1/market-data/bars", response_model=None)
def fetch_bars(request: dict = Body(...)) -> dict | JSONResponse:
    """按 V1 请求读取数据源历史 K 线。"""
    try:
        parsed_request = BarRequest.model_validate(request)
    except ValidationError as exc:
        return _failure(400, "INVALID_REQUEST", "Invalid bars request", {"errors": exc.errors()})
    error = _source_error(parsed_request.sourceId)
    if error:
        return error
    return _dispatch(_HANDLERS["bars"][parsed_request.sourceId], parsed_request)


@router.post("/api/v1/market-data/timeshare")
def fetch_time_share(request: dict = Body(...)) -> JSONResponse:
    """声明当前数据源均未提供 V1 分时能力。"""
    source_id = str(request.get("sourceId", ""))
    error = _source_error(source_id)
    if error:
        return error
    return _failure(400, "UNSUPPORTED_CAPABILITY", f"{source_id} does not support timeshare")


@router.post("/api/v1/market-data/snapshot", response_model=None)
def fetch_snapshot(request: SnapshotRequest = Body(...)) -> dict | JSONResponse:
    """读取数据源实时快照；当前由 finshare 提供期货快照。"""
    error = _source_error(request.sourceId)
    if error:
        return error
    if request.sourceId != "finshare":
        return _failure(400, "UNSUPPORTED_CAPABILITY", f"{request.sourceId} does not support snapshot")
    return _dispatch(_fetch_finshare_snapshot, request)

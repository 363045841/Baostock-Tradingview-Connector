"""将 finshare 期货 SDK 转换为 V1 可消费的标准数据。"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from data.datetime_ts import epoch_to_date_str

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def _value(item: Any, *names: str, default: Any = None) -> Any:
    """从 SDK 记录或字典读取第一个存在的字段。"""
    for name in names:
        if isinstance(item, dict) and name in item:
            return item[name]
        value = getattr(item, name, default)
        if value is not default:
            return value
    return default


def _number(value: Any) -> float | None:
    """将 SDK 数值转换为可选浮点数。"""
    if value is None or value == "":
        return None
    return float(value)


def _timestamp_ms(item: Any) -> int:
    """将 finshare 日期字段转换为 UTC 毫秒时间戳。"""
    raw = _value(item, "timestamp", "datetime", "trade_date", "date", "time")
    if isinstance(raw, (int, float)):
        return int(raw if raw > 10_000_000_000 else raw * 1000)
    if isinstance(raw, datetime):
        parsed = raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    elif isinstance(raw, date):
        parsed = datetime.combine(raw, datetime.min.time(), tzinfo=SHANGHAI_TZ)
    else:
        parsed = datetime.fromisoformat(str(raw).replace("/", "-").replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _import_sdk():
    """延迟导入 finshare，保证其他数据源可独立启动。"""
    import finshare

    return finshare


def source_capabilities() -> dict[str, Any]:
    """返回 finshare 支持的期货源级能力声明。"""
    return {
        "instrumentTypes": ["future"],
        "bars": {"periods": ["daily"], "adjustments": ["none"]},
    }


def _exchange_for_symbol(symbol: str) -> str:
    """根据期货品种前缀推断交易所。"""
    prefix = "".join(char for char in symbol if char.isalpha())
    mapping = {
        "IF": "CFFEX", "IH": "CFFEX", "IC": "CFFEX", "IM": "CFFEX",
        "CU": "SHFE", "AL": "SHFE", "AU": "SHFE", "AG": "SHFE", "RB": "SHFE",
        "M": "DCE", "Y": "DCE", "P": "DCE", "I": "DCE", "J": "DCE",
        "SR": "CZCE", "CF": "CZCE", "TA": "CZCE", "MA": "CZCE", "SA": "CZCE",
        "SC": "INE",
    }
    return mapping.get(prefix, "UNKNOWN")


def _instrument(symbol: str, exchange: str) -> dict[str, Any]:
    """构造 finshare 期货的标准品种描述。"""
    return {
        "id": f"finshare:future:{exchange.lower()}:{symbol.lower()}",
        "sourceId": "finshare",
        "symbol": symbol,
        "name": symbol,
        "instrumentType": "future",
        "exchange": exchange,
        "sessionId": "CN",
        "currency": "CNY",
        "providerRef": {"futureCode": symbol},
        "capabilities": {
            "bars": {"periods": ["daily"], "adjustments": ["none"]},
            "snapshot": True,
        },
    }


def search(keyword: str, limit: int) -> list[dict[str, Any]]:
    """根据期货代码生成标准品种描述。"""
    symbol = keyword.strip().upper()
    return [_instrument(symbol, _exchange_for_symbol(symbol))][:limit] if symbol else []


def fetch_bars(code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    """调用 finshare 获取期货日线并转换为 V1 K 线字段。"""
    records = _import_sdk().get_future_kline(code, start_date, end_date, adjustment="none")
    items = []
    for record in records or []:
        timestamp = _timestamp_ms(record)
        items.append({
            "timestamp": timestamp,
            "date": epoch_to_date_str(timestamp),
            "open": _number(_value(record, "open_price", "open")),
            "high": _number(_value(record, "high_price", "high")),
            "low": _number(_value(record, "low_price", "low")),
            "close": _number(_value(record, "close_price", "close")),
            "volume": _number(_value(record, "volume", "vol")),
            "openInterest": _number(_value(record, "open_interest", "openInterest")),
        })
    return items


def fetch_snapshot(code: str) -> dict[str, Any]:
    """调用 finshare 获取期货实时快照并转换为 V1 字段。"""
    snapshot = _import_sdk().get_future_snapshot(code)
    fields = {
        "lastPrice": ("last_price", "lastPrice"), "open": ("day_open", "open"),
        "high": ("day_high", "high"), "low": ("day_low", "low"),
        "preClose": ("prev_close", "pre_close", "preClose"),
        "volume": ("volume", "vol"), "openInterest": ("open_interest", "openInterest"),
        "bidPrice": ("bid1_price", "bid_price", "bid_price1", "bidPrice"),
        "askPrice": ("ask1_price", "ask_price", "ask_price1", "askPrice"),
        "bidVolume": ("bid1_volume", "bid_volume", "bid_volume1", "bidVolume"),
        "askVolume": ("ask1_volume", "ask_volume", "ask_volume1", "askVolume"),
    }
    result = {key: _number(_value(snapshot, *names)) for key, names in fields.items()}
    result["symbol"] = code
    return result

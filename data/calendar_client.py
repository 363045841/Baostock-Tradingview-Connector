"""将 Baostock 沪深日线日历请求转发到独立 KCQ Calendar 服务。"""

import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


CALENDAR_URL_ENV = "KCQ_CALENDAR_URL"
CALENDAR_PATH = "/api/v1/market-data/trading-calendar"
CALENDAR_TIMEOUT_SECONDS = 5
_MAX_RESPONSE_BYTES = 1024 * 1024
_BAOSTOCK_MARKETS = {"SH": "SSE", "SZ": "SZSE"}


class CalendarError(Exception):
    """携带可转换成 V1 envelope 的日历服务错误。"""

    def __init__(self, code: str, message: str, status: int = 502):
        super().__init__(message)
        self.code = code
        self.status = status


def calendar_enabled() -> bool:
    """仅在显式配置日历服务地址时声明转发能力。"""
    return bool(os.environ.get(CALENDAR_URL_ENV, "").strip())


def supports_calendar(exchange: str) -> bool:
    """当前只保证 Baostock 沪深市场的日线日期口径。"""
    return calendar_enabled() and exchange in _BAOSTOCK_MARKETS


def _read_json(response) -> dict:
    """读取有大小上限的 JSON envelope，异常由调用层统一转换。"""
    raw = response.read(_MAX_RESPONSE_BYTES + 1)
    if len(raw) > _MAX_RESPONSE_BYTES:
        raise ValueError("calendar response exceeds size limit")
    body = json.loads(raw)
    if not isinstance(body, dict):
        raise ValueError("calendar response must be an object")
    return body


def fetch_calendar(payload: dict) -> dict:
    """保留行情标识，固定 Baostock 的市场与时区口径；失败不推测日期。"""
    exchange = payload["instrument"]["exchange"]
    if (
        payload["sourceId"] != "baostock"
        or not supports_calendar(exchange)
        or payload["period"] != "daily"
        or payload["barAggregation"] != "original"
    ):
        raise CalendarError(
            "UNSUPPORTED_CAPABILITY",
            "Calendar requires KCQ_CALENDAR_URL and Baostock SH/SZ daily/original",
            400,
        )
    # 这里必须与 _timestamp_ms 的上海当地日期标签一致，不接受调用方覆盖成其他市场。
    instrument = dict(payload["instrument"])
    instrument["providerRef"] = {
        **(instrument.get("providerRef") or {}),
        "calendarExchange": _BAOSTOCK_MARKETS[exchange],
        "calendarTimeZone": "Asia/Shanghai",
    }
    forwarded = {**payload, "instrument": instrument}
    base = os.environ[CALENDAR_URL_ENV].strip().rstrip("/")
    try:
        parsed = urlsplit(base)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError("KCQ_CALENDAR_URL must be an HTTP(S) base URL")
        request = Request(
            base + CALENDAR_PATH,
            data=json.dumps(forwarded).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=CALENDAR_TIMEOUT_SECONDS) as response:
                body = _read_json(response)
        except HTTPError as exc:
            with exc:
                body = _read_json(exc)
            error = body.get("error", {})
            if isinstance(error, dict) and exc.code in {400, 422} and error.get("code") in {
                "INVALID_REQUEST", "UNSUPPORTED_CAPABILITY"
            } and isinstance(error.get("message"), str):
                raise CalendarError(error["code"], error["message"], 400) from exc
            raise CalendarError("UPSTREAM_UNAVAILABLE", f"Calendar service returned HTTP {exc.code}") from exc
        result = body.get("data")
        if not isinstance(result, dict) or result.get("anchorTimestamp") != payload["anchorTimestamp"]:
            raise ValueError("calendar response has an invalid anchor")
        timestamps = result.get("futureTimestamps")
        if not isinstance(timestamps, list) or len(timestamps) > payload["count"]:
            raise ValueError("calendar response has invalid timestamps")
        previous = payload["anchorTimestamp"]
        for stamp in timestamps:
            if type(stamp) is not int or stamp <= previous:
                raise ValueError("calendar timestamps must be strictly increasing")
            previous = stamp
        # 覆盖末尾允许返回不足 count 的前缀，遵循 KCQ V1 原有约定。
        return {"anchorTimestamp": result["anchorTimestamp"], "futureTimestamps": timestamps}
    except (URLError, OSError, ValueError) as exc:
        raise CalendarError("UPSTREAM_UNAVAILABLE", f"Calendar service unavailable: {exc}") from exc

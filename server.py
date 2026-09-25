import asyncio
import time
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from stock_service import get_stock_k_data, query_all_stock
from stock_directory import StockDirectoryError, list_stocks, warm_up
from data.tradingview.source import TradingViewSource
from data.datetime_ts import ts_open_to_ms, epoch_to_date_str
from api.market_data_v1 import router as market_data_v1_router

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """启动时预热股票目录；失败不阻断服务，首个请求再报错。"""
    try:
        await asyncio.to_thread(warm_up)
    except Exception as exc:
        print(f"股票目录预热失败: {exc}")
    yield


app = FastAPI(title="股票数据API服务", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def add_timing(request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start
    print(f"耗时: {duration*1000:.0f}ms ({duration:.2f}s)")
    return response

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(market_data_v1_router)


class StockDataResponse(BaseModel):
    success: bool
    stock_code: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    data_count: int = 0
    data: list = []
    error_code: Optional[str] = None
    error_msg: Optional[str] = None


class StockListResponse(BaseModel):
    success: bool
    date: Optional[str] = None
    data_count: int = 0
    data: list = []
    error_code: Optional[str] = None
    error_msg: Optional[str] = None


@app.get("/")
def root():
    return {"message": "股票数据API服务", "docs": "/docs"}


@app.get("/api/stock/kdata", response_model=StockDataResponse)
def stock_k_data(
    stock_code: str = Query(..., description="股票代码，如 sh.600000"),
    start_date: str = Query(..., description="开始日期，格式 2024-07-01"),
    end_date: str = Query(..., description="结束日期，格式 2024-12-31"),
    frequency: str = Query("d", description="数据频率: d=日, w=周, m=月, 5=5分钟, 15=15分钟, 30=30分钟, 60=60分钟"),
    adjustflag: str = Query("3", description="复权类型: 1=后复权, 2=前复权, 3=不复权")
):
    """
    获取股票历史K线数据
    """
    result = get_stock_k_data(
        stock_code=stock_code,
        start_date=start_date,
        end_date=end_date,
        frequency=frequency,
        adjustflag=adjustflag
    )
    return result


@app.get("/api/stock/list", response_model=StockListResponse)
def stock_list(
    date: Optional[str] = Query(None, description="查询日期，格式 2024-07-01；留空读取本地目录快照")
):
    """
    获取股票列表：指定日期实时查询，留空读取每日目录快照
    """
    if date:
        return query_all_stock(date=date)
    try:
        entries, loaded_at = list_stocks()
    except StockDirectoryError as exc:
        return StockListResponse(success=False, error_code="UPSTREAM_UNAVAILABLE", error_msg=str(exc))
    return StockListResponse(
        success=True,
        date=loaded_at.astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d") if loaded_at else None,
        data_count=len(entries),
        data=[entry.as_row() for entry in entries],
    )


class TradingViewDataResponse(BaseModel):
    success: bool
    symbol: Optional[str] = None
    exchange: Optional[str] = None
    timeframe: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    data_count: int = 0
    data: list = []
    warning: Optional[str] = None
    error_msg: Optional[str] = None


@app.get("/api/tradingview/kdata", response_model=TradingViewDataResponse)
def tradingview_kdata(
    symbol: str = Query(..., description="品种代码，如 XAUUSD、600519、BTCUSDT、小米集团"),
    exchange: str = Query("", description="交易所代码，如 OANDA、SSE、BINANCE；留空自动探测"),
    timeframe: str = Query("1d", description="周期: 1m,3m,5m,15m,30m,1h,2h,3h,4h,1d,1w,1M"),
    start_date: str = Query(..., description="开始日期，格式 2024-01-01"),
    end_date: str = Query(..., description="结束日期，格式 2024-12-31"),
    adjust: str = Query("splits", description="复权类型: splits(仅拆股), dividends(前复权), none(不复权)"),
):
    """
    从 TradingView 获取 K 线数据（通过 tvDatafeed）
    """
    try:
        src = TradingViewSource(adjust=adjust)
        src.connect()
        try:
            if exchange:
                src.set_exchange(exchange)
            src.subscribe(symbol, timeframe)
            bars, warning = src.fetch_range(start_date, end_date)
        finally:
            src.disconnect()
    except Exception as e:
        return TradingViewDataResponse(
            success=False,
            symbol=symbol,
            exchange=exchange or "自动",
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            data_count=0,
            data=[],
            error_msg=str(e),
        )

    data = [
        {
            "seq": b.seq,
            "ts_open": int(ts_open_to_ms(b.ts_open)),
            "date": epoch_to_date_str(b.ts_open),
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
            "closed": b.closed,
        }
        for b in bars
    ]
    return TradingViewDataResponse(
        success=True,
        symbol=symbol,
        exchange=src.exchange or exchange or "自动",
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
        data_count=len(data),
        data=data,
        warning=warning,
        error_msg=None,
    )


@app.get("/api/stock/query")
def stock_query(
    stock_code: str = Query(..., description="股票代码，如 sh.600000"),
    days: int = Query(30, description="查询天数")
):
    """
    快捷查询最近N天的股票数据
    """
    from datetime import datetime, timedelta

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    result = get_stock_k_data(
        stock_code=stock_code,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        frequency="d",
        adjustflag="3"
    )
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

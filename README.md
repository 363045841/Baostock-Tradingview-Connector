# 股票数据 API 服务 (stockbao)

基于 FastAPI 的行情接口服务，支持 **Baostock**（A 股）、**TradingView**（全球品种）和 **finshare**（国内期货）数据源。

---

## 快速开始

```bash
# 安装依赖（推荐 uv）
uv sync

# 启动服务
uv run python server.py

# 服务地址: http://localhost:8000
# API 文档: http://localhost:8000/docs
```

---

## 接口一览

| 接口 | 方法 | 数据源 | 说明 |
|---|---|---|---|
| `/api/stock/kdata` | GET | Baostock | 历史 K 线（A 股） |
| `/api/stock/query` | GET | Baostock | 最近 N 天快捷查询 |
| `/api/stock/list` | GET | Baostock | 股票列表 |
| `/api/tradingview/kdata` | GET | TradingView | 全球品种 K 线 |
| `/api/v1/market-data/bars` | POST | finshare | 期货日线 K 线（`cu0`、`IF2409` 等） |
| `/api/v1/market-data/snapshot` | POST | finshare | 期货实时快照 |

---

## 1. Baostock 接口（A 股）

### GET /api/stock/kdata

获取 A 股历史 K 线数据。

**参数：**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `stock_code` | string | 是 | — | 股票代码，如 `sh.600000`、`sz.000001` |
| `start_date` | string | 是 | — | 开始日期，格式 `YYYY-MM-DD` |
| `end_date` | string | 是 | — | 结束日期，格式 `YYYY-MM-DD` |
| `frequency` | string | 否 | `d` | 频率: `d`=日, `w`=周, `m`=月, `5`=5分钟, `15`=15分钟, `30`=30分钟, `60`=60分钟 |
| `adjustflag` | string | 否 | `3` | 复权: `1`=后复权, `2`=前复权, `3`=不复权 |

**响应：**

```json
{
  "success": true,
  "stock_code": "sh.600000",
  "start_date": "2024-07-01",
  "end_date": "2024-12-31",
  "data_count": 120,
  "data": [
    {
      "date": "2024-07-01",
      "code": "sh.600000",
      "open": 4.52,
      "high": 4.58,
      "low": 4.50,
      "close": 4.55,
      "preclose": 4.53,
      "volume": 52345678,
      "amount": 2.37e8,
      "adjustflag": "3",
      "turn": 0.15,
      "tradestatus": "1",
      "pctChg": 0.44,
      "isST": "0"
    }
  ],
  "error_code": null,
  "error_msg": null
}
```

**示例：**

```
GET /api/stock/kdata?stock_code=sh.600000&start_date=2024-07-01&end_date=2024-12-31&frequency=d&adjustflag=3
```

---

### GET /api/stock/query

快捷查询最近 N 天的日线数据（不复权）。

**参数：**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `stock_code` | string | 是 | — | 股票代码，如 `sh.600000` |
| `days` | int | 否 | `30` | 查询天数 |

**响应：** 同 `/api/stock/kdata`

**示例：**

```
GET /api/stock/query?stock_code=sh.600000&days=30
```

---

### GET /api/stock/list

查询指定日期的全部股票列表。

**参数：**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `date` | string | 否 | — | 查询日期 `YYYY-MM-DD`，留空返回最近交易日 |

**响应：**

```json
{
  "success": true,
  "date": "2024-12-31",
  "data_count": 5000,
  "data": [
    {
      "code": "sh.600000",
      "code_name": "浦发银行"
    }
  ],
  "error_code": null,
  "error_msg": null
}
```

**示例：**

```
GET /api/stock/list?date=2024-12-31
```

---

## 2. finshare 期货接口（V1）

V1 数据源 ID 为 `finshare`，支持国内五大期货交易所：CFFEX、SHFE、DCE、CZCE 和 INE。
搜索期货时使用代码，例如 `cu0`（沪铜连续）或 `IF2409`（股指期货合约）。

```json
POST /api/v1/market-data/bars
{
  "sourceId": "finshare",
  "instrument": {
    "id": "finshare:future:shfe:cu0",
    "symbol": "CU0",
    "exchange": "SHFE",
    "providerRef": {"futureCode": "CU0"}
  },
  "period": "daily",
  "adjustment": "none",
  "limit": 100
}
```

`limit` 必填，范围为 1 到 5000。省略 `beforeTimestamp` 时返回最新的 N 根 K 线；传入 `beforeTimestamp`（UTC Unix 毫秒）时，返回严格早于该时间戳的上一页数据。每页均按时间升序返回。
`barAggregation` 选填，仅作为聚合口径透传并回显（本服务不做重采样，原生周期边界恒为 baostock / TradingView / finshare 的上游边界）。响应中的 `olderData` 为 `exhausted` / `unknown`，表示游标之前是否还有历史。

```json
{
  "sourceId": "finshare",
  "instrument": {
    "id": "finshare:future:shfe:cu0",
    "symbol": "CU0",
    "exchange": "SHFE"
  },
  "period": "daily",
  "adjustment": "none",
  "limit": 100,
  "barAggregation": "original",
  "beforeTimestamp": 1735603200000
}
```

实时快照使用同样的 `instrument`，将路径改为 `/api/v1/market-data/snapshot`。

---

## 3. TradingView 接口（全球品种）

### GET /api/tradingview/kdata

通过 tvDatafeed 从 TradingView 获取 K 线数据，支持 A 股、港股、美股、黄金、外汇、加密货币、指数等全球品种。

**参数：**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `symbol` | string | 是 | — | 品种代码（见下方说明） |
| `exchange` | string | 否 | `""` | 交易所代码，留空自动探测 |
| `timeframe` | string | 否 | `1d` | 周期: `1m`,`3m`,`5m`,`15m`,`30m`,`1h`,`2h`,`3h`,`4h`,`1d`,`1w`,`1M` |
| `count` | int | 否 | `100` | 返回 K 线根数 |

**响应：**

```json
{
  "success": true,
  "symbol": "XAUUSD",
  "exchange": "OANDA",
  "timeframe": "1d",
  "data_count": 100,
  "data": [
    {
      "seq": 1,
      "ts_open": 1718208000000,
      "open": 2320.5,
      "high": 2335.0,
      "low": 2315.0,
      "close": 2330.0,
      "volume": 125000.0,
      "closed": false
    }
  ],
  "error_msg": null
}
```

**data 字段说明：**

| 字段 | 类型 | 说明 |
|---|---|---|
| `seq` | int | 序号，1=最新一根（可能未闭合） |
| `ts_open` | int | 开盘时间戳（Unix 毫秒，UTC） |
| `open` | float | 开盘价 |
| `high` | float | 最高价 |
| `low` | float | 最低价 |
| `close` | float | 收盘价 |
| `volume` | float | 成交量 |
| `closed` | bool | K 线是否已闭合（`false` = 当前仍在形成的 K 线） |

---

#### symbol 写法

不同品种的 `symbol` 填写规则：

| 品种 | 示例 | 备注 |
|---|---|---|
| **现货黄金** | `XAUUSD` | 自动选择 OANDA / PEPPERSTONE |
| **A 股** | `600519`、`000001` | 6 位数字代码，自动判断 SSE / SZSE |
| **港股** | `1810`、`700` | 纯数字代码（不加前导零） |
| **美股** | `AAPL`、`MSFT` | 英文 ticker，需指定 exchange |
| **加密货币** | `BTCUSDT`、`ETHUSDT` | 自动选择 BINANCE |
| **指数** | `SPX`、`NDX`、`VIX` | 已知指数自动解析 |
| **外汇** | `EURUSD`、`GBPUSD` | 自动选择外汇交易所 |
| **中文名称** | `小米集团`、`贵州茅台` | 内置别名自动映射 |

---

#### exchange 写法

留空（`""`）或 `AUTO` 时自动探测交易所。如需指定：

| 交易所代码 | 适用品种 |
|---|---|
| `OANDA` | 黄金、外汇 |
| `PEPPERSTONE` | 黄金、外汇 |
| `FOREXCOM` | 黄金、外汇 |
| `TVC` | 黄金（用 `GOLD` 代码）、指数 |
| `CAPITALCOM` | 黄金（用 `GOLD` 代码） |
| `SSE` | A 股（上海） |
| `SZSE` | A 股（深圳） |
| `HKEX` | 港股 |
| `BINANCE` | 加密货币 |
| `COINBASE` | 加密货币 |
| `NYSE` | 美股 |
| `NASDAQ` | 美股 |
| `SP` | 标普指数 |
| `CBOT` | 期货、VIX |

**示例：**

```
# 自动探测黄金
GET /api/tradingview/kdata?symbol=XAUUSD

# 指定交易所获取 A 股
GET /api/tradingview/kdata?symbol=600519&exchange=SSE

# 港股
GET /api/tradingview/kdata?symbol=1810&exchange=HKEX

# 加密货币
GET /api/tradingview/kdata?symbol=BTCUSDT&exchange=BINANCE

# 5 分钟线
GET /api/tradingview/kdata?symbol=XAUUSD&timeframe=5m&count=50

# 中文名称
GET /api/tradingview/kdata?symbol=小米集团&timeframe=1h&count=200
```

---

## 通用说明

### 错误响应

所有接口在失败时返回：

```json
{
  "success": false,
  "data_count": 0,
  "data": [],
  "error_msg": "TradingView 连接超时（OANDA / XAUUSD）：请检查网络..."
}
```

### CORS

所有接口已开启跨域（`allow_origins=["*"]`），可直接在浏览器中调用。

### Swagger 文档

启动服务后访问 `http://localhost:8000/docs` 即可在线调试。

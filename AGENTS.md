# stockbao — K 线数据 API 服务

## 快速命令

```bash
uv sync            # 安装依赖
uv run python server.py   # 启动（http://localhost:8000）
python server.py          # 如果 .venv 已激活
```

## 架构

- 纯后端 FastAPI，无前端。股票目录（search / `/api/stock/list` 不带日期）落 SQLite 快照，每天向 baostock 拉一次；K 线仍实时拉取
- **两个数据源**：`/api/stock/*` 走 baostock（A 股），`/api/tradingview/*` 走 tvDatafeed 连 TradingView（全球品种）
- 两个 remote：`gitee` + `github`

## TradingView 代理

tvDatafeed 底层走 WebSocket，部分环境需要代理才能连 TradingView。优先级（高→低）：
1. 环境变量 `HTTP_PROXY` / `HTTPS_PROXY`
2. `config/proxy.json` 显式配置（已 gitignore）
3. Windows 注册表 WinINET 代理自动检测（兜底）

## 数据包结构

```
data/
├── base.py                KlineBar 数据类、DataSource 抽象、错误类型（共享）
├── datetime_ts.py         时间戳转换（共享）
└── tradingview/           TradingView 数据源子包
    ├── source.py           TvDatafeed 封装，含连接/订阅/快照/交易所自动探测
    ├── market_defaults.py   交易所解析、品种自动探测（A 股/港股/黄金/加密/指数）
    ├── symbol_lookup.py     中文/英文名称 → TradingView 交易所+代码映射
    ├── config.py            配置加载（proxy.json）
    ├── bar_close_wait.py    K 线闭合状态判断
    └── errors.py            TradingView 错误消息格式化
```

baostock 相关代码在顶层 `stock_service.py` 与 `api/market_data_v1.py`，股票目录快照与本地搜索在顶层 `stock_directory.py`。

## Docker

```bash
docker build -t stockbao .
docker run -p 8000:8000 stockbao
```

Docker 使用官方 PyPI 源。

## 股票目录快照

- 数据源：baostock `query_stock_basic()`，无参返回全市场证券基本资料（含退市）
- 落盘：`data/stock-directory.db`（SQLite，WAL），可用环境变量 `STOCK_DIRECTORY_DB_PATH` 覆盖
- 刷新：TTL 24h；上游有访问频率限制，刷新单次尝试，失败直接报错，不重试、不回退旧快照

## 注意

- 测试：`uv run python -m unittest discover -s tests`；无 lint、无 typecheck，暂无 CI
- 新增依赖：`tvdatafeed @ git+https://github.com/rongardF/tvdatafeed.git`
- `data/config.py` 仅加载 `config/proxy.json`

## Git 提交

- 提交信息使用 **英文**
- 格式：`<type>: <subject>`（conventional commits）
- 类型：`feat` / `fix` / `chore` / `docs` / `refactor` / `perf` / `test`

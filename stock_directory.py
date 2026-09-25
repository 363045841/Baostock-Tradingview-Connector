"""股票目录快照：每天从 baostock 拉取全量证券基本资料并落盘 SQLite，搜索与列表读本地。

对齐 GoTDX Connector 的证券目录缓存思路：TTL 内复用内存快照，过期重新拉取。
上游有访问频率限制，因此刷新只做单次尝试，失败直接抛错，不做重试与陈旧回退。
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

import baostock as bs

# 目录快照存活时间，超过则重新拉取上游
DIRECTORY_TTL = timedelta(hours=24)
# 默认 SQLite 快照路径，可用环境变量 STOCK_DIRECTORY_DB_PATH 覆盖
DEFAULT_DB_PATH = Path("data") / "stock-directory.db"
# 匹配度最大值；达到该值表示不匹配
_NO_MATCH_RANK = 6


class StockDirectoryError(RuntimeError):
    """上游证券目录不可用。"""


@dataclass(frozen=True)
class DirectoryEntry:
    """一条证券基本资料，字段与 baostock query_stock_basic 返回列对齐。"""

    code: str
    code_name: str
    ipo_date: str = ""
    out_date: str = ""
    security_type: str = ""
    status: str = ""

    def as_row(self) -> dict[str, str]:
        """转换为 baostock 原字段名的字典，供列表接口原样返回。"""
        return {
            "code": self.code,
            "code_name": self.code_name,
            "ipoDate": self.ipo_date,
            "outDate": self.out_date,
            "type": self.security_type,
            "status": self.status,
        }


_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS stock_directory (
    code TEXT PRIMARY KEY,
    code_name TEXT NOT NULL,
    ipo_date TEXT NOT NULL DEFAULT '',
    out_date TEXT NOT NULL DEFAULT '',
    security_type TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS stock_directory_metadata (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    loaded_at_unix INTEGER NOT NULL
);
"""


class StockDirectoryStore:
    """股票目录的 SQLite 快照存储。"""

    def __init__(self, path: str | os.PathLike[str] = DEFAULT_DB_PATH) -> None:
        """打开或创建目录库，启用 WAL 与忙等待。"""
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SQLITE_SCHEMA)
        self._conn.commit()

    def load(self) -> tuple[list[DirectoryEntry], datetime] | None:
        """读取快照；库为空时返回 None。"""
        row = self._conn.execute(
            "SELECT loaded_at_unix FROM stock_directory_metadata WHERE id = 1"
        ).fetchone()
        if row is None:
            return None
        entries = [
            DirectoryEntry(
                code=code,
                code_name=code_name,
                ipo_date=ipo_date,
                out_date=out_date,
                security_type=security_type,
                status=status,
            )
            for code, code_name, ipo_date, out_date, security_type, status in self._conn.execute(
                "SELECT code, code_name, ipo_date, out_date, security_type, status "
                "FROM stock_directory ORDER BY rowid"
            )
        ]
        return entries, datetime.fromtimestamp(row[0], tz=timezone.utc)

    def replace(self, entries: Iterable[DirectoryEntry], loaded_at: datetime) -> None:
        """以事务整表替换快照，失败回滚保留旧快照。"""
        rows = [
            (entry.code, entry.code_name, entry.ipo_date, entry.out_date, entry.security_type, entry.status)
            for entry in entries
        ]
        with self._conn:
            self._conn.execute("DELETE FROM stock_directory")
            self._conn.executemany(
                "INSERT INTO stock_directory "
                "(code, code_name, ipo_date, out_date, security_type, status) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows,
            )
            self._conn.execute(
                "INSERT INTO stock_directory_metadata (id, loaded_at_unix) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET loaded_at_unix = excluded.loaded_at_unix",
                (int(loaded_at.timestamp()),),
            )

    def close(self) -> None:
        """关闭数据库连接。"""
        self._conn.close()


def load_stock_basic() -> list[DirectoryEntry]:
    """登录 baostock 拉取全量证券基本资料；失败或结果为空直接抛 StockDirectoryError。"""
    login = bs.login()
    if login.error_code != "0":
        raise StockDirectoryError(login.error_msg or "baostock login failed")
    try:
        result = bs.query_stock_basic()
        if result is None or result.error_code != "0":
            message = getattr(result, "error_msg", "") or "baostock query_stock_basic failed"
            raise StockDirectoryError(message)
        entries = []
        while result.error_code == "0" and result.next():
            fields = dict(zip(result.fields, result.get_row_data()))
            entries.append(
                DirectoryEntry(
                    code=str(fields.get("code", "")),
                    code_name=str(fields.get("code_name", "")),
                    ipo_date=str(fields.get("ipoDate", "")),
                    out_date=str(fields.get("outDate", "")),
                    security_type=str(fields.get("type", "")),
                    status=str(fields.get("status", "")),
                )
            )
        # SDK 在下一页请求失败时会静默返回 False，末页恰好满页即视为翻页中断
        if result.error_code != "0":
            raise StockDirectoryError(result.error_msg or "baostock query_stock_basic failed")
        page_size = int(result.per_page_count or 0)
        if page_size > 0 and len(result.data) == page_size:
            raise StockDirectoryError(
                "baostock stock directory paging was interrupted; refusing a partial snapshot"
            )
        if not entries:
            raise StockDirectoryError("baostock returned an empty stock directory")
        return entries
    finally:
        bs.logout()


def _match_rank(entry: DirectoryEntry, needle: str) -> int:
    """计算匹配度：代码精确/前缀/包含优先于名称，数值越小越靠前。"""
    code = entry.code.casefold()
    name = entry.code_name.casefold()
    if code == needle:
        return 0
    if code.startswith(needle):
        return 1
    if needle in code:
        return 2
    if name == needle:
        return 3
    if name.startswith(needle):
        return 4
    if needle in name:
        return 5
    return _NO_MATCH_RANK


class StockDirectoryCache:
    """进程内股票目录缓存：TTL 内读内存，过期重新拉取并落盘。"""

    def __init__(
        self,
        loader: Callable[[], list[DirectoryEntry]] = load_stock_basic,
        store: StockDirectoryStore | None = None,
        ttl: timedelta = DIRECTORY_TTL,
    ) -> None:
        """装配加载器与持久化存储。"""
        self._loader = loader
        self._store = store
        self._ttl = ttl
        self._lock = threading.Lock()
        self._entries: list[DirectoryEntry] = []
        self._loaded_at: datetime | None = None
        self._store_read = False

    def warm_up(self) -> None:
        """预热目录，供服务启动时调用。"""
        print("[stock-directory] warming up")
        self._directory()

    def search(self, keyword: str, limit: int) -> list[DirectoryEntry]:
        """按关键字本地匹配代码与名称，按匹配度排序后截断。"""
        needle = keyword.strip().casefold()
        if not needle:
            return []
        entries = self._directory()
        matched = [entry for entry in entries if _match_rank(entry, needle) < _NO_MATCH_RANK]
        matched.sort(key=lambda entry: _match_rank(entry, needle))
        return matched[:limit]

    def all_entries(self) -> list[DirectoryEntry]:
        """返回当前目录快照，供列表接口使用。"""
        return self._directory()

    def loaded_at(self) -> datetime | None:
        """返回当前快照的加载时间。"""
        return self._loaded_at

    def _directory(self) -> list[DirectoryEntry]:
        """取目录：TTL 内直接返回，否则单次拉取刷新，失败即抛错。"""
        with self._lock:
            self._read_store()
            now = datetime.now(timezone.utc)
            if self._loaded_at is not None and now - self._loaded_at < self._ttl:
                return self._entries
            print("[stock-directory] fetching full directory from baostock")
            started = time.perf_counter()
            entries = self._loader()
            elapsed = time.perf_counter() - started
            self._entries = entries
            self._loaded_at = now
            self._persist(entries, now)
            print(f"[stock-directory] loaded {len(entries)} entries in {elapsed:.2f}s")
            return self._entries

    def _read_store(self) -> None:
        """首次使用时读取一次 SQLite 快照填充内存。"""
        if self._store_read or self._store is None:
            return
        self._store_read = True
        snapshot = self._store.load()
        if snapshot is not None:
            self._entries, self._loaded_at = snapshot
            print(f"[stock-directory] loaded {len(self._entries)} entries from local snapshot")

    def _persist(self, entries: list[DirectoryEntry], loaded_at: datetime) -> None:
        """落盘快照；持久化失败只告警，不影响本次内存结果。"""
        if self._store is None:
            return
        try:
            self._store.replace(entries, loaded_at)
        except (OSError, sqlite3.Error) as exc:
            print(f"[stock-directory] failed to persist snapshot: {exc}")


_cache: StockDirectoryCache | None = None
_cache_lock = threading.Lock()


def _get_cache() -> StockDirectoryCache:
    """惰性创建进程级目录缓存，库路径由环境变量或默认值决定。"""
    global _cache
    with _cache_lock:
        if _cache is None:
            path = os.environ.get("STOCK_DIRECTORY_DB_PATH") or DEFAULT_DB_PATH
            _cache = StockDirectoryCache(store=StockDirectoryStore(path))
        return _cache


def warm_up() -> None:
    """预热进程级目录缓存。"""
    _get_cache().warm_up()


def search_stocks(keyword: str, limit: int) -> list[DirectoryEntry]:
    """搜索本地股票目录。"""
    return _get_cache().search(keyword, limit)


def list_stocks() -> tuple[list[DirectoryEntry], datetime | None]:
    """返回本地股票目录快照与加载时间。"""
    cache = _get_cache()
    return cache.all_entries(), cache.loaded_at()

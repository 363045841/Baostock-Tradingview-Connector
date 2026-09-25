"""股票目录 SQLite 快照与本地搜索的无网络单元测试。"""

import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import stock_directory
from stock_directory import DirectoryEntry, StockDirectoryCache, StockDirectoryError


def _entries() -> list[DirectoryEntry]:
    """构造覆盖代码/名称匹配优先级的目录样本。"""
    return [
        DirectoryEntry(code="sh.600000", code_name="浦发银行"),
        DirectoryEntry(code="sz.000001", code_name="平安银行"),
        DirectoryEntry(code="sz.000002", code_name="600000测试"),
    ]


class _FakeLoader:
    """可计数、可抛错的目录加载替身。"""

    def __init__(self, entries: list[DirectoryEntry] | None = None, error: Exception | None = None):
        self.calls = 0
        self._entries = entries if entries is not None else _entries()
        self._error = error

    def __call__(self) -> list[DirectoryEntry]:
        self.calls += 1
        if self._error is not None:
            raise self._error
        return self._entries


class StockDirectorySearchTest(unittest.TestCase):
    """验证本地搜索的匹配与排序。"""

    def test_code_match_ranks_before_name_match(self):
        """代码命中排在名称命中之前。"""
        cache = StockDirectoryCache(loader=_FakeLoader())
        result = cache.search("600000", 10)
        self.assertEqual([entry.code for entry in result], ["sh.600000", "sz.000002"])

    def test_search_reuses_snapshot_within_ttl(self):
        """TTL 内重复搜索只拉取一次上游。"""
        loader = _FakeLoader()
        cache = StockDirectoryCache(loader=loader)
        cache.search("浦发", 10)
        cache.search("平安", 10)
        self.assertEqual(loader.calls, 1)

    def test_non_matching_keyword_returns_empty(self):
        """无命中返回空列表。"""
        cache = StockDirectoryCache(loader=_FakeLoader())
        self.assertEqual(cache.search("不存在", 10), [])

    def test_expired_snapshot_triggers_reload(self):
        """TTL 过期后重新拉取上游。"""
        loader = _FakeLoader()
        cache = StockDirectoryCache(loader=loader, ttl=timedelta(0))
        cache.search("浦发", 10)
        cache.search("浦发", 10)
        self.assertEqual(loader.calls, 2)

    def test_loader_failure_propagates(self):
        """无快照且拉取失败时直接抛错，不做重试。"""
        loader = _FakeLoader(error=StockDirectoryError("upstream down"))
        cache = StockDirectoryCache(loader=loader)
        with self.assertRaises(StockDirectoryError):
            cache.search("浦发", 10)
        self.assertEqual(loader.calls, 1)


class StockDirectoryPersistenceTest(unittest.TestCase):
    """验证 SQLite 快照的落盘与复用。"""

    def test_snapshot_is_persisted_and_reloaded(self):
        """首个进程落盘后，新进程无需再拉上游。"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stock-directory.db"
            first_loader = _FakeLoader()
            first_store = stock_directory.StockDirectoryStore(path)
            try:
                first = StockDirectoryCache(loader=first_loader, store=first_store)
                self.assertEqual(len(first.search("银行", 10)), 2)
                self.assertEqual(first_loader.calls, 1)
            finally:
                first_store.close()

            offline_loader = _FakeLoader(error=StockDirectoryError("should not be called"))
            second_store = stock_directory.StockDirectoryStore(path)
            try:
                second = StockDirectoryCache(loader=offline_loader, store=second_store)
                self.assertEqual(len(second.search("银行", 10)), 2)
                self.assertEqual(offline_loader.calls, 0)
            finally:
                second_store.close()


class _FakeResult:
    """模拟 baostock ResultData 的翻页读取，用于复现末页满页的静默中断。"""

    def __init__(self, pages: list[list[list[str]]], per_page_count: int):
        self.fields = ["code", "code_name", "ipoDate", "outDate", "type", "status"]
        self.per_page_count = per_page_count
        self.error_code = "0"
        self.error_msg = ""
        self.data = pages[0]
        self._pending = list(pages[1:])
        self._row = 0

    def next(self) -> bool:
        """当前页有剩余行返回 True；否则取下一页，无下一页返回 False。"""
        if self._row < len(self.data):
            return True
        if not self._pending:
            return False
        self.data = self._pending.pop(0)
        self._row = 0
        return True

    def get_row_data(self) -> list[str]:
        """返回当前行并前进到下一条。"""
        row = self.data[self._row]
        self._row += 1
        return row


def _patched_baostock(result: _FakeResult):
    """把 stock_directory 的 baostock 客户端替换为返回指定结果的替身。"""
    fake = MagicMock()
    fake.login.return_value = SimpleNamespace(error_code="0", error_msg="")
    fake.query_stock_basic.return_value = result
    return patch.object(stock_directory, "bs", fake)


class StockDirectoryLoaderTest(unittest.TestCase):
    """验证上游翻页中断不会被误当成完整目录。"""

    def test_short_final_page_is_a_complete_directory(self):
        """末页不足一页视为完整拉取。"""
        row = ["sh.600000", "浦发银行", "1999-11-10", "", "1", "1"]
        result = _FakeResult([[row]], per_page_count=2000)
        with _patched_baostock(result):
            entries = stock_directory.load_stock_basic()
        self.assertEqual([entry.code for entry in entries], ["sh.600000"])

    def test_full_final_page_raises_on_interrupted_paging(self):
        """末页恰好满页说明下一页请求静默失败，必须报错而非存部分快照。"""
        row = ["sh.600000", "浦发银行", "1999-11-10", "", "1", "1"]
        result = _FakeResult([[row] * 2000], per_page_count=2000)
        with _patched_baostock(result):
            with self.assertRaises(StockDirectoryError):
                stock_directory.load_stock_basic()


if __name__ == "__main__":
    unittest.main()

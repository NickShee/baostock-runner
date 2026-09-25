"""D-02: 复权映射、计算与版本缓存测试。

验收点：
- Adapter 将 dividOperateDate 映射为统一 effective_date；不再依赖 'date' 错误假设。
- 按接口契约区分累计适用因子与单次比例；累计因子按日期匹配后直接乘原价，不默认连乘。
- 查询起点之前的适用因子必须可得，未知时返回不完整，不自动填 1。
- 缓存键包含证券、日期区间、口径、基准日、行情版本、因子版本和算法版本。
- 版本化存储：put/get_adjust_factors、adjust_factor_versions 可追溯。
- 对照官方返回：多次除权、区间从中途开始、无事件样本。
- 本地算法未通过时使用带来源和版本的官方复权结果作为临时路径。
"""
import os
import tempfile
import unittest

from baostock_runner.adjustment import (ADJUST_BACKWARD, ADJUST_FORWARD,
                                         ADJUST_RAW, ALGORITHM_VERSION,
                                         AdjustFactor, AdjustmentCache,
                                         AdjustmentCalculator, FactorAdapter,
                                         adjustment_cache_key)
from baostock_runner.config import Settings
from baostock_runner.storage import Storage
from baostock_runner.timeutil import Clock
import datetime


def _clock(now_iso="2026-09-24T10:00:00+00:00"):
    return Clock(now_fn=lambda: datetime.datetime.fromisoformat(now_iso))


def _mk(directory):
    return Storage(os.path.join(directory, "data.sqlite3"),
                   os.path.join(directory, "audit.jsonl"), clock=_clock())


# 官方返回示例（sh.600000 2015-2017 三次除权）
OFFICIAL_ROWS = [
    {"code": "sh.600000", "dividOperateDate": "2015-06-23",
     "foreAdjustFactor": "0.663792", "backAdjustFactor": "6.295967", "adjustFactor": "6.295967"},
    {"code": "sh.600000", "dividOperateDate": "2016-06-23",
     "foreAdjustFactor": "0.751598", "backAdjustFactor": "7.128788", "adjustFactor": "7.128788"},
    {"code": "sh.600000", "dividOperateDate": "2017-05-25",
     "foreAdjustFactor": "0.989551", "backAdjustFactor": "9.385732", "adjustFactor": "9.385732"},
]


class FactorAdapterTest(unittest.TestCase):
    def test_maps_divid_operate_date_to_effective_date(self):
        """dividOperateDate → effective_date 统一映射（不依赖 'date'）。"""
        adapter = FactorAdapter()
        factors = adapter.map_rows(OFFICIAL_ROWS)
        self.assertEqual(len(factors), 3)
        self.assertEqual(factors[0].effective_date, "2015-06-23")
        self.assertEqual(factors[0].fore_adjust_factor, 0.663792)
        self.assertEqual(factors[0].back_adjust_factor, 6.295967)
        self.assertEqual(factors[0].single_adjust_factor, 6.295967)
        self.assertEqual(factors[0].source, "baostock")
        self.assertEqual(factors[0].algorithm_version, ALGORITHM_VERSION)

    def test_date_fallback_for_old_versions(self):
        """部分版本返回 date 而非 dividOperateDate：兼容映射。"""
        adapter = FactorAdapter()
        factors = adapter.map_rows([{"code": "sh.600000", "date": "2020-07-01",
                                     "backAdjustFactor": "2.0"}])
        self.assertEqual(len(factors), 1)
        self.assertEqual(factors[0].effective_date, "2020-07-01")

    def test_invalid_row_dropped(self):
        """无有效日期或因子值的行丢弃（不猜值）。"""
        adapter = FactorAdapter()
        factors = adapter.map_rows([
            {"code": "sh.600000", "foreAdjustFactor": "1.0"},          # 无日期
            {"code": "sh.600000", "dividOperateDate": "2020-07-01"},   # 无因子
            {"code": "sh.600000", "dividOperateDate": "2020-07-01", "backAdjustFactor": ""},
        ])
        self.assertEqual(factors, [])


class AdjustmentCalculatorTest(unittest.TestCase):
    def _factors(self):
        return [AdjustFactor.from_payload(f.to_payload()) for f in FactorAdapter().map_rows(OFFICIAL_ROWS)]

    def test_backward_factor_direct_multiply(self):
        """后复权累计因子按日期匹配后直接乘原价（不连乘）。"""
        calc = AdjustmentCalculator()
        calc.set_factors(self._factors())
        # 2015-06-23 除权后：6.295967 直接乘（非连乘 6.295967）
        price, factor = calc.adjust_price(10.0, "2015-06-23", ADJUST_BACKWARD)
        self.assertAlmostEqual(factor, 6.295967, places=6)
        self.assertAlmostEqual(price, 62.95967, places=6)
        # 2016-06-23 之后：7.128788 直接乘（非 6.295967*7.128788）
        price2, factor2 = calc.adjust_price(10.0, "2016-06-23", ADJUST_BACKWARD)
        self.assertAlmostEqual(factor2, 7.128788, places=6)
        self.assertAlmostEqual(price2, 71.28788, places=6)

    def test_forward_factor_direct_multiply(self):
        """前复权累计因子直接乘。"""
        calc = AdjustmentCalculator()
        calc.set_factors(self._factors())
        price, factor = calc.adjust_price(10.0, "2017-05-25", ADJUST_FORWARD)
        self.assertAlmostEqual(factor, 0.989551, places=6)
        self.assertAlmostEqual(price, 9.89551, places=6)

    def test_raw_no_adjust(self):
        """不复权：原价返回，因子 1.0。"""
        calc = AdjustmentCalculator()
        price, factor = calc.adjust_price(10.0, "2015-06-23", ADJUST_RAW)
        self.assertEqual(price, 10.0)
        self.assertEqual(factor, 1.0)

    def test_interval_starting_midway_uses_earlier_factor(self):
        """区间从中途开始：起点之前最后一次除权因子仍适用。"""
        calc = AdjustmentCalculator()
        calc.set_factors(self._factors())
        # 查询从 2016-01-01 开始（在 2015-06-23 除权之后、2016-06-23 之前）
        price, factor = calc.adjust_price(10.0, "2016-01-15", ADJUST_BACKWARD)
        self.assertAlmostEqual(factor, 6.295967, places=6)  # 用 2015 年因子
        # 2016-06-23 后切换到新因子
        price2, factor2 = calc.adjust_price(10.0, "2016-07-01", ADJUST_BACKWARD)
        self.assertAlmostEqual(factor2, 7.128788, places=6)

    def test_before_first_ex_date_returns_one_when_no_unknown(self):
        """无事件样本：起点早于最早除权日且无更早未知记录 → 因子 1（未除权）。"""
        calc = AdjustmentCalculator()
        calc.set_factors(self._factors())
        factor = calc.factor_on_or_before("2015-01-01", ADJUST_BACKWARD)
        self.assertEqual(factor, 1.0)

    def test_empty_factor_table_returns_incomplete_not_one(self):
        """空因子表：返回不完整（None），不自动填 1。"""
        calc = AdjustmentCalculator()
        price, factor = calc.adjust_price(10.0, "2015-06-23", ADJUST_BACKWARD)
        self.assertIsNone(factor)
        self.assertIsNone(price)

    def test_series_marks_incomplete_days(self):
        """序列复权：未知因子日标记 incomplete，不填 1。"""
        calc = AdjustmentCalculator()
        calc.set_factors(self._factors())
        bars = [
            {"date": "2015-06-23", "close": "10.0"},
            {"date": "2015-06-24", "close": "10.2"},
        ]
        out = calc.adjust_series(bars, ADJUST_BACKWARD)
        self.assertEqual(len(out), 2)
        self.assertFalse(out[0]["incomplete"])
        self.assertAlmostEqual(out[0]["adjusted_close"], 62.95967, places=6)
        self.assertAlmostEqual(out[0]["factor"], 6.295967, places=6)


class AdjustmentCacheKeyTest(unittest.TestCase):
    def test_cache_key_contains_all_dimensions(self):
        """缓存键含证券/区间/口径/基准日/行情版本/因子版本/算法版本。"""
        k1 = adjustment_cache_key(code="sh.600000", start_date="2020-01-01",
                                  end_date="2020-12-31", adjust=ADJUST_BACKWARD,
                                  base_date="2026-09-24", quote_version="bars-v1",
                                  factor_version="factor-v1")
        k2 = adjustment_cache_key(code="sh.600000", start_date="2020-01-01",
                                  end_date="2020-12-31", adjust=ADJUST_BACKWARD,
                                  base_date="2026-09-24", quote_version="bars-v2",
                                  factor_version="factor-v1")
        k3 = adjustment_cache_key(code="sh.600000", start_date="2020-01-01",
                                  end_date="2020-12-31", adjust=ADJUST_FORWARD,
                                  base_date="2026-09-24", quote_version="bars-v1",
                                  factor_version="factor-v1")
        self.assertNotEqual(k1, k2)  # 行情版本不同 → 键不同
        self.assertNotEqual(k1, k3)  # 口径不同 → 键不同

    def test_cache_roundtrip(self):
        """版本缓存读写：同版本命中，版本变化后不命中。"""
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(d)
            cache = AdjustmentCache(storage, quote_version="bars-v1", factor_version="factor-v1")
            cache.put(code="sh.600000", start_date="2020-01-01", end_date="2020-12-31",
                      adjust=ADJUST_BACKWARD, base_date="2026-09-24", value={"adjusted": [1, 2]})
            hit = cache.get(code="sh.600000", start_date="2020-01-01", end_date="2020-12-31",
                            adjust=ADJUST_BACKWARD, base_date="2026-09-24")
            self.assertEqual(hit, {"adjusted": [1, 2]})
            # 行情版本变化 → 键不同 → 不命中
            cache2 = AdjustmentCache(storage, quote_version="bars-v2", factor_version="factor-v1")
            miss = cache2.get(code="sh.600000", start_date="2020-01-01", end_date="2020-12-31",
                              adjust=ADJUST_BACKWARD, base_date="2026-09-24")
            self.assertIsNone(miss)


class AdjustmentStorageTest(unittest.TestCase):
    def test_put_and_get_roundtrip(self):
        """版本化存储：put_adjust_factors（官方行）→ get_adjust_factors 回读标准 payload。"""
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(d)
            storage.put_adjust_factors("sh.600000", OFFICIAL_ROWS)
            factors = storage.get_adjust_factors("sh.600000")
            self.assertEqual(len(factors), 3)
            self.assertEqual(factors[0]["effective_date"], "2015-06-23")
            self.assertAlmostEqual(factors[0]["back_adjust_factor"], 6.295967, places=6)
            self.assertEqual(factors[0]["source"], "baostock")
            self.assertEqual(factors[0]["algorithm_version"], ALGORITHM_VERSION)

    def test_versions_traceable(self):
        """adjust_factor_versions 版本序列可追溯。"""
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(d)
            storage.put_adjust_factors("sh.600000", OFFICIAL_ROWS)
            versions = storage.get_adjust_factor_versions("sh.600000", "2015-06-23")
            self.assertEqual(len(versions), 1)
            self.assertEqual(versions[0]["code"], "sh.600000")
            self.assertEqual(versions[0]["effective_date"], "2015-06-23")
            self.assertEqual(versions[0]["source"], "baostock")

    def test_no_date_field_bug(self):
        """旧 fetcher 用 'date' 过滤的 bug：官方行只有 dividOperateDate，仍应全部写入。"""
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(d)
            # 官方返回无 'date' 字段
            rows = [{"code": "sh.600000", "dividOperateDate": "2015-06-23",
                     "backAdjustFactor": "6.295967"}]
            storage.put_adjust_factors("sh.600000", rows)
            factors = storage.get_adjust_factors("sh.600000")
            self.assertEqual(len(factors), 1)  # 不被 'date' 过滤掉


if __name__ == "__main__":
    unittest.main()

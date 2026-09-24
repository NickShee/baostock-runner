"""A-03: 可重查任务状态机、财务修订、指数退避、同事务提交、重启回收测试。

测试原则：全部使用临时目录、可注入时钟与本地文件，不登录真实 BaoStock、
不访问生产数据库、不依赖真实当前日期。
验收点：
- 状态机 pending/running/succeeded/waiting_data/retryable_failed/permanent_failed 流转。
- 租约 claim/release；重启回收过期 running 任务。
- 空财报 → waiting_data，到期后重查；未来未结束季度不入队。
- 财务修订保留旧版本（financial_history）。
- 最近两个已结束季度每日检查修订，其余 30 天检查。
- 网络失败指数退避（60s 起、最长 1h）。
- 成功数据与作业完成状态同事务提交。
- 旧 'done' 迁移为 'succeeded'（允许按规则刷新）。
"""
import datetime
import os
import tempfile
import unittest

from baostock_runner.config import Settings
from baostock_runner.fetcher import Fetcher
from baostock_runner.gateway import BaoStockGateway
from baostock_runner.storage import (JOB_PENDING, JOB_PERMANENT_FAILED, JOB_RETRYABLE_FAILED,
                                     JOB_RUNNING, JOB_SUCCEEDED, JOB_WAITING_DATA, SCHEMA_VERSION,
                                     Storage)


def _clock(now_iso):
    now = datetime.datetime.fromisoformat(now_iso)
    from baostock_runner.timeutil import Clock
    return Clock(now_fn=lambda: now)


class JobStateMachineTest(unittest.TestCase):
    def _storage(self, d, now_iso="2026-09-24T10:00:00+00:00"):
        return Storage(os.path.join(d, "data.sqlite3"), os.path.join(d, "audit.jsonl"),
                       clock=_clock(now_iso))

    def test_job_claim_release_flow(self):
        with tempfile.TemporaryDirectory() as d:
            storage = self._storage(d)
            storage.job_upsert("financials", "a|2026Q2|profit", JOB_PENDING)
            # 领取租约
            self.assertTrue(storage.job_claim("financials", "a|2026Q2|profit", lease_seconds=600))
            row = storage._job_row("financials", "a|2026Q2|profit")
            self.assertEqual(row["status"], JOB_RUNNING)
            self.assertIsNotNone(row["lease_expires_at"])
            # running 状态不能再次领取
            self.assertFalse(storage.job_claim("financials", "a|2026Q2|profit", lease_seconds=600))
            # 释放为 succeeded
            storage.job_release("financials", "a|2026Q2|profit", JOB_SUCCEEDED, rows_written=5)
            row = storage._job_row("financials", "a|2026Q2|profit")
            self.assertEqual(row["status"], JOB_SUCCEEDED)
            self.assertIsNone(row["lease_expires_at"])
            self.assertEqual(row["rows_written"], 5)

    def test_invalid_status_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            storage = self._storage(d)
            with self.assertRaises(ValueError):
                storage.job_upsert("financials", "a|1", "nonsense_status")

    def test_recover_stale_running(self):
        with tempfile.TemporaryDirectory() as d:
            # 时钟推进：先创建 running（租约已过期），再推进时间后回收
            storage = self._storage(d, "2026-09-24T10:00:00+00:00")
            storage.job_upsert("financials", "a|2026Q2|profit", JOB_RUNNING,
                               lease_expires_at="2026-09-24T10:05:00+00:00")
            # 推进到 10:10（租约 10:05 已过期）
            storage.clock = _clock("2026-09-24T10:10:00+00:00")
            recovered = storage.job_recover_stale_running()
            self.assertEqual(recovered, 1)
            row = storage._job_row("financials", "a|2026Q2|profit")
            self.assertEqual(row["status"], JOB_RETRYABLE_FAILED)
            self.assertEqual(row["error_class"], "restart_recovery")

    def test_legacy_done_migrated_to_succeeded(self):
        """旧 'done' 状态迁移为 'succeeded'（schema v3 迁移），允许按规则刷新。"""
        with tempfile.TemporaryDirectory() as d:
            storage = self._storage(d)
            # 用裸 SQL 写旧状态（新 job_upsert 会拒绝 'done'）
            with storage._session() as db:
                db.execute("""INSERT INTO download_jobs(dataset, batch_id, status, attempts, error, updated_at)
                              VALUES ('daily_bars', 'sh.600000|3', 'done', 1, NULL, ?)""",
                           (storage.clock.now_utc().isoformat(),))
            # 重新打开（触发迁移）
            storage2 = Storage(os.path.join(d, "data.sqlite3"), os.path.join(d, "audit2.jsonl"),
                               clock=_clock("2026-09-24T10:00:00+00:00"))
            self.assertEqual(storage2.integrity_check()["user_version"], SCHEMA_VERSION)
            self.assertEqual(storage2.job_status("daily_bars", "sh.600000|3"), JOB_SUCCEEDED)


class FinancialRevisionTest(unittest.TestCase):
    def _fetcher(self, d, now_iso="2026-09-24T10:00:00+00:00", **kw):
        settings = Settings(
            db_path=os.path.join(d, "data.sqlite3"),
            log_path=os.path.join(d, "audit.jsonl"),
            min_interval_seconds=0, daily_hard_limit=1000, offline=True,
            **kw)
        gateway = BaoStockGateway(settings, clock=_clock(now_iso))
        return Fetcher(gateway, settings), gateway

    def test_future_quarter_not_queued(self):
        with tempfile.TemporaryDirectory() as d:
            fetcher, gateway = self._fetcher(d, "2026-09-24T10:00:00+00:00",
                                             fetch_financial_start_year=2026)
            try:
                # 2026-09-24：已结束季度 = 2026Q2；Q3/Q4 为未来季度
                self.assertEqual(fetcher._current_ended_quarter(), (2026, 2))
                self.assertTrue(fetcher._is_future_quarter(2026, 4))
                self.assertTrue(fetcher._is_future_quarter(2026, 3))
                self.assertFalse(fetcher._is_future_quarter(2026, 2))
            finally:
                gateway.close()

    def test_empty_report_waiting_then_recheck(self):
        """空财报 → waiting_data；到期后重查（可注入时钟推进）。"""
        with tempfile.TemporaryDirectory() as d:
            fetcher, gateway = self._fetcher(d, "2026-09-24T10:00:00+00:00",
                                             fetch_financial_start_year=2026,
                                             financial_waiting_retry_hours=24)
            try:
                storage = gateway.storage
                due, _r = fetcher._financial_job_due("profit", "sh.600000", 2026, 2)
                self.assertTrue(due)  # 初始无记录 → 执行
                # 模拟空财报：标记 waiting_data（next_retry_at = 1 小时后）
                job_id = "sh.600000|2026Q2|profit"
                storage.job_upsert("financials", job_id, JOB_WAITING_DATA,
                                   next_retry_at="2026-09-24T11:00:00+00:00")
                # 未到期 → 不重查
                due2, _r2 = fetcher._financial_job_due("profit", "sh.600000", 2026, 2)
                self.assertFalse(due2)
                # 推进 25 小时 → 到期重查（同时推进 fetcher 与 storage 的时钟）
                new_clock = _clock("2026-09-25T11:00:00+00:00")
                fetcher.clock = new_clock
                fetcher.storage.clock = new_clock
                due3, _r3 = fetcher._financial_job_due("profit", "sh.600000", 2026, 2)
                self.assertTrue(due3)
            finally:
                gateway.close()

    def test_revision_history_preserved(self):
        """修订保留旧版本：financial_history 留存旧 payload。"""
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(os.path.join(d, "data.sqlite3"), os.path.join(d, "audit.jsonl"),
                              clock=_clock("2026-09-24T10:00:00+00:00"))
            storage.put_financials("profit", "sh.600000", 2026, 2, {"netProfit": "100"})
            storage.put_financials("profit", "sh.600000", 2026, 2, {"netProfit": "120"})  # 修订
            hist = storage.financial_history("profit", "sh.600000", 2026, 2)
            self.assertEqual(len(hist), 2)  # 旧版本 + 当前版本
            self.assertEqual(hist[0]["data"]["netProfit"], "120")
            self.assertEqual(hist[1]["data"]["netProfit"], "100")
            # 相同 payload 不产生新版本
            storage.put_financials("profit", "sh.600000", 2026, 2, {"netProfit": "120"})
            self.assertEqual(len(storage.financial_history("profit", "sh.600000", 2026, 2)), 2)

    def test_recent_quarters_checked_daily_others_30d(self):
        """最近两个已结束季度每天检查修订，其余 30 天检查。"""
        with tempfile.TemporaryDirectory() as d:
            fetcher, gateway = self._fetcher(d, "2026-09-24T10:00:00+00:00",
                                             fetch_financial_start_year=2022,
                                             financial_revision_check_days=1,
                                             financial_revision_check_days_full=30)
            try:
                storage = gateway.storage
                # 近季度 2026Q2：succeeded + 刚采集（fetched_at = 当前）→ 不检查
                storage.put_financials("profit", "sh.600000", 2026, 2, {"netProfit": "1"})
                storage.job_upsert("financials", "sh.600000|2026Q2|profit", JOB_SUCCEEDED)
                due_recent, reason = fetcher._financial_job_due("profit", "sh.600000", 2026, 2)
                self.assertFalse(due_recent)
                # 推进 1 天零 1 分钟 → 近季度到期（>1d）
                c1 = _clock("2026-09-25T10:01:00+00:00")
                fetcher.clock = c1
                fetcher.storage.clock = c1
                due_recent2, reason2 = fetcher._financial_job_due("profit", "sh.600000", 2026, 2)
                self.assertTrue(due_recent2)
                self.assertIn("revision", reason2)
                # 老季度 2022Q4：28 天前采集 → 未到 30 天 → 不检查
                old_dt = (datetime.datetime.fromisoformat("2026-09-25T10:01:00+00:00")
                          - datetime.timedelta(days=28))
                c_old = _clock(old_dt.isoformat())
                storage.clock = c_old
                storage.put_financials("profit", "sh.600000", 2022, 4, {"netProfit": "5"})
                storage.job_upsert("financials", "sh.600000|2022Q4|profit", JOB_SUCCEEDED)
                storage.clock = c1
                due_old, _r = fetcher._financial_job_due("profit", "sh.600000", 2022, 4)
                self.assertFalse(due_old)  # 28 天 < 30 天
                # 31 天前采集 → 到期
                old_dt2 = (datetime.datetime.fromisoformat("2026-09-25T10:01:00+00:00")
                           - datetime.timedelta(days=31))
                c_old2 = _clock(old_dt2.isoformat())
                storage.clock = c_old2
                storage.put_financials("profit", "sh.600000", 2022, 4, {"netProfit": "5"})
                storage.clock = c1
                due_old2, _r2 = fetcher._financial_job_due("profit", "sh.600000", 2022, 4)
                self.assertTrue(due_old2)
            finally:
                gateway.close()

    def test_exponential_backoff(self):
        """指数退避：60s 起、翻倍、最长 1h（返回下一次重试时间戳）。"""
        with tempfile.TemporaryDirectory() as d:
            fetcher, gateway = self._fetcher(d, "2026-09-24T10:00:00+00:00",
                                             task_retry_base_seconds=60,
                                             task_retry_max_seconds=3600)
            try:
                now = fetcher.storage.clock.now_utc().timestamp()
                self.assertEqual(fetcher._retry_delay(1) - now, 60)
                self.assertEqual(fetcher._retry_delay(2) - now, 120)
                self.assertEqual(fetcher._retry_delay(3) - now, 240)
                # 封顶 1h
                self.assertEqual(fetcher._retry_delay(100) - now, 3600)
            finally:
                gateway.close()

    def test_same_transaction_commit(self):
        """成功数据与作业完成状态同事务提交。"""
        with tempfile.TemporaryDirectory() as d:
            fetcher, gateway = self._fetcher(d, "2026-09-24T10:00:00+00:00",
                                             fetch_financial_start_year=2026)
            try:
                storage = gateway.storage
                job_id = "sh.600000|2026Q2|profit"
                # 模拟 fetch 成功后同事务写入
                with storage._session() as db:
                    storage._put_financials_in_txn(db, "profit", "sh.600000", 2026, 2,
                                                   {"netProfit": "88"})
                    storage._job_upsert_in_txn(db, "financials", job_id, JOB_SUCCEEDED,
                                               rows_written=1)
                self.assertTrue(storage.financial_exists("profit", "sh.600000", 2026, 2))
                self.assertEqual(storage.job_status("financials", job_id), JOB_SUCCEEDED)
            finally:
                gateway.close()

    def test_failure_does_not_block_following_tasks(self):
        """失败股票不阻塞后续任务：retryable_failed 后仍可处理其他任务。"""
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(os.path.join(d, "data.sqlite3"), os.path.join(d, "audit.jsonl"),
                              clock=_clock("2026-09-24T10:00:00+00:00"))
            storage.job_upsert("financials", "bad|2026Q2|profit", JOB_RETRYABLE_FAILED,
                               error="network", error_class="ConnectionError",
                               next_retry_at="2026-09-24T10:30:00+00:00")
            storage.job_upsert("financials", "good|2026Q2|profit", JOB_PENDING)
            # 到期任务只包含 good（bad 未到期）
            due = storage.job_due("financials")
            self.assertIn(("good|2026Q2|profit", None), due)
            self.assertNotIn(("bad|2026Q2|profit", "2026-09-24T10:30:00+00:00"), due)


if __name__ == "__main__":
    unittest.main()

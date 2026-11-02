# -*- coding: utf-8 -*-
"""假目标站上的端到端测试。

覆盖的正是"只在服务端异常时才暴露"的行为。真站点上复现不起这些场景，
所以用本地假服务端把每种异常做成可开关的行为，然后断言。

每个用例的名字就是它守护的那条规则。
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from crawler_kit import (  # noqa: E402
    Config,
    Crawler,
    Outcome,
    RequestSpec,
    RetryPolicy,
    Spider,
    Verdict,
    read_json,
    read_jsonl,
)
from mock_target import MockTarget  # noqa: E402


class FakeSleep:
    """记录等待时长而不真睡 —— 让"等了多久"变成可断言的事实。

    同时充当**虚拟时钟**：``sleep`` 推进 ``now``，于是闸门的冷却窗口真的会
    到期。没有这一步，``sleep`` 不消耗时间、冷却截止点永远在未来，下一个
    页会凭空多睡一次，断言就变成在测假时间而不是测行为。
    """

    def __init__(self) -> None:
        self.waits: list[float] = []
        self.now: float = 0.0

    def __call__(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds

    def clock(self) -> float:
        return self.now

    @property
    def total(self) -> float:
        return sum(self.waits)


class ItemSpider(Spider):
    """对着假站的分页采集器。"""

    name = "items"
    page_size = 10

    def __init__(self, target: MockTarget, end_page: int):
        self.target = target
        self.end_page = end_page

    def request(self, page: int) -> RequestSpec:
        return RequestSpec(url=f"{self.target.url()}?page={page}")

    def parse(self, page: int, response) -> Outcome:
        payload = response.json() or {}
        return Outcome(Verdict.OK, records=payload.get("items", []))

    def stop_at(self, page: int, outcome: Outcome) -> bool:
        return not outcome.records and page >= self.end_page


class CrawlerTest(unittest.TestCase):
    """共享脚手架：假站 + 临时工作目录 + 记录式等待。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.work_dir = self._tmp.name
        self.target: MockTarget | None = None
        self.sleep = FakeSleep()

    def tearDown(self) -> None:
        if self.target is not None:
            self.target.stop()
        self._tmp.cleanup()

    def start(self, **kwargs) -> MockTarget:
        self.target = MockTarget(**kwargs).start()
        return self.target

    def crawler(self, *, end_page: int, policy: RetryPolicy | None = None,
                remedy_rounds: int = 0, work_dir: str | None = None,
                concurrency: int = 1) -> Crawler:
        assert self.target is not None
        spider = ItemSpider(self.target, end_page)
        config = Config(work_dir=work_dir or self.work_dir,
                        remedy_rounds=remedy_rounds, verbose=False,
                        concurrency=concurrency,
                        min_interval=0,   # 本地假站：不需要限速，否则测试要跑几分钟
                        policy=policy or RetryPolicy())
        return Crawler(spider, config, sleep=self.sleep, clock=self.sleep.clock)

    # -- 正常路径 ---------------------------------------------------------
    def test_full_run_writes_all_pages_and_csv(self) -> None:
        self.start(total=35)
        report = self.crawler(end_page=4).run(first=1, last=4)
        self.assertEqual(report.pages_ok, 4)
        self.assertEqual(report.pages_failed, 0)
        self.assertEqual(report.records_written, 35)
        self.assertEqual(len(read_jsonl(Path(self.work_dir) / "records.jsonl")), 35)
        self.assertIsNotNone(report.csv_path)
        self.assertTrue(Path(report.csv_path).exists())
        self.assertEqual(report.cursor, 4)

    def test_empty_last_page_is_success_not_error(self) -> None:
        """最后一页之后是空列表 —— 这是合法结果，不是失败。"""
        self.start(total=20)
        report = self.crawler(end_page=5).run(first=1, last=5)
        self.assertEqual(report.pages_failed, 0)
        self.assertEqual(report.records_written, 20)
        self.assertTrue(report.stopped_early)

    # -- 限流 -------------------------------------------------------------
    def test_rate_limit_backs_off_by_server_hint_and_keeps_going(self) -> None:
        """429 必须原地退避后重试同一页，而不是抛出去。"""
        self.start(total=20, rate_limit_first=2)
        report = self.crawler(end_page=2).run(first=1, last=2)
        self.assertEqual(report.pages_ok, 2, "限流不该让页失败")
        self.assertEqual(report.records_written, 20)
        self.assertEqual(len(self.sleep.waits), 2, "两次 429 各退避一次")
        for wait in self.sleep.waits:
            # 服务端说 1 秒，策略加 1 秒余量
            self.assertAlmostEqual(wait, 2.0, delta=0.01)

    def test_rate_limit_gives_up_after_budget(self) -> None:
        self.start(total=10, rate_limit_first=50)
        policy = RetryPolicy(max_rate_limit_attempts=3, wait_ceiling=5.0)
        report = self.crawler(end_page=1, policy=policy).run(first=1, last=1)
        self.assertEqual(report.pages_ok, 0)
        self.assertEqual(report.failed_pages, [1])
        self.assertEqual(len(self.sleep.waits), 2, "3 次尝试 = 2 次退避")

    # -- 瞬时故障 ---------------------------------------------------------
    def test_transient_5xx_is_retried_then_succeeds(self) -> None:
        self.start(total=30, transient_pages=(2,))
        report = self.crawler(end_page=3, policy=RetryPolicy(max_attempts=3)) \
            .run(first=1, last=3)
        self.assertEqual(report.pages_ok, 3)
        self.assertEqual(report.pages_failed, 0)
        self.assertEqual(report.records_written, 30)
        self.assertEqual(len(self.sleep.waits), 1)

    def test_persistent_5xx_fails_and_reports_page(self) -> None:
        self.start(total=30, fail_pages=(3,))
        report = self.crawler(end_page=4, policy=RetryPolicy(max_attempts=2)) \
            .run(first=1, last=4)
        self.assertEqual(report.pages_failed, 1)
        self.assertEqual(report.failed_pages, [3])
        self.assertEqual(report.records_written, 20,
                         "第 3 页失败只丢它自己那 10 条，前两页照常落盘")

    # -- 断点 -------------------------------------------------------------
    def test_cursor_stops_before_failed_page(self) -> None:
        """第 3 页失败、第 4 页成功时，游标必须停在 2。"""
        self.start(total=40, fail_pages=(3,))
        report = self.crawler(end_page=4, policy=RetryPolicy(max_attempts=2)) \
            .run(first=1, last=4)
        self.assertEqual(report.pages_ok, 3)
        self.assertEqual(report.cursor, 2,
                         "游标越洞会让第 3 页在续采时永久丢失")

    def test_resume_skips_done_pages_and_does_not_duplicate(self) -> None:
        """续采：跳过已完成页，且数据不重复。"""
        self.start(total=40, fail_pages=(3,))
        self.crawler(end_page=4, policy=RetryPolicy(max_attempts=1)).run(
            first=1, last=4)
        # 第二次跑：把假站的故障去掉，模拟"服务端恢复了"
        self.target.fail_pages.clear()
        self.target.reset()
        report = self.crawler(end_page=4, policy=RetryPolicy(max_attempts=1)).run(
            first=1, last=4)
        self.assertEqual(report.pages_skipped, 2, "游标停在 2，所以 1、2 页跳过")
        self.assertEqual(report.pages_ok, 2, "只跑 3、4 两页")
        # 第 3 页这次成功写 10 条；第 4 页上次已写过，这次全是重复
        self.assertEqual(report.records_written, 10)
        self.assertEqual(report.records_duplicates, 10)
        all_records = read_jsonl(Path(self.work_dir) / "records.jsonl")
        self.assertEqual(len(all_records), 40, "续采后总条数正好是全量")
        ids = [r["id"] for r in all_records]
        self.assertEqual(len(ids), len(set(ids)), "不能有重复记录")

    def test_remedy_round_recovers_failed_page(self) -> None:
        """补采轮：失败的页在本轮末尾再试，成功后才推进断点。"""
        self.start(total=40, fail_pages=(3,))
        policy = RetryPolicy(max_attempts=1)
        report = self.crawler(end_page=4, policy=policy, remedy_rounds=1) \
            .run(first=1, last=4)
        # 第一轮第 3 页失败；补采轮里假站仍然失败 -> 最终仍上报
        self.assertEqual(report.failed_pages, [3])
        self.assertEqual(report.cursor, 2)

    def test_remedy_round_succeeds_when_target_recovers(self) -> None:
        self.start(total=40, fail_pages=(3,))
        self.target.fail_pages.clear()      # 补采前服务端恢复
        report = self.crawler(end_page=4, policy=RetryPolicy(max_attempts=1),
                              remedy_rounds=1).run(first=1, last=4)
        self.assertEqual(report.pages_failed, 0)
        self.assertEqual(report.records_written, 40)
        self.assertEqual(report.cursor, 4, "补采成功后断点补上洞")

    def test_stop_early_does_not_request_remaining_pages(self) -> None:
        """判定到底之后，后面的页不该再打请求 —— 白打会被当成翻越边界。"""
        self.start(total=20)
        report = self.crawler(end_page=3).run(first=1, last=10)
        self.assertTrue(report.stopped_early)
        self.assertEqual(report.records_written, 20)
        # 第 1、2 页有数据，第 3 页空 -> 停。第 4~10 页一次都不该发。
        self.assertEqual(sorted(set(self.target.hits)), [1, 2, 3])

    def test_same_crawler_can_run_twice(self) -> None:
        """同一个 Crawler 改范围重跑是常见用法，第二轮不能被上一轮的停止信号卡死。"""
        self.start(total=35)
        crawler = self.crawler(end_page=4)
        first_run = crawler.run(first=1, last=4)
        self.assertEqual(first_run.records_written, 35)
        second_run = crawler.run(first=1, last=4)
        self.assertEqual(second_run.pages_skipped, 4, "第二轮应从断点跳过")
        self.assertEqual(second_run.records_written, 0)
        self.assertEqual(second_run.stopped_early, False)

    def test_checkpoint_identity_mismatch_restarts(self) -> None:
        """换 page_size 会让断点作废 —— 因为页码的含义变了。"""
        self.start(total=40)
        self.crawler(end_page=4).run(first=1, last=4)
        spider = ItemSpider(self.target, 4)
        spider.page_size = 20          # 页码含义变了
        config = Config(work_dir=self.work_dir, verbose=False)
        report = Crawler(spider, config, sleep=self.sleep,
                         clock=self.sleep.clock).run(first=1, last=4)
        self.assertEqual(report.pages_skipped, 0, "身份不符必须从头跑")

    def test_report_file_written(self) -> None:
        self.start(total=20)
        report = self.crawler(end_page=2).run(first=1, last=2)
        payload = read_json(Path(self.work_dir) / "report.json")
        self.assertEqual(payload["pages_ok"], 2)
        self.assertEqual(payload["records_written"], 20)
        self.assertEqual(payload["cursor"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)

# -*- coding: utf-8 -*-
"""并发采集的测试。

并发不是"开个线程池"就完了。它把三件事从"显然"变成"必须证明"：

1. **写记录仍然只发生一次** —— 多线程同时写同一个 JSONL 文件、同时改同一个
   去重集合，最容易出的错是重复行或丢行；
2. **断点仍然只停在洞前面** —— 并发时"哪些页完成了"是一个乱序集合，
   如果谁先完成谁就推进游标，断点会越过失败页，续采时直接漏数据；
3. **限速仍然是全局的** —— 每个 worker 各自 sleep 会叠乘成 N 倍速度，
   这正是并发最容易撞穿限流窗口的地方。

另有一个反直觉的点：本地假站快到 0ms，串行和并发耗时几乎一样，
所以假站必须能人为变慢（``MockTarget(delay=...)``），否则"并发真的省了
网络等待"这件事根本没有被测到。
"""

from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from crawler_kit import (  # noqa: E402
    Config,
    Crawler,
    Outcome,
    RateLimitGate,
    RequestSpec,
    RetryPolicy,
    Spider,
    Verdict,
    read_jsonl,
)
from mock_target import MockTarget  # noqa: E402


class FakeSleep:
    """记录等待时长而不真睡，同时充当虚拟时钟（闸门冷却要靠它到期）。"""

    def __init__(self) -> None:
        self.waits: list[float] = []
        self.now = 0.0

    def __call__(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds

    def clock(self) -> float:
        return self.now


class ItemSpider(Spider):
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


class ConcurrencyTest(unittest.TestCase):
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

    def crawler(self, *, end_page: int, concurrency: int,
                policy: RetryPolicy | None = None, remedy_rounds: int = 0,
                work_dir: str | None = None) -> Crawler:
        assert self.target is not None
        spider = ItemSpider(self.target, end_page)
        config = Config(work_dir=work_dir or self.work_dir, verbose=False,
                        concurrency=concurrency, remedy_rounds=remedy_rounds,
                        min_interval=0,   # 限速另有用例，这里先不掺进来
                        policy=policy or RetryPolicy())
        return Crawler(spider, config, sleep=self.sleep, clock=self.sleep.clock)

    # -- 数据正确性 -------------------------------------------------------
    def test_concurrent_run_writes_every_record_exactly_once(self) -> None:
        """4 个 worker 抢着写，最终仍应是 35 条、无重复、无缺失。"""
        self.start(total=35)
        report = self.crawler(end_page=4, concurrency=4).run(first=1, last=4)
        self.assertEqual(report.pages_ok, 4)
        self.assertEqual(report.records_written, 35)
        self.assertEqual(report.records_duplicates, 0)
        rows = read_jsonl(Path(self.work_dir) / "records.jsonl")
        self.assertEqual(len(rows), 35)
        self.assertEqual(len({r["id"] for r in rows}), 35, "记录重复了")
        self.assertEqual(sorted(r["id"] for r in rows), list(range(1, 36)), "记录缺失")

    def test_concurrent_run_advances_cursor_over_all_completed_pages(self) -> None:
        """全部成功时，乱序完成也必须把游标推到最后一页。"""
        self.start(total=35)
        report = self.crawler(end_page=4, concurrency=4).run(first=1, last=4)
        self.assertEqual(report.cursor, 4)

    def test_concurrent_run_stops_cursor_before_failed_page(self) -> None:
        """第 3 页失败、其余成功：游标必须停在 2，不许被第 4 页带过去。"""
        self.start(total=40, fail_pages=(3,))
        report = self.crawler(end_page=4, concurrency=4,
                              policy=RetryPolicy(max_attempts=1)).run(first=1, last=4)
        self.assertEqual(report.pages_ok, 3)
        self.assertEqual(report.pages_failed, 1)
        self.assertEqual(report.cursor, 2, "断点越过了失败的页")
        self.assertEqual(report.failed_pages, [3])

    def test_concurrent_run_resume_after_hole_does_not_duplicate(self) -> None:
        """补上洞之后续采：已写过的页只算重复，不重复落盘。"""
        self.start(total=40, fail_pages=(3,))
        self.crawler(end_page=4, concurrency=4,
                     policy=RetryPolicy(max_attempts=1)).run(first=1, last=4)
        self.target.fail_pages.clear()
        report = self.crawler(end_page=4, concurrency=4).run(first=1, last=4)
        self.assertEqual(report.pages_skipped, 2, "游标 2 之前的页应被跳过")
        self.assertEqual(report.records_written, 10, "只该补第 3 页的 10 条")
        self.assertEqual(report.cursor, 4)
        rows = read_jsonl(Path(self.work_dir) / "records.jsonl")
        self.assertEqual(len({r["id"] for r in rows}), len(rows), "续采写出了重复行")

    def test_concurrent_run_retries_transient_page(self) -> None:
        """并发下单个页的 503 仍走重试，不影响别的页。"""
        self.start(total=40, transient_pages=(2,))
        report = self.crawler(end_page=4, concurrency=4).run(first=1, last=4)
        self.assertEqual(report.pages_ok, 4)
        self.assertEqual(report.records_written, 40)

    def test_remedy_round_works_under_concurrency(self) -> None:
        """并发 + 补采：第一轮第 3 页失败，第二轮只补它。"""
        self.start(total=40, fail_pages=(3,))
        report = self.crawler(end_page=4, concurrency=4, remedy_rounds=1,
                              policy=RetryPolicy(max_attempts=1)).run(first=1, last=4)
        # 页一直不恢复：两轮各失败一次，所以计数是 2 而不是 1。
        # 计数记的是"失败了几次尝试轮"，不是"有几页失败"——后者看 failed_pages。
        self.assertEqual(report.pages_failed, 2)
        self.assertEqual(report.failed_pages, [3], "失败的始终只有第 3 页")
        self.assertEqual(report.cursor, 2, "洞没补上，游标不许动")
        self.assertEqual(report.records_written, 30, "只有第 3 页的 10 条没写进去")

    # -- 并发真的省时间 ---------------------------------------------------
    def test_concurrency_actually_overlaps_network_wait(self) -> None:
        """4 页各慢 0.3 秒：串行约 1.2 秒，4 并发应在 0.4 秒上下。

        阈值留 3 倍余量（0.9s），因为这条是唯一量真实墙钟的用例：机器被
        别的进程占满（CI、并行测试）时，串行/并发的差距会被系统调度吃掉，
        阈值卡太紧会随机红——守的是"并发有没有生效"，不是"机器快不快"。
        """
        self.start(total=40, delay=0.3)
        start = time.monotonic()
        report = self.crawler(end_page=4, concurrency=4).run(first=1, last=4)
        elapsed = time.monotonic() - start
        self.assertEqual(report.records_written, 40)
        self.assertLess(elapsed, 0.9, f"并发没起作用，耗时 {elapsed:.2f}s（串行约 1.2s）")

    def test_concurrency_one_still_serial(self) -> None:
        """concurrency=1 是默认值，行为必须和以前一模一样。"""
        self.start(total=40, delay=0.1)
        report = self.crawler(end_page=4, concurrency=1).run(first=1, last=4)
        self.assertEqual(report.records_written, 40)
        self.assertEqual(report.cursor, 4)

    def test_concurrency_does_not_break_rate_limit_globality(self) -> None:
        """4 并发 + 固定 0.1 秒间隔：总耗时必须仍按间隔累加（全局限速）。

        如果限速器不是全局的，4 个 worker 会各睡各的，总耗时约等于串行，
        这个断言就会失败 —— 它守的是"限速一旦不是全局就等于没限速"。
        """
        self.start(total=40)
        spider = ItemSpider(self.target, 4)
        config = Config(work_dir=self.work_dir, verbose=False, concurrency=4,
                        min_interval=0.1, remedy_rounds=0)
        start = time.monotonic()
        report = Crawler(spider, config, sleep=self.sleep,
                         clock=self.sleep.clock).run(first=1, last=4)
        elapsed = time.monotonic() - start
        self.assertEqual(report.records_written, 40)
        # FixedIntervalLimiter 内部用的是真实 time.sleep（不走注入的 sleep），
        # 所以这里只能量真实耗时：4 页 = 首次不等 + 3 次 0.1 秒。
        # 若限速器被每个 worker 各自持有，3 次等待会互相重叠，耗时掉到约 0.1 秒。
        self.assertGreaterEqual(elapsed, 0.28,
                                f"限速不是全局的，耗时只有 {elapsed:.2f}s")


class RiskAbort(RuntimeError):
    pass


class AbortSpider(Spider):
    """模拟"整轮必须停"的站点（淘宝风控就是这种）。

    它不返回判决，而是**抛异常** —— 页级失败和整轮中止是两件事：
    页级失败由 ``_run_page`` 自己降级成 ``PageStat(failed)``，
    能一路冒到 ``run()`` 的异常只可能是插件主动喊停。
    """

    name = "abort"
    page_size = 1

    def __init__(self, target: MockTarget):
        self.target = target

    def request(self, page: int) -> RequestSpec:
        return RequestSpec(url=f"{self.target.url()}?page={page}")

    def classify(self, page: int, response) -> None:
        raise RiskAbort(f"page {page} 命中风控")


class FatalsSpider(ItemSpider):
    """指定页返回确定性 403，被判成 FATAL。"""

    def __init__(self, target: MockTarget, fatal_pages: tuple[int, ...]):
        super().__init__(target, end_page=len(fatal_pages))
        self.fatal_pages = fatal_pages

    def classify(self, page: int, response) -> Outcome | None:
        if page in self.fatal_pages:
            return Outcome(Verdict.FATAL, error=f"HTTP 403（page {page}）")
        return super().classify(page, response)


class AbortTest(unittest.TestCase):
    """中止路径：异常必须传出去，且报告必须留下。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.work_dir = self._tmp.name
        self.sleep = FakeSleep()
        self.target = MockTarget(total=40).start()

    def tearDown(self) -> None:
        self.target.stop()
        self._tmp.cleanup()

    def crawler(self, *, concurrency: int, work_dir: str | None = None) -> Crawler:
        config = Config(work_dir=work_dir or self.work_dir, verbose=False,
                        concurrency=concurrency, remedy_rounds=2, min_interval=0)
        return Crawler(AbortSpider(self.target), config,
                       sleep=self.sleep, clock=self.sleep.clock)

    def test_abort_propagates_in_sequential_mode(self) -> None:
        crawler = self.crawler(concurrency=1)
        with self.assertRaises(RiskAbort):
            crawler.run(first=1, last=4)
        self.assertEqual(len(self.target.hits), 1, "中止后不该继续请求")

    def test_abort_propagates_in_concurrent_mode(self) -> None:
        """并发时也一样：早先这里被吞成 failed，风控被无视继续打完全部页。"""
        crawler = self.crawler(concurrency=3)
        with self.assertRaises(RiskAbort):
            crawler.run(first=1, last=8)
        # 取数改成按需提交后，在最飞的只有 workers 个，abort 之后不再补位，
        # 所以命中数最多 3；留一页余量是给「abort 前已提交完」的边界情况。
        self.assertLessEqual(len(self.target.hits), 4,
                             "中止后仍把剩余页打完了")

    def test_abort_still_writes_report(self) -> None:
        """中止是正常退出路径，report.json 必须留下来回答"采了多少"。"""
        crawler = self.crawler(concurrency=1)
        with self.assertRaises(RiskAbort):
            crawler.run(first=1, last=4)
        report_path = Path(self.work_dir) / "report.json"
        self.assertTrue(report_path.exists(), "中止后丢了 report.json")
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["failed_pages"], [1], "中止的那一页应记进报告")

    def test_abort_page_is_recorded_in_both_modes(self) -> None:
        """两种模式都要把中止页记进报告（并发时在飞的页也会记，属正常）。"""
        for concurrency in (1, 3):
            with tempfile.TemporaryDirectory() as tmp:
                crawler = self.crawler(concurrency=concurrency, work_dir=tmp)
                with self.assertRaises(RiskAbort):
                    crawler.run(first=1, last=2)
                payload = json.loads(
                    (Path(tmp) / "report.json").read_text(encoding="utf-8"))
                self.assertIn(1, payload["failed_pages"],
                              f"concurrency={concurrency} 没记中止页")
                self.assertTrue(all(d["error"].startswith("RiskAbort")
                                    for d in payload["failed_detail"]),
                                f"concurrency={concurrency} 的失败原因不对")


class RemedyTest(unittest.TestCase):
    """补采只该重跑"运气不好"的页，不该重跑"对方明确拒绝"的页。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.work_dir = self._tmp.name
        self.sleep = FakeSleep()
        self.target = MockTarget(total=40).start()

    def tearDown(self) -> None:
        self.target.stop()
        self._tmp.cleanup()

    def test_fatal_pages_are_not_retried(self) -> None:
        """确定性 403 + 3 轮补采，仍应只打 2 次（每页一次）。"""
        spider = FatalsSpider(self.target, fatal_pages=(1, 2))
        config = Config(work_dir=self.work_dir, verbose=False, remedy_rounds=3,
                        min_interval=0)
        report = Crawler(spider, config, sleep=self.sleep,
                         clock=self.sleep.clock).run(first=1, last=2)
        self.assertEqual(len(self.target.hits), 2, "FATAL 页被补采轮重打了")
        self.assertEqual(report.pages_failed, 2)
        self.assertEqual(report.failed_pages, [1, 2], "FATAL 页仍要出现在报告里")
        self.assertTrue(all(s.fatal for s in report.stats
                            if s.status == "failed"))

    def test_retryable_pages_still_get_remedy(self) -> None:
        """对照：瞬时故障页照旧享受补采。"""
        self.target.transient_pages = {1}
        spider = ItemSpider(self.target, end_page=2)
        config = Config(work_dir=self.work_dir, verbose=False, remedy_rounds=1,
                        min_interval=0, policy=RetryPolicy(max_attempts=1))
        report = Crawler(spider, config, sleep=self.sleep,
                         clock=self.sleep.clock).run(first=1, last=2)
        self.assertEqual(report.pages_ok, 2, "瞬时故障页应该在补采轮恢复")
        self.assertEqual(report.failed_pages, [])


class RateLimitGateTest(unittest.TestCase):
    """闸门的语义：只延长不缩短、冷却期全组一起等。"""

    def test_block_for_only_extends(self) -> None:
        gate = RateLimitGate()
        gate.block_for(5.0)
        gate.block_for(1.0)          # 更短的要求不能缩短已生效的冷却
        self.assertGreater(gate.wait(), 3.0, "冷却窗口被缩短了")

    def test_wait_returns_zero_when_not_cooling(self) -> None:
        gate = RateLimitGate()
        self.assertEqual(gate.wait(), 0.0)

    def test_block_for_zero_is_noop(self) -> None:
        gate = RateLimitGate()
        gate.block_for(0)
        gate.block_for(-1)
        self.assertEqual(gate.wait(), 0.0)

    def test_virtual_clock_makes_window_expire(self) -> None:
        """注入假时钟：睡过一次之后窗口到期，不会凭空再睡。"""
        clock = [0.0]

        def sleep(seconds: float) -> None:
            clock[0] += seconds

        gate = RateLimitGate(sleep=sleep, clock=lambda: clock[0])
        gate.block_for(3.0)
        self.assertAlmostEqual(gate.wait(), 3.0)
        self.assertEqual(gate.wait(), 0.0, "窗口到期后不该再等")


if __name__ == "__main__":
    unittest.main()

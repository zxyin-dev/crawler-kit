# -*- coding: utf-8 -*-
"""编排层：把限速、退避、断点、去重、落盘、报告串成一次采集。

这是全库唯一"知道全局"的地方。业务插件只回答"这一页怎么请求、怎么解析"，
传输层只回答"把请求变成回答"，剩下的都在这里。

五个决定，每一个都对着一次真实故障：

1. **并发只用来掩盖网络等待，落盘与断点永远串行。**
   并发翻页会让"哪些页完成了"变成一个不确定的集合，断点就没法只前进到
   连续完成的位置 —— 你要么丢页，要么重采。所以并发只开在"取数"这一侧，
   写记录、累加统计、推进断点全部收进 :meth:`Crawler._commit` 的一把锁里。
   默认 ``concurrency=1``（顺序）；加速是显式动作，代价是断点更容易被
   一个慢页钉住。

2. **限速器由编排层持有，全局一个。**
   worker 数量、重试次数、翻页顺序都不改变它。改速率只改一个对象。
   并发时它必须是线程安全的（本库的实现都带锁），否则 N 个 worker
   各自 sleep 会叠乘成 N 倍速度。

3. **重试只在这里发生。**
   传输层不重试、插件不重试。429 一律原地退避（等够时间再发同一个请求），
   绝不抛给上层 —— 抛出去就会和别处的重试叠成连点。
   并发时还要额外做一件事：拿到 429 的 worker 调 ``gate.block_for``，
   让**全组**一起停。否则其余 worker 会把退避窗口反复撞穿。

4. **断点只推进到"连续完成"的位置。**
   第 3 页失败、第 4 页成功时，断点不能推到 4。否则下次续采会从 5 开始，
   第 3 页永久丢失。失败的页进补采队列，本轮末尾重试。

5. **先落盘再推断点。**
   顺序反了会得到"断点说完成了、数据却没写进去"，而这类错误只在续采时暴露，
   且无法区分是漏采还是本来没数据。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from .checkpoint import Checkpoint
from .limiter import Limiter, RateLimitGate, make_limiter
from .log import get_logger
from .retry import Attempt, RetryPolicy, RetryReport, RetryRunner, Verdict, parse_retry_after
from .signer import Signer
from .spider import Outcome, RequestSpec, Spider
from .store import JsonlDedupeStore, read_jsonl, safe_key, write_csv
from .transport import HttpTransport, Response, Transport, TransportError

__all__ = ["Config", "Crawler", "RunReport", "PageStat"]

log = get_logger("crawler_kit.crawler")


@dataclass
class Config:
    """一次采集的全部配置。

    默认值都是"保守且正确"的：限速偏慢、重试次数少、落盘即刷盘。
    想加速就显式改 ``min_interval`` 或令牌桶参数 —— 让"加速"成为一个
    看得见的动作，而不是默认行为。
    """

    work_dir: str | Path = "data"
    # 限速：显式给 min_interval 就用固定间隔，否则用令牌桶
    min_interval: float | None = None
    capacity: float = 2.0
    refill_per_sec: float = 1.0 / 6.0
    # 重试
    policy: RetryPolicy = field(default_factory=RetryPolicy)
    # 补采：本轮失败的页在末尾再试几轮。
    # 只重跑**可重试**的页；被判死（FATAL）的页不参加 —— 再打一次是纯损失。
    remedy_rounds: int = 2
    # 并发：同时在飞的页数。1 = 顺序（默认）。
    # 为什么默认是 1：并发会把"哪些页完成"变成不确定集合，而断点只能安全地
    # 前进到"连续完成"的位置 —— 并发越大，一个慢页/失败页越容易把游标钉住，
    # 续采时要重跑的页就越多。所以加速是一个显式动作，不是默认行为。
    concurrency: int = 1
    # 文件
    jsonl_name: str = "records.jsonl"
    checkpoint_name: str = "progress.json"
    report_name: str = "report.json"
    # 行为开关
    resume: bool = True           # 有断点就续采
    dedupe: bool = True           # 按去重键过滤重复记录
    export_csv: bool = True       # 结束时整体重写 CSV
    verbose: bool = True
    # 身份：参与断点校验；只放"改变页码含义"的东西
    identity: dict[str, Any] = field(default_factory=dict)
    info: dict[str, Any] = field(default_factory=dict)

    def path(self, name: str) -> Path:
        return Path(self.work_dir) / name

    @property
    def jsonl_path(self) -> Path:
        return self.path(self.jsonl_name)

    @property
    def checkpoint_path(self) -> Path:
        return self.path(self.checkpoint_name)


@dataclass
class PageStat:
    """单页的执行结果。"""

    page: int
    status: str            # "ok" / "failed" / "skipped" / "stopped"
    records: int = 0
    duplicates: int = 0
    attempts: int = 0
    waited: float = 0.0
    rate_limited: int = 0
    error: str | None = None
    # FATAL 表示"这页别再打了"（403 拒绝、接口改版、风控）。
    # 它和"重试额度用完"（RETRYABLE）不是一回事：前者再打是纯损失 ——
    # 对风控站点甚至会把处罚升级，所以补采轮必须排除它。
    fatal: bool = False


@dataclass
class RunReport:
    """一次采集的汇总。"""

    spider: str
    pages_requested: int = 0
    pages_ok: int = 0
    pages_failed: int = 0
    pages_skipped: int = 0
    records_written: int = 0
    records_duplicates: int = 0
    records_total: int = 0
    attempts: int = 0
    rate_limited: int = 0
    waited: float = 0.0
    elapsed: float = 0.0
    cursor: int | None = None
    stopped_early: bool = False
    csv_path: str | None = None
    failed_pages: list[int] = field(default_factory=list)
    stats: list[PageStat] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "spider": self.spider,
            "pages_requested": self.pages_requested,
            "pages_ok": self.pages_ok,
            "pages_failed": self.pages_failed,
            "pages_skipped": self.pages_skipped,
            "records_written": self.records_written,
            "records_duplicates": self.records_duplicates,
            "records_total": self.records_total,
            "attempts": self.attempts,
            "rate_limited": self.rate_limited,
            "waited": round(self.waited, 3),
            "elapsed": round(self.elapsed, 3),
            "cursor": self.cursor,
            "stopped_early": self.stopped_early,
            "csv_path": self.csv_path,
            "failed_pages": self.failed_pages,
        }

    def summary(self) -> str:
        """一行人类可读的结论（打印在采集结束处）。"""
        parts = [
            f"页 {self.pages_ok}/{self.pages_requested} 成功",
            f"写入 {self.records_written} 条",
            f"重复 {self.records_duplicates} 条",
            f"尝试 {self.attempts} 次",
            f"等待 {self.waited:.1f}s",
            f"耗时 {self.elapsed:.1f}s",
        ]
        if self.pages_failed:
            parts.append(f"失败页 {len(self.failed_pages)} 个：{self.failed_pages}")
        if self.pages_skipped:
            parts.append(f"跳过 {self.pages_skipped} 页（断点）")
        if self.stopped_early:
            parts.append("提前停止")
        return " | ".join(parts)


class Crawler:
    """把一次采集跑完。

    用法::

        Crawler(spider, Config(work_dir="data")).run()

    参数
    ----
    spider:
        业务插件。
    config:
        配置。
    transport:
        传输实现；默认 :class:`HttpTransport`（零依赖）。
    signer:
        签名插件；``None`` 表示不需要签名。
    key_func:
        去重键；``None`` 表示整条记录作为键。
    """

    def __init__(self, spider: Spider, config: Config | None = None, *,
                 transport: Transport | None = None,
                 signer: Signer | None = None,
                 limiter: Limiter | None = None,
                 key_func: Callable[[dict[str, Any]], str] | None = None,
                 sleep: Callable[[float], None] | None = None,
                 clock: Callable[[], float] | None = None):
        self.spider = spider
        self.config = config or Config()
        self.transport = transport or HttpTransport()
        self.signer = signer
        # 可注入的等待函数。测试用它替换真实 sleep，于是"被限流时是否按
        # 服务端说的秒数退避"从一件只能靠观察的事变成可断言的事实。
        self.sleep = sleep or time.sleep
        # 可注入的时钟。sleep 换成假的之后真实时间不再前进，闸门截止点会永远
        # 停在"未来"，凭空多睡一次；配套假时钟让虚拟时间真的走动。
        self.clock = clock or time.monotonic
        self.limiter = limiter or make_limiter(
            min_interval=self.config.min_interval,
            capacity=self.config.capacity,
            refill_per_sec=self.config.refill_per_sec,
        )
        self.key_func = key_func or (lambda record: safe_key(
            *[record.get(k) for k in _default_keys(record)]))
        self.report = RunReport(spider=spider.name)
        self._store: Any = None
        self._completed: set[int] = set()
        # 全局冷却闸门：一个 worker 被限流，全部 worker 一起停
        self.gate = RateLimitGate(sleep=self.sleep, clock=self.clock)
        # 并发时唯一的写点（写记录 + 累加统计 + 推进断点都在锁内）
        self._commit_lock = threading.Lock()
        # 提前停止：一页判定要停，其余在飞/未跑的页不再开始
        self._stop_event = threading.Event()
        # 被判死（FATAL）的页：进报告，但不参加补采
        self._fatal_pages: set[int] = set()

    # -- 主流程 -----------------------------------------------------------
    def run(self, *, first: int | None = None, last: int | None = None) -> RunReport:
        """跑完一次采集。``first`` / ``last`` 省略时取插件的默认范围。"""
        started = time.monotonic()
        first = self.spider.start_page if first is None else first
        last = self._resolve_last(first, last)

        # 同一个 Crawler 允许跑第二次（改范围重跑是常见用法），所以每轮开头
        # 必须把上一轮留下的停止信号和统计清掉 —— 否则第二轮一页都不会跑。
        self._stop_event.clear()
        self.report = RunReport(spider=self.spider.name)

        config = self.config
        Path(config.work_dir).mkdir(parents=True, exist_ok=True)

        checkpoint = Checkpoint(
            config.checkpoint_path,
            identity={**self.spider.signature(), **config.identity},
            info={"spider": self.spider.name, "first": first, "last": last,
                  **config.info},
        )
        resume_from = checkpoint.load() if config.resume else None

        pages = self.spider.pages(first, last)
        self.report.pages_requested = len(pages)

        # 断点语义是"游标之前的页都完成了"，所以这里过滤的是页码而非下标
        todo: list[int] = []
        for page in pages:
            if resume_from is not None and page <= resume_from:
                self.report.pages_skipped += 1
                self.report.stats.append(PageStat(page, "skipped"))
                continue
            todo.append(page)

        if self.report.pages_skipped and config.verbose:
            log.info("从断点续采：跳过 {} 页（游标 {}）", self.report.pages_skipped,
                     resume_from)

        store = JsonlDedupeStore(config.jsonl_path, self.key_func) \
            if config.dedupe else _NoDedupeStore(config.jsonl_path, self.key_func)
        self._store = store

        # 游标基线：有断点就从断点起，否则从"第一页之前"起
        self._cursor_base = resume_from if resume_from is not None else first - 1
        self._completed = set(range(first, self._cursor_base + 1))

        failed: list[int] = []
        # 被判定为 FATAL 的页单独记着：它们会出现在报告里，但**不参加补采**。
        # 补采只该重跑"这次运气不好"的页，不该重跑"对方明确拒绝"的页。
        self._fatal_pages = set()
        try:
            self._run_pages(todo, checkpoint, store, failed)
            # 补采：本轮失败的页在末尾重试，轮数有上限
            for round_no in range(1, config.remedy_rounds + 1):
                pending = [page for page in failed
                           if page not in self._fatal_pages]
                if not pending:
                    break
                if config.verbose:
                    log.warning("补采第 {}/{} 轮：{} 页", round_no,
                                config.remedy_rounds, pending)
                    if len(pending) != len(failed):
                        log.warning("{} 页已判死（FATAL），不再重试：{}",
                                    len(failed) - len(pending),
                                    sorted(self._fatal_pages))
                failed = []
                self._run_pages(pending, checkpoint, store, failed)
        finally:
            store.close()
            # 收尾必须在 finally 里：整轮中止（风控、退出码 3）是一条**正常**
            # 的退出路径，而 report.json 是唯一能回答"这一轮到底采了多少"的文件。
            # 放在 try 之后会被异常跳过，最后只剩一个 records.jsonl。
            self._finish(started, checkpoint, failed)

        if config.verbose:
            log.info("采集结束：{}", self.report.summary())
            if self.report.failed_pages:
                log.error("以下页仍未成功，数据不完整：{}", self.report.failed_pages)
        return self.report

    def _finish(self, started: float, checkpoint: Checkpoint,
                failed: list[int]) -> None:
        """汇总报告。异常传播路径上也会被调用，所以自身不能再抛。"""
        # 补采轮会把 failed 清空重填，所以 FATAL 页要单独并回来，
        # 否则"被拒绝的页"会从报告里消失。
        self.report.failed_pages = sorted(set(failed) | self._fatal_pages)
        self.report.cursor = checkpoint.load()
        self.report.elapsed = time.monotonic() - started
        try:
            if self.config.export_csv:
                self.report.csv_path = self._export_csv()
            self._write_report()
        except Exception as exc:   # 收尾失败不该盖掉真正的异常
            log.error("收尾写报告失败：{}: {}", type(exc).__name__, exc)

    def _resolve_last(self, first: int, last: int | None) -> int:
        if last is not None:
            return last
        # 插件可以给 last_page()（方法）或 end_page（属性）。
        # 淘宝详情侧就是靠它"采完就停"，所以这是正式契约，不是鸭子类型巧合。
        hook = getattr(self.spider, "last_page", None)
        if callable(hook):
            value = hook()
            if value is not None:
                return int(value)
        configured = getattr(self.spider, "end_page", None)
        return int(configured) if configured else first

    # -- 翻页 -------------------------------------------------------------
    def _run_pages(self, pages: Sequence[int], checkpoint: Checkpoint,
                   store: Any, failed: list[int]) -> None:
        """跑一序列页；成功的页推进断点，失败的页记入 ``failed``。

        ``concurrency == 1`` 时就是顺序循环；大于 1 时开线程池并发**取数**，
        但**落盘与断点推进始终串行**（见 :meth:`_commit`）。这条分界线是刻意的：
        并发只用来掩盖网络等待，不用来并发写文件。
        """
        workers = max(1, int(self.config.concurrency))
        if workers == 1 or len(pages) <= 1:
            for page in pages:
                # 顺序路径同样要认停止信号：一页判定"到底了"之后再跑后面的页
                # 只是白打请求（而且会被服务端当成翻越边界）。
                if self._stop_event.is_set():
                    if self.config.verbose:
                        log.info("已判定到底，剩余 {} 页不再请求",
                                 len(pages) - pages.index(page))
                    break
                self._run_one(page, checkpoint, store, failed)
            return
        self._run_pages_concurrent(pages, checkpoint, store, failed, workers)

    def _run_one(self, page: int, checkpoint: Checkpoint, store: Any,
                 failed: list[int]) -> None:
        try:
            stat, outcome = self._run_page(page)
        except BaseException as exc:
            # 顺序路径也要把中止页记进报告再往外抛，否则"这一轮到底卡在哪页"
            # 只能从日志里猜，而并发路径是把这一页记成 failed 的 —— 两条路径
            # 的记账方式必须一致，不然同一个插件换个 concurrency 报告就变了。
            self._commit(PageStat(page, "failed",
                                  error=f"{type(exc).__name__}: {exc}",
                                  fatal=True), None, checkpoint, store, failed)
            raise
        self._commit(stat, outcome, checkpoint, store, failed)

    def _run_pages_concurrent(self, pages: Sequence[int], checkpoint: Checkpoint,
                              store: Any, failed: list[int], workers: int) -> None:
        from collections import deque
        from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

        if self.config.verbose:
            log.info("并发采集：{} 个 worker，共 {} 页", workers, len(pages))
        # 用 with 保证异常时线程池被收回；shutdown(wait=True) 是默认，
        # 意味着中途抛异常也会等所有在飞请求收尾 —— 这是对的：
        # 否则连接被硬切断，服务端看到的是一堆半开的请求。
        abort: BaseException | None = None
        with ThreadPoolExecutor(max_workers=workers,
                                thread_name_prefix="crawler") as pool:
            # 只提交「当前空出来的 worker 数」，不预先排满整个队列。
            # 早先是先把全部页 submit 进队列、中止时再 cancel()，可等主线程看到
            # abort 时，空闲 worker 已经从队列里又领走一页 —— 并发 3 时实测
            # 会多打 1-2 页，风控场景下这些就是白送的违规次数，而且多运一页
            # 就要多一次真实请求，测试也会偶发红。按需提交把这个窗口直接消掉：
            # abort 之后主线程不再提交，在飞的至多 workers 页。
            remaining = deque(pages)
            inflight: dict[Any, int] = {}
            while remaining or inflight:
                while (remaining and len(inflight) < workers
                       and abort is None and not self._stop_event.is_set()):
                    page = remaining.popleft()
                    inflight[pool.submit(self._run_page, page)] = page
                if not inflight:
                    break
                done, _ = wait(inflight, return_when=FIRST_COMPLETED)
                for future in done:
                    page = inflight.pop(future)
                    try:
                        stat, outcome = future.result()
                    except BaseException as exc:
                        # 单页崩掉不该拖垮整轮 —— 但要看崩的是哪种。
                        # _run_page 内部已经把"传输失败""解析失败"降级成页级失败，
                        # 所以能冒到这里的是插件自己抛的**中止信号**（淘宝风控就是这么
                        # 做的：classify 抛 RiskControlError，穿透到 CLI 变成退出码 3）。
                        # 早先这里一律吞成 failed，结果并发模式下风控被吞掉：继续把
                        # 剩下所有页打完，白送违规次数。所以记下来，等 pool 收尾后再抛。
                        abort = exc
                        stat, outcome = (PageStat(page, "failed",
                                                  error=f"{type(exc).__name__}: {exc}",
                                                  fatal=True),
                                         None)
                    self._commit(stat, outcome, checkpoint, store, failed)
        if abort is not None:
            raise abort

    def _commit(self, stat: PageStat, outcome: Outcome | None,
                checkpoint: Checkpoint, store: Any, failed: list[int]) -> None:
        """把一页的结果并入全局状态。**并发时这里是唯一写点，必须持锁。**

        三件事都在同一把锁里：写记录、累加统计、推进断点。
        分开加锁会出现"记录已落盘但统计没加""游标推进了但记录还没写"这类
        中间状态，而这些中间状态在并发下会被另一个 worker 读到。
        """
        with self._commit_lock:
            self.report.stats.append(stat)
            self.report.attempts += stat.attempts
            self.report.waited += stat.waited
            self.report.rate_limited += stat.rate_limited

            if stat.status in ("ok", "stopped"):
                self.report.pages_ok += 1
                counts = self._store.add(outcome.records if outcome else [])
                self.report.records_written += counts["written"]
                self.report.records_duplicates += counts["duplicates"]
                self.report.records_total += counts["written"] + counts["duplicates"]
                stat.records = counts["written"]
                stat.duplicates = counts["duplicates"]
                self._completed.add(stat.page)
                if stat.status == "stopped":
                    self.report.stopped_early = True
                # 断点只能推到"从当前位置连续完成"的最后一页。
                # 第 3 页失败、第 4 页成功时，游标停在 2 —— 否则续采会从 5 开始，
                # 第 3 页永久丢失。
                self._advance_cursor(checkpoint)
            elif stat.status == "failed":
                self.report.pages_failed += 1
                if stat.fatal:
                    self._fatal_pages.add(stat.page)
                failed.append(stat.page)

    def _advance_cursor(self, checkpoint: Checkpoint) -> None:
        """把游标推到连续完成区间的末尾。调用方必须持 ``_commit_lock``。"""
        cursor = self._cursor_base
        while cursor + 1 in self._completed:
            cursor += 1
        if cursor > self._cursor_base:
            self._cursor_base = cursor
            checkpoint.save(cursor)

    def _run_page(self, page: int) -> tuple[PageStat, Outcome | None]:
        """跑一页：限速 → 等冷却 → 签名 → 请求 → 判决 → 解析。

        **不落盘**：落盘由 :meth:`_commit` 在锁内完成，这样并发时写文件
        仍然只有一个线程在写。返回的 ``Outcome`` 里带着待写入的记录。
        """
        attempts = 0
        waited = 0.0
        rate_limited = 0
        stop_early = False
        # 自己刚被限流的页，本轮后续尝试不再过闸门：它已经按服务端提示退避过，
        # 再等一遍闸门就是同一个等待睡两次（实测表现为 waited 恰好翻倍）。
        # 闸门的职责是拦住**别的** worker，不是重复惩罚肇事者。
        penalized = False

        def attempt(n: int) -> Attempt:
            nonlocal attempts, waited, rate_limited, stop_early, penalized
            attempts = n
            if not penalized:
                waited += self.gate.wait()
            waited += self.limiter.acquire()

            spec = self.spider.request(page)
            if self.signer is not None:
                spec = self.signer.sign(spec)

            try:
                response = self.transport.fetch(spec)
            except TransportError as exc:
                return Attempt(Verdict.RETRYABLE, error=f"传输失败：{exc}")

            verdict = self.spider.classify(page, response)
            if verdict is not None:
                if verdict.verdict is Verdict.RATE_LIMITED:
                    rate_limited += 1
                    penalized = True
                    if verdict.wait is None:
                        payload = response.json()
                        verdict.wait = parse_retry_after(payload, response.headers)
                    # 被限流 = 全组都得停，不只是我这一页
                    if verdict.wait:
                        self.gate.block_for(verdict.wait)
                return verdict.attempt()

            try:
                outcome = self.spider.parse(page, response)
            except Exception as exc:   # 解析崩了通常是接口改版：FATAL，别重试
                return Attempt(Verdict.FATAL,
                               error=f"解析失败：{type(exc).__name__}: {exc}")

            if outcome.verdict is Verdict.OK and self.spider.stop_at(page, outcome):
                stop_early = True
                self._stop_event.set()
            return outcome.attempt()

        runner = RetryRunner(self.config.policy, sleep=self.sleep,
                             on_event=self._on_event)
        result = runner.run(attempt)
        waited += result.wait_seconds
        rate_limited += result.rate_limited

        if not result.ok:
            return (PageStat(page, "failed", attempts=result.attempts,
                             waited=waited, rate_limited=rate_limited,
                             error=result.error,
                             fatal=result.outcome.verdict is Verdict.FATAL),
                    None)

        outcome: Outcome = result.payload
        return (PageStat(page, "stopped" if stop_early else "ok",
                         attempts=result.attempts, waited=waited,
                         rate_limited=rate_limited), outcome)

    def _on_event(self, kind: str, message: str, wait: float) -> None:
        if not self.config.verbose:
            return
        if kind in ("rate_limited", "retryable"):
            log.warning("{}（等 {:.1f}s）：{}", kind, wait, message)
        elif kind in ("fatal", "gave_up"):
            log.error("{}：{}", kind, message)
        else:
            log.debug("{}：{}", kind, message)

    # -- 收尾 -------------------------------------------------------------
    def _export_csv(self) -> str | None:
        """从 JSONL 整体重写 CSV。输出是输入的纯函数。"""
        records = read_jsonl(self.config.jsonl_path)
        if not records:
            return None
        fieldnames = _union_fields(records)
        path = self.config.path("records.csv")
        rows = _flatten(records)
        write_csv(path, rows, fieldnames)
        return str(path)

    def _write_report(self) -> None:
        from .checkpoint import atomic_write_json
        payload = self.report.as_dict()
        payload["failed_detail"] = [
            {"page": s.page, "error": s.error, "attempts": s.attempts}
            for s in self.report.stats if s.status == "failed"
        ]
        atomic_write_json(self.config.path(self.config.report_name), payload)


class _NoDedupeStore:
    """关掉去重时的写入器：只追加、不比对。"""

    def __init__(self, path: str | Path, key_func: Callable[[dict[str, Any]], str]):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.key_func = key_func
        self._fh = self.path.open("a", encoding="utf-8")

    def add(self, records: Iterable[dict[str, Any]]) -> dict[str, int]:
        import json
        import os
        written = 0
        for record in records:
            self._fh.write(json.dumps(dict(record), ensure_ascii=False) + "\n")
            written += 1
        if written:
            self._fh.flush()
            os.fsync(self._fh.fileno())
        return {"written": written, "duplicates": 0}

    def close(self) -> None:
        if not self._fh.closed:
            self._fh.close()


def _default_keys(record: dict[str, Any]) -> list[str]:
    """猜一个去重键：优先常见主键字段，否则整条记录。"""
    for name in ("id", "item_id", "sku", "code", "url", "link", "uid"):
        if name in record and record[name] is not None:
            return [name]
    return sorted(record.keys())


def _union_fields(records: Sequence[dict[str, Any]]) -> list[str]:
    """所有记录字段的并集（保持首次出现顺序）。"""
    fields: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in record:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    return fields


def _flatten(records: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """嵌套的 dict/list 压成 JSON 字符串，好让 CSV 是一张规整的表。"""
    import json
    rows = []
    for record in records:
        row = {}
        for key, value in record.items():
            if isinstance(value, (dict, list)):
                row[key] = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, bool):
                row[key] = int(value)
            else:
                row[key] = value
        rows.append(row)
    return rows

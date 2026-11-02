# -*- coding: utf-8 -*-
"""退避与重试。

这个模块存在的唯一理由，是把一条踩过坑的规则固化下来：

    429（限流）必须**原地退避后重试同一个请求**，绝不能抛成异常交给上层。

为什么？因为上层框架通常自带重试。你把 429 抛上去，框架立刻重连；
你的退避和框架的重试叠加成"连点"，每一次连点都把服务端的限流时间轴往后推 ——
于是恢复时间从 2 秒变成十几秒，最后变成"越重试越打不开"。

用法是把"一次请求"写成一个纯函数，返回 ``Attempt``：

```python
def attempt(n):
    limiter.acquire()
    resp = do_request()
    if resp.status == 429:
        return Attempt(Verdict.RATE_LIMITED, wait=parse_retry_after(resp.json))
    if resp.status >= 500:
        return Attempt(Verdict.RETRYABLE, error=f"HTTP {resp.status}")
    if resp.status == 403:
        return Attempt(Verdict.FATAL, error="被拒绝，重试无意义")
    return Attempt(Verdict.OK, payload=resp.json)

report = RetryRunner().run(attempt)
```

四种判决刻意分开，因为它们的正确处置**不一样**：

===============  ====================================  ========================
判决             含义                                  处置
===============  ====================================  ========================
``OK``           成功                                  停止
``RATE_LIMITED`` 被限流，服务端可能给了等待时长      按给定时长原地退避（独立预算）
``RETRYABLE``    瞬时故障（网络抖动、5xx）             指数退避 + 抖动（独立预算）
``FATAL``        不该重试（鉴权失败、参数错误）        立即停止并如实上报
===============  ====================================  ========================
"""

from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping

__all__ = [
    "Verdict",
    "Attempt",
    "RetryPolicy",
    "RetryReport",
    "RetryRunner",
    "Backoff",
    "parse_retry_after",
]

# 「请在 3 秒后重试」「retry after 3s」「wait: 4s」等常见写法
_WAIT_PATTERNS = (
    re.compile(r"(\d+(?:\.\d+)?)\s*秒"),
    re.compile(r"retry[\s\-_]*after[^\d]{0,8}(\d+(?:\.\d+)?)", re.I),
    re.compile(r"wait[^\d]{0,8}(\d+(?:\.\d+)?)", re.I),
    re.compile(r"(\d+(?:\.\d+)?)\s*(?:s|sec|seconds?)\b", re.I),
)

# 会去正文里找"该等多少秒"的字段名
_TEXT_FIELDS = ("errmsg", "err_msg", "message", "msg", "error", "detail")
# 会当成"直接给出等待秒数"的字段名
_NUMERIC_FIELDS = ("retry_after", "retryAfter", "wait", "wait_seconds")


class Verdict(str, Enum):
    """一次尝试的判决。"""

    OK = "ok"
    RATE_LIMITED = "rate_limited"
    RETRYABLE = "retryable"
    FATAL = "fatal"


@dataclass
class Attempt:
    """一次尝试的结果。

    ``wait`` 只在 ``RATE_LIMITED`` 时有意义，表示**服务端建议**的等待秒数；
    为 ``None`` 时由 :class:`RetryPolicy` 用递增兜底值处理。
    ``payload`` 在成功时携带业务结果，:class:`RetryReport` 会把它带出来。
    """

    verdict: Verdict
    wait: float | None = None
    error: str | None = None
    payload: Any = None

    @property
    def ok(self) -> bool:
        return self.verdict is Verdict.OK


@dataclass
class RetryPolicy:
    """重试策略。

    ``max_rate_limit_attempts`` 与 ``max_attempts`` 分开，是因为两类失败的
    "值得再试几次"完全不是一个量级：限流等一会儿大概率能过（可以多试），
    真故障通常不会因为多试两次就好（少试，早点上报）。
    """

    max_attempts: int = 3
    max_rate_limit_attempts: int = 10
    wait_ceiling: float = 65.0        # 单次等待上限（秒）
    margin: float = 1.0               # 在服务端给的时长上再加的安全余量
    fallback_wait: float = 15.0       # 服务端没说时长时的兜底起点
    escalate: float = 1.5             # 兜底时长的递增倍率
    fallback_jitter: float = 0.1      # 兜底时长的抖动比例
    backoff_base: float = 1.0         # 指数退避基数
    backoff_factor: float = 2.0
    backoff_cap: float = 30.0
    backoff_jitter: float = 0.3

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1")
        if self.max_rate_limit_attempts < 1:
            raise ValueError("max_rate_limit_attempts 必须 >= 1")
        if self.wait_ceiling <= 0:
            raise ValueError("wait_ceiling 必须为正数")
        if self.escalate < 1.0:
            raise ValueError("escalate 不能小于 1（兜底值不该越等越短）")


class Backoff:
    """指数退避 + 抖动。

    抖动不是装饰：多个客户端同时被拦、同时醒来，会再次撞在一起。
    抖动把它们的相位错开。
    """

    def __init__(self, base: float = 1.0, factor: float = 2.0, cap: float = 30.0,
                 jitter: float = 0.3):
        if base <= 0:
            raise ValueError("base 必须为正数")
        if factor < 1.0:
            raise ValueError("factor 不能小于 1")
        self.base, self.factor, self.cap, self.jitter = base, factor, cap, jitter
        self.attempt = 0

    def reset(self) -> None:
        self.attempt = 0

    def next_delay(self) -> float:
        raw = min(self.cap, self.base * (self.factor ** self.attempt))
        self.attempt += 1
        return raw + random.uniform(0, raw * self.jitter)


@dataclass
class RetryReport:
    """一次重试执行的结果。"""

    ok: bool
    attempts: int
    rate_limited: int
    retried: int
    wait_seconds: float
    outcome: Attempt
    waits: list[float] = field(default_factory=list)

    @property
    def payload(self) -> Any:
        return self.outcome.payload

    @property
    def error(self) -> str | None:
        return self.outcome.error


class RetryRunner:
    """按 :class:`RetryPolicy` 执行"尝试 → 判决 → 等待 → 再尝试"。

    参数
    ----
    sleep:
        可注入的等待函数。测试用它替换真实等待，**同时还能断言等待时长** ——
        "被限流时是否按服务端说的秒数退避"因此变成一个可测的事实。
    on_event:
        事件回调 ``(kind, message, wait)``。日志格式交给调用方，
        本库只负责汇报发生了什么，不替调用方决定怎么打印。
    """

    def __init__(self, policy: RetryPolicy | None = None,
                 sleep: Callable[[float], None] = time.sleep,
                 on_event: Callable[[str, str, float], None] | None = None):
        self.policy = policy or RetryPolicy()
        self.sleep = sleep
        self.on_event = on_event

    # -- 内部 -------------------------------------------------------------
    def _emit(self, kind: str, message: str, wait: float = 0.0) -> None:
        if self.on_event is not None:
            self.on_event(kind, message, wait)

    def _rate_limit_wait(self, wait: float | None, previous: float | None) -> float:
        """算出限流该等多久，并更新兜底状态的递增值。"""
        policy = self.policy
        if wait is None:
            base = previous if previous is not None else policy.fallback_wait
            if previous is not None:
                base = min(base * policy.escalate, policy.wait_ceiling)
            base += random.uniform(0, base * policy.fallback_jitter)
            wait = base
        return min(max(wait, 1.0), policy.wait_ceiling) + policy.margin

    # -- 主流程 -----------------------------------------------------------
    def run(self, attempt: Callable[[int], Attempt]) -> RetryReport:
        """执行 ``attempt``，按判决决定是否重试。

        ``attempt`` 收到的是 1 起的尝试序号。
        """
        policy = self.policy
        backoff = Backoff(base=policy.backoff_base, factor=policy.backoff_factor,
                          cap=policy.backoff_cap, jitter=policy.backoff_jitter)

        total_wait = 0.0
        waits: list[float] = []
        rate_limited = 0
        retried = 0
        last_rate_limit_wait: float | None = None
        outcome = Attempt(Verdict.RETRYABLE, error="未执行")
        n = 0

        while True:
            n += 1
            outcome = attempt(n)

            if outcome.verdict is Verdict.OK:
                self._emit("ok", f"第 {n} 次尝试成功")
                return RetryReport(True, n, rate_limited, retried, total_wait,
                                   outcome, waits)

            if outcome.verdict is Verdict.FATAL:
                self._emit("fatal", outcome.error or "不可重试的错误")
                return RetryReport(False, n, rate_limited, retried, total_wait,
                                   outcome, waits)

            if outcome.verdict is Verdict.RATE_LIMITED:
                rate_limited += 1
                if rate_limited >= policy.max_rate_limit_attempts:
                    self._emit("gave_up",
                               f"连续 {rate_limited} 次被限流，放弃")
                    return RetryReport(False, n, rate_limited, retried, total_wait,
                                       outcome, waits)
                wait = self._rate_limit_wait(outcome.wait, last_rate_limit_wait)
                last_rate_limit_wait = wait
                self._emit("rate_limited", outcome.error or "被限流", wait)
            else:  # RETRYABLE
                retried += 1
                if retried >= policy.max_attempts:
                    self._emit("gave_up",
                               f"已重试 {retried} 次，放弃：{outcome.error or ''}")
                    return RetryReport(False, n, rate_limited, retried, total_wait,
                                       outcome, waits)
                wait = backoff.next_delay()
                self._emit("retryable", outcome.error or "可恢复错误", wait)

            total_wait += wait
            waits.append(wait)
            self.sleep(wait)


def parse_retry_after(payload: Any = None,
                      headers: Mapping[str, Any] | None = None) -> float | None:
    """从响应里解析「该等多少秒」，解析不出来返回 ``None``。

    优先级：**正文文本 → 数值字段 → Retry-After 头**。

    正文优先不是随便排的：真实服务端经常只在 ``errmsg`` 里写
    「请在 N 秒后重试」，而 ``Retry-After`` 头反而是缺的。

    返回 ``None`` 而不是某个默认值，因为"猜一个数字"是调用方的决定，
    不是本库的决定 —— 兜底策略由 :class:`RetryPolicy` 表达。
    """
    texts: list[str] = []
    if isinstance(payload, str):
        texts.append(payload)
    elif isinstance(payload, Mapping):
        for key in _TEXT_FIELDS:
            value = payload.get(key)
            if isinstance(value, str):
                texts.append(value)

    for text in texts:
        for pattern in _WAIT_PATTERNS:
            match = pattern.search(text)
            if match:
                return max(0.0, float(match.group(1)))

    if isinstance(payload, Mapping):
        for key in _NUMERIC_FIELDS:
            value = payload.get(key)
            if isinstance(value, bool):
                continue          # bool 是 int 的子类，别把 True 当成 1 秒
            if isinstance(value, (int, float)):
                return max(0.0, float(value))
            if isinstance(value, str):
                try:
                    return max(0.0, float(value))
                except ValueError:
                    pass

    if headers:
        header = headers.get("Retry-After")
        if header is not None:
            try:
                return max(0.0, float(header))
            except (TypeError, ValueError):
                pass

    return None

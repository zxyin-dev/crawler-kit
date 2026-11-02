# -*- coding: utf-8 -*-
"""限速器。

为什么必须"全局"
----------------
限速一旦不是全局的，就等于没限速。典型错误写法是"每个请求前 sleep 一下"：
单线程看起来没问题，一旦并发起来，N 个 worker 各自 sleep 会叠乘成 N 倍速度，
直接把服务端的限流窗口撞穿。

所以本库只提供**有状态的限速器对象**，由调用方持有单例并统一调用。
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Protocol, runtime_checkable

__all__ = ["Limiter", "TokenBucket", "FixedIntervalLimiter", "NoLimiter",
           "RateLimitGate", "make_limiter"]


@runtime_checkable
class Limiter(Protocol):
    """限速器协议：取一个配额，返回实际等待的秒数。"""

    def acquire(self) -> float:  # pragma: no cover - 协议声明
        ...


class TokenBucket:
    """令牌桶（线程安全）。

    适合"服务端允许短时突发、但稳态速率有上限"的场景 —— 这也是多数
    按客户端限流的接口的真实形态。

    参数
    ----
    capacity:
        桶容量，即允许的瞬时突发量。
    refill_per_sec:
        稳态回填速率（个/秒）。``1/6`` 表示每 6 秒回一个。

    例：服务端实测"10 秒内最多 2 次"，则稳态速率上限是 0.2 次/秒。
    留 20% 余量取 ``refill_per_sec=1/6``，并允许开头两次突发 ``capacity=2``。
    """

    def __init__(self, capacity: float = 2.0, refill_per_sec: float = 1.0 / 6.0):
        if capacity <= 0:
            raise ValueError("capacity 必须为正数")
        if refill_per_sec <= 0:
            raise ValueError("refill_per_sec 必须为正数")
        self.capacity = float(capacity)
        self.refill = float(refill_per_sec)
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        return (f"TokenBucket(capacity={self.capacity:g}, "
                f"refill_per_sec={self.refill:g})")

    def acquire(self) -> float:
        """取一个令牌；不足则阻塞等待。返回实际等待秒数。"""
        waited = 0.0
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self.capacity,
                                   self._tokens + (now - self._last) * self.refill)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return waited
                need = (1.0 - self._tokens) / self.refill
            time.sleep(need)
            waited += need


class FixedIntervalLimiter:
    """固定最小间隔（线程安全）。

    适合"不知道服务端上限，只知道不能快"的保守场景。代价是慢：
    间隔取多大就只能那么快，不会因为服务端其实更宽松而提速。
    """

    def __init__(self, min_interval: float):
        if min_interval < 0:
            raise ValueError("min_interval 不能为负")
        self.min_interval = float(min_interval)
        self._last = 0.0
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        return f"FixedIntervalLimiter(min_interval={self.min_interval:g})"

    def acquire(self) -> float:
        with self._lock:
            now = time.monotonic()
            wait = self._last + self.min_interval - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._last = now
            return max(0.0, wait)


class NoLimiter:
    """不限速（仅用于离线测试与已知无上限的本地服务）。"""

    def acquire(self) -> float:
        return 0.0

    def __repr__(self) -> str:
        return "NoLimiter()"


class RateLimitGate:
    """全局冷却闸门：一个 worker 被限流，全部 worker 一起停。

    为什么并发下必须有它
    -------------------
    限速器管的是**主动节奏**（我打算多久发一次），管不了**被动惩罚**（服务端
    刚告诉我"6 秒后再来"）。并发时只有拿到 429 的那个 worker 知道要退避，
    其余 worker 还在按原节奏敲门 —— 于是退避秒数被 N 个 worker 反复重置，
    实际变成"越退避越打不开"。

    闸门把"现在起 N 秒内谁都别发"变成全局事实：拿到 429 的 worker 调
    :meth:`block_for`，其余 worker 在 :meth:`wait` 处一起等。

    ``sleep`` 和 ``clock`` 都可注入。测试里换成假的时钟就能让"虚拟时间"真正
    走动，否则 ``sleep`` 不推进时间、闸门截止点永远在未来，会凭空多睡一次。
    """

    def __init__(self, sleep: Callable[[float], None] | None = None,
                 clock: Callable[[], float] | None = None):
        self._sleep = sleep or time.sleep
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._until = 0.0

    def block_for(self, seconds: float) -> None:
        """把全局冷却截止时间往后推到 ``seconds`` 秒之后（只延长，不缩短）。"""
        if seconds <= 0:
            return
        with self._lock:
            until = self._clock() + seconds
            if until > self._until:
                self._until = until

    def wait(self) -> float:
        """若处于冷却期则阻塞到冷却结束，返回实际等待秒数。"""
        with self._lock:
            remaining = self._until - self._clock()
        if remaining <= 0:
            return 0.0
        self._sleep(remaining)
        return remaining

    def __repr__(self) -> str:
        return "RateLimitGate()"


def make_limiter(*, min_interval: float | None = None, capacity: float = 2.0,
                 refill_per_sec: float = 1.0 / 6.0,
                 disabled: bool = False) -> Limiter:
    """显式给了间隔就用固定间隔，否则用令牌桶。

    这个"二选一"的签名是刻意的：它让"回退到旧的保守限速"变成一行改动
    （把 ``min_interval`` 传进来即可），而不是去改一堆参数。
    """
    if disabled:
        return NoLimiter()
    if min_interval is not None:
        return FixedIntervalLimiter(min_interval)
    return TokenBucket(capacity, refill_per_sec)

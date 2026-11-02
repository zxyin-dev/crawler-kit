# -*- coding: utf-8 -*-
"""业务插件：一个页面怎么请求、怎么解析。

框架里**只有这里需要按站点重写**。其余（限速、退避、断点、去重、落盘、报告）
都是内建的，因为它们在每个项目里的正确做法都一样，重写一遍只会重写一遍 bug。

三个方法，职责划得很死：

* ``request(page)`` —— 生成第 N 页的请求。**不签名、不重试、不发请求。**
* ``parse(page, response)`` —— 把回答变成记录列表。**不写盘、不判重。**
* ``classify(page, response)`` —— 可选。把"状态码/错误体"翻译成判决。

为什么 ``request`` 不签名？因为签名经常要"拿到完整请求再算"，而签名方式又会
在不同站点间换来换去（纯 Python / Node 桥 / 干脆不需要）。把它放在独立插件里，
换站点时只换签名器，业务代码一行不动。

分页的默认实现是 ``?page=N``，但真正的分页规则千奇百怪（游标、时间窗、
偏移量、页码从 0 开始）。所以 :class:`Spider` 只提供 ``pages()`` 生成**有序页码列表**，
不假设任何"自动翻页"逻辑 —— 顺序分页比并发翻页更容易做对断点。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence

from .retry import Attempt, Verdict
from .transport import Response

__all__ = [
    "RequestSpec",
    "Outcome",
    "Spider",
    "StdoutSpider",
    "Response",
    "json_field",
    "to_items",
]


@dataclass
class RequestSpec:
    """一次请求的描述。

    ``meta`` 是给签名器/解析器自由携带的字段（比如"这一页的游标"），
    传输层不看它。
    """

    method: str = "GET"
    url: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    body: Any = None
    timeout: float | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def replace(self, **changes: Any) -> "RequestSpec":
        """返回改过若干字段的副本（签名器用它拼装最终请求）。"""
        data = {
            "method": self.method,
            "url": self.url,
            "params": dict(self.params),
            "headers": dict(self.headers),
            "body": self.body,
            "timeout": self.timeout,
            "meta": dict(self.meta),
        }
        data.update(changes)
        return RequestSpec(**data)

    def to_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "url": self.url,
            "params": self.params,
            "headers": self.headers,
            "body": self.body,
            "timeout": self.timeout,
            "meta": self.meta,
        }


@dataclass
class Outcome:
    """解析结果。

    ``records`` 是本次拿到的数据行。空列表是**合法结果**（最后一页、
    查询无命中都可能是空），框架不会把"空"当成失败。

    ``verdict`` 只在 ``classify`` / ``parse`` 想主动表达"这一页该重试或该放弃"
    时才需要设成非 ``OK``。
    """

    verdict: Verdict = Verdict.OK
    records: list[dict[str, Any]] = field(default_factory=list)
    wait: float | None = None
    error: str | None = None
    payload: Any = None

    @property
    def ok(self) -> bool:
        return self.verdict is Verdict.OK

    def attempt(self) -> Attempt:
        return Attempt(self.verdict, wait=self.wait, error=self.error,
                       payload=self if self.ok else None)


class Spider:
    """业务插件基类。

    子类至少要实现 :meth:`request` 与 :meth:`parse`，并设 ``name``。
    """

    name: str = "spider"
    page_size: int = 20
    start_page: int = 1

    # -- 必写 -------------------------------------------------------------
    def request(self, page: int) -> RequestSpec:  # pragma: no cover - 抽象
        raise NotImplementedError("必须实现 request(page) -> RequestSpec")

    def parse(self, page: int, response: Response) -> Outcome:  # pragma: no cover
        raise NotImplementedError("必须实现 parse(page, response) -> Outcome")

    # -- 可选钩子 ---------------------------------------------------------
    def pages(self, first: int, last: int) -> list[int]:
        """生成**有序**页码列表。默认首尾闭区间。

        重写它就能支持游标/时间窗/自定义分页 —— 只要返回一个有序序列即可。
        """
        return list(range(first, last + 1))

    def classify(self, page: int, response: Response) -> Outcome | None:
        """把状态码翻译成判决；返回 ``None`` 表示"没问题，去解析吧"。

        默认规则覆盖了绝大多数情况：

        ============  ==================  ==============================
        状态码         判决                理由
        ============  ==================  ==============================
        2xx           ``None``            正常，交给 ``parse``
        429           ``RATE_LIMITED``    等一会儿再来（独立预算，可多试）
        5xx / 408     ``RETRYABLE``       瞬时故障
        401 / 403     ``FATAL``           鉴权/拒绝，重试只会更糟
        其他 4xx      ``FATAL``           参数或路径错了
        ============  ==================  ==============================

        重写时记得保留 429 的处理 —— 站点若把限流伪装成 200 + 错误码，
        就在 ``parse`` 里返回 ``Outcome(Verdict.RATE_LIMITED, wait=...)``。
        """
        status = response.status
        if status == 0:
            return Outcome(Verdict.RETRYABLE, error=response.error or "无响应")
        if 200 <= status < 300:
            return None
        if status == 429:
            return Outcome(Verdict.RATE_LIMITED, error="HTTP 429 限流")
        if status in (408, 425) or 500 <= status < 600:
            return Outcome(Verdict.RETRYABLE, error=f"HTTP {status}")
        return Outcome(Verdict.FATAL, error=f"HTTP {status}")

    def stop_at(self, page: int, outcome: Outcome) -> bool:
        """返回 ``True`` 表示"到最后一页了，后面的不用跑"。

        默认 ``False``：分页范围由 :meth:`pages` 显式给定，不猜终点。
        """
        return False

    def last_page(self) -> int | None:
        """可选：本次采集的最后一页。返回 ``None`` 表示"不知道，由调用方给"。

        和 :meth:`stop_at` 的区别是**时机**：``last_page`` 在开跑前就知道边界，
        所以后续页根本不会被请求；``stop_at`` 是跑到某一页才知道到底了。
        "我手上只有 40 个 id，就是 40 页"这种场景必须用前者 —— 否则第 41 页
        照样会被请求一次，而服务端会把它当成翻越边界（淘宝详情接口就是这么
        判定异常的）。写成方法而不是属性，是因为它往往依赖实例状态，而属性
        在类上写不出"可能为 None"的语义。
        """
        return None

    def signature(self) -> dict[str, Any]:
        """参与断点校验的身份字段。

        **只放"改变页码含义"的东西**（比如 ``page_size``）。
        页范围不要放进来 —— 它只是"这次跑多少"，放进去会让
        "先跑 1~3 页、再扩到 1~6 页"这种正常续采被误判成换任务而从头重来。
        """
        return {"page_size": self.page_size}

    def __repr__(self) -> str:
        return f"<{type(self).__name__} name={self.name!r} page_size={self.page_size}>"


class StdoutSpider(Spider):
    """把解析结果直接打印出来的最小示例（也用于调试解析器）。"""

    def request(self, page: int) -> RequestSpec:  # pragma: no cover - 示例
        return RequestSpec(url=f"http://127.0.0.1:8899/items?page={page}")

    def parse(self, page: int, response: Response) -> Outcome:  # pragma: no cover
        payload = response.json()
        records = to_items(payload)
        print(f"[page {page}] {len(records)} 条")
        return Outcome(Verdict.OK, records=records)


# -- 解析辅助 -------------------------------------------------------------
def json_field(obj: Any, path: str, default: Any = None) -> Any:
    """按点号路径取值：``json_field(payload, "data.list")``。

    少写一层 ``if`` 套 ``if``。取不到就返回 ``default``，不抛异常 ——
    接口字段缺失是常态，应该在解析结果里体现为"这行缺个字段"，
    而不是让整页解析崩掉。
    """
    current = obj
    for part in path.split("."):
        if isinstance(current, Mapping):
            if part not in current:
                return default
            current = current[part]
        elif isinstance(current, Sequence) and not isinstance(current, (str, bytes)):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return default
        else:
            return default
    return current


def to_items(payload: Any, keys: Sequence[str] = ("items", "list", "data", "rows",
                                                  "records", "results")) -> list[dict[str, Any]]:
    """把各种"列表藏在字段里"的回答统一成 ``list[dict]``。

    依次尝试：payload 本身是列表 → 逐个候选字段 → 字段里再套一层
    （``{"data": {"list": [...]}}``）。找到就停，找不到返回空列表。
    """
    if isinstance(payload, list):
        return [_as_record(x) for x in payload]
    if not isinstance(payload, Mapping):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [_as_record(x) for x in value]
        if isinstance(value, Mapping):
            nested = to_items(value, keys)
            if nested:
                return nested
    return []


def _as_record(item: Any) -> dict[str, Any]:
    if isinstance(item, Mapping):
        return dict(item)
    return {"value": item}

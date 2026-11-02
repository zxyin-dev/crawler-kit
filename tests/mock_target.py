# -*- coding: utf-8 -*-
"""离线假目标站：把"服务端行为"变成可测的东西。

采集框架的错误几乎都只在**服务端异常**时才暴露 —— 限流、5xx、半页数据、
最后一页、字段缺失。真站点上没法复现这些（也复现不起），
所以用本地假服务端把每种异常都做成一个可开关的行为。

这样一来，下面这些原本"只能靠上线才知道"的问题，全部变成断言：

* 429 时是否按 ``Retry-After`` 原地退避，而不是抛异常
* 500 连续几次后是否放弃并如实上报
* 第 3 页失败时断点是否**停在 2**（而不是跟着第 4 页前进）
* 续采时是否跳过已完成页、且不重复写数据
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlsplit

__all__ = ["MockTarget", "Item"]


class MockTarget:
    """可编程的假服务端。

    参数
    ----
    total:
        总记录数；``page_size`` 固定 10 条一页。
    rate_limit_first:
        前 N 次请求返回 429（带 ``Retry-After: 1``），模拟突发限流。
    fail_pages:
        这些页码永远返回 500，用于测"断点不越洞"与补采。
    transient_pages:
        这些页码**第一次**返回 503，第二次正常，用于测指数退避重试。
    empty_last:
        最后一页之后返回空列表（测"空页是合法结果"）。
    delay:
        每次响应前先睡这么久（秒）。并发测试用它把"网络等待"造出来，
        否则本地假站快到 0ms，并发与串行的耗时差测不出来。
    """

    def __init__(self, *, total: int = 35, rate_limit_first: int = 0,
                 fail_pages: tuple[int, ...] = (), transient_pages: tuple[int, ...] = (),
                 empty_last: bool = True, page_size: int = 10, delay: float = 0.0):
        self.total = total
        self.rate_limit_first = rate_limit_first
        self.fail_pages = set(fail_pages)
        self.transient_pages = set(transient_pages)
        self.empty_last = empty_last
        self.page_size = page_size
        self.delay = delay

        self.hits: list[int] = []
        self.statuses: list[int] = []
        self._seen_transient: set[int] = set()
        self._lock = threading.Lock()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    # -- 生命周期 ---------------------------------------------------------
    def __enter__(self) -> "MockTarget":
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()

    def start(self) -> "MockTarget":
        handler = self._make_handler()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    @property
    def base_url(self) -> str:
        assert self._server is not None, "先 start() 再取 base_url"
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def url(self, path: str = "/items") -> str:
        return f"{self.base_url}{path}"

    # -- 数据 -------------------------------------------------------------
    def items_for(self, page: int) -> list[dict[str, Any]]:
        """第 ``page`` 页的记录（1 起）。"""
        start = (page - 1) * self.page_size
        end = min(start + self.page_size, self.total)
        if start >= self.total:
            return []
        return [
            {"id": i + 1, "title": f"第 {i + 1} 条", "page": page}
            for i in range(start, end)
        ]

    def reset(self) -> None:
        with self._lock:
            self.hits.clear()
            self.statuses.clear()
            self._seen_transient.clear()

    # -- 内部 -------------------------------------------------------------
    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        target = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:   # 静音
                pass

            def do_GET(self) -> None:  # noqa: N802
                target._handle(self)

            def do_POST(self) -> None:  # noqa: N802
                target._handle(self)

        return Handler

    def _handle(self, handler: BaseHTTPRequestHandler) -> None:
        parts = urlsplit(handler.path)
        query = parse_qs(parts.query)
        try:
            page = int(query.get("page", ["1"])[0])
        except ValueError:
            page = 1

        # 在锁外睡：睡在锁内会把假站变回单线程，并发测试就失去意义
        if self.delay:
            time.sleep(self.delay)

        with self._lock:
            self.hits.append(page)

            if self.rate_limit_first and len(self.hits) <= self.rate_limit_first:
                self.statuses.append(429)
                self._reply(handler, 429, {"error": "限流", "retry_after": 1},
                            extra_headers={"Retry-After": "1"})
                return

            if page in self.fail_pages:
                self.statuses.append(500)
                self._reply(handler, 500, {"error": "boom"})
                return

            if page in self.transient_pages and page not in self._seen_transient:
                self._seen_transient.add(page)
                self.statuses.append(503)
                self._reply(handler, 503, {"error": "临时故障"})
                return

            items = self.items_for(page)
            if not items and not self.empty_last:
                self.statuses.append(404)
                self._reply(handler, 404, {"error": "no such page"})
                return

            self.statuses.append(200)
            self._reply(handler, 200, {"page": page, "items": items})

    def _reply(self, handler: BaseHTTPRequestHandler, status: int,
               payload: dict[str, Any], *,
               extra_headers: dict[str, str] | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        for key, value in (extra_headers or {}).items():
            handler.send_header(key, value)
        handler.end_headers()
        handler.wfile.write(body)


class Item(dict):
    """一条记录（仅用于让测试断言更好读）。"""


def make_handler_factory(fn: Callable[[int], tuple[int, dict[str, Any]]]) -> type:
    """给需要完全自定义响应的测试用：``fn(page) -> (status, payload)``。"""
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args: Any) -> None:
            pass

        def do_GET(self) -> None:  # noqa: N802
            query = parse_qs(urlsplit(self.path).query)
            page = int(query.get("page", ["1"])[0])
            status, payload = fn(page)
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler

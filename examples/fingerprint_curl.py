# -*- coding: utf-8 -*-
"""示例：指纹传输（curl_cffi）+ 多并发。

两件事在同一个文件里，因为它们在真实项目里总是同时出现：站点开始上风控时，
你既要让 TLS 握手像浏览器，又要把被风控拖慢的等待用并发补回来。

一、指纹传输
------------
框架默认的 ``HttpTransport`` 走标准库 urllib，零依赖。但有些站点看 TLS 指纹
（JA3/JA4）和 HTTP/2 帧序，urllib 的特征太明显。这时只换传输插件：

    Crawler(spider, config, transport=CurlCffiTransport(impersonate="chrome"))

``curl_cffi`` 复用 curl-impersonate 的补丁，把密码套件顺序、扩展列表、ALPN
伪装成指定浏览器。已内建在 ``crawler_kit.transport`` 里，不用自己写适配层。

为什么适配层必须存在（而不是直接用 ``curl_cffi.Session``）：

* 框架契约是"返回 ``Response``，状态码是数据不是异常"，
  而 ``curl_cffi`` 默认对 4xx/5xx 抛异常 —— 一抛就丢了"限流还是真拒绝"。
* 框架契约是"只有网络层故障才抛 ``TransportError``"。
* ``curl_cffi`` 自带 ``retry``，必须显式关掉（``retry=0``），否则它的内部重试
  会和框架的重试叠加成连点，越重试越打不开。

二、多并发
----------
    Config(concurrency=4)

并发只并行**网络等待**，写盘和断点推进仍然串行（都在一把锁里）。
本地假站快到 0ms 时并发看不出好处，所以本示例让假站每页慢 0.3 秒 ——
这正是并发存在的理由。

跑法::

    python examples/fingerprint_curl.py
"""

from __future__ import annotations

import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler_kit import (  # noqa: E402
    Config,
    Crawler,
    CurlCffiTransport,
    HttpTransport,
    Outcome,
    RequestSpec,
    Response,
    Spider,
    TransportError,
    Verdict,
    setup_logging,
)


class SlowTarget:
    """每页慢 0.3 秒的假站 —— 让"并发省了等待"成为可观察的事实。"""

    DELAY = 0.3
    TOTAL = 40
    PAGE_SIZE = 10

    def __init__(self) -> None:
        target = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:
                pass

            def do_GET(self) -> None:  # noqa: N802
                import json
                from urllib.parse import parse_qs, urlsplit

                page = int(parse_qs(urlsplit(self.path).query).get("page", ["1"])[0])
                time.sleep(target.DELAY)          # 模拟网络往返
                start = (page - 1) * target.PAGE_SIZE
                items = [{"id": i + 1, "page": page}
                         for i in range(start, min(start + target.PAGE_SIZE,
                                                   target.TOTAL))]
                body = json.dumps({"items": items}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/items"

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


class DemoSpider(Spider):
    name = "fingerprint-demo"
    page_size = SlowTarget.PAGE_SIZE

    def __init__(self, url: str):
        self.url = url

    def request(self, page: int) -> RequestSpec:
        return RequestSpec(url=f"{self.url}?page={page}")

    def parse(self, page: int, response: Response) -> Outcome:
        return Outcome(Verdict.OK, records=(response.json() or {}).get("items", []))


def main() -> int:
    setup_logging(level="INFO")
    target = SlowTarget()
    work_dir = Path(__file__).parent / "_fingerprint_data"
    try:
        # 没装 curl_cffi 就退回 urllib —— 示例要能在这两种环境下都跑通
        try:
            transport = CurlCffiTransport(impersonate="chrome", timeout=10.0)
            label = "curl_cffi(chrome)"
        except TransportError as exc:
            transport = HttpTransport(timeout=10.0)
            label = f"urllib（{exc}）"
        print(f"传输层：{label}")

        for concurrency in (1, 4):
            # 每组用独立工作目录：复用同一个目录时，第二轮全是"重复"，
            # records_written 会显示 0，看起来像并发没采到数据。
            config = Config(work_dir=work_dir / f"c{concurrency}",
                            concurrency=concurrency,
                            min_interval=0, verbose=False)
            start = time.monotonic()
            report = Crawler(DemoSpider(target.url), config, transport=transport) \
                .run(first=1, last=4)
            elapsed = time.monotonic() - start
            print(f"concurrency={concurrency}: {elapsed:.2f}s, "
                  f"{report.records_written} 条, 游标 {report.cursor}")
    finally:
        target.stop()

    print("\n并发 4 应明显快于并发 1（4 页各 0.3 秒，理论 1.2s 对 0.3s）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

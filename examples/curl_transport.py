# -*- coding: utf-8 -*-
"""示例：把传输层换成 curl_cffi（需要 TLS/JA3 指纹时才用）。

框架默认的 ``HttpTransport`` 走标准库 urllib，零依赖。但有些站点的风控看
TLS 指纹（JA3/JA4）和 HTTP/2 帧序，urllib 的特征太明显，一眼就是脚本。

指纹传输已经内建，不需要自己写适配层：

.. code-block:: python

    Crawler(spider, config, transport=CurlCffiTransport(impersonate="chrome"))

``curl_cffi`` 复用 curl-impersonate 的指纹补丁，把 TLS 握手的密码套件顺序、
扩展列表、ALPN 都伪装成某个真实浏览器。

为什么框架里必须有一个适配层（而不是直接用 ``curl_cffi.Session``）：

* 框架契约是"返回 :class:`Response`，状态码是数据不是异常"，
  而 ``curl_cffi`` 默认对 4xx/5xx 抛异常 —— 一抛就丢了"限流还是真拒绝"。
* 框架契约是"只有网络层故障才抛 :class:`TransportError`"。
* ``curl_cffi`` 自带 ``retry``，必须显式关掉，否则它的内部重试会和框架的
  重试叠加成连点，越重试越打不开。

本示例演示三件事：

1. 直接用内建的 ``CurlCffiTransport``；
2. 想按页换指纹时，怎么用子类只覆盖 ``impersonate_for``（其余全部复用）；
3. 没装 ``curl_cffi`` 时怎么优雅退回 urllib。

跑法::

    python examples/curl_transport.py
"""

from __future__ import annotations

import json
import sys
import threading
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


class PerPageFingerprintTransport(CurlCffiTransport):
    """按页换指纹：只覆盖 ``fetch``，其它一律继承。

    真实场景：第一页用 chrome 拿正常 HTML，某个 JSON 接口反而对 safari 更宽松。
    这类差异只应体现在"选哪个指纹"上，不该把 URL 拼装、请求头合并、
    响应体截断这些已验证逻辑再抄一遍 —— 所以覆盖 :meth:`impersonate_for`
    一个方法就够，:meth:`fetch` 完全继承。
    """

    #: 页码 -> 指纹名
    FINGERPRINTS = {1: "chrome", 2: "safari"}

    def impersonate_for(self, spec: RequestSpec) -> str:
        # 覆盖方法而不是改 self.impersonate：传输对象被多线程共享，
        # 改属性就是让两个线程互相踩；只读的方法天然并发安全。
        return self.FINGERPRINTS.get(spec.meta.get("page"), self.impersonate)


class DemoTarget:
    """示例用的最小假站（只在本文件里用，不依赖 tests/）。"""

    def __init__(self) -> None:
        target = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:
                pass

            def do_GET(self) -> None:  # noqa: N802
                from urllib.parse import parse_qs, urlsplit

                page = int(parse_qs(urlsplit(self.path).query).get("page", ["1"])[0])
                items = [{"id": page * 3 + i, "page": page} for i in range(3)]
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
    name = "curl-demo"

    def __init__(self, url: str):
        self.url = url

    def request(self, page: int) -> RequestSpec:
        # meta 里带上页码，让传输层能按页换指纹（框架本身不解释 meta）
        return RequestSpec(url=f"{self.url}?page={page}", meta={"page": page})

    def parse(self, page: int, response: Response) -> Outcome:
        return Outcome(Verdict.OK, records=(response.json() or {}).get("items", []))


def main() -> int:
    setup_logging(level="INFO")
    target = DemoTarget()
    work_dir = Path(__file__).parent / "_curl_data"
    try:
        # 没装 curl_cffi 就退回 urllib —— 示例要能在这两种环境下都跑通
        try:
            transport: HttpTransport = CurlCffiTransport(impersonate="chrome",
                                                         timeout=10.0)
            label = "curl_cffi(chrome)"
        except TransportError as exc:
            transport = HttpTransport(timeout=10.0)
            label = f"urllib（{exc}）"
        print(f"传输层：{label}")

        report = Crawler(DemoSpider(target.url),
                         Config(work_dir=work_dir, min_interval=0, verbose=False),
                         transport=transport).run(first=1, last=2)
    finally:
        target.stop()

    print("报告：", report.summary())
    return 0 if not report.failed_pages else 1


if __name__ == "__main__":
    raise SystemExit(main())

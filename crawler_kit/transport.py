# -*- coding: utf-8 -*-
"""传输层：默认 urllib（零依赖），可换。

两条约定，都很要紧：

1. **HTTP 状态码是数据，不是异常。**
   403 / 429 / 500 都是"服务端给出的回答"，要以 ``Response.status`` 交回去，
   由编排层判决该怎么处置。传输层一旦把 4xx 抛成异常，上层就失去了"这是限流
   还是真拒绝"的信息，只能一概重试 —— 这正是"越重试越打不开"的起点。

2. **网络故障才是异常。**
   连接失败、超时、DNS 挂了 —— 这些没有"回答"，抛 :class:`TransportError`。
   它是 ``RETRYABLE`` 的天然信号。

传输层**自己绝不重试**。重试只在编排层发生（见 ``retry`` 模块的说明）。

3. **传输对象必须可被多线程共享。**
   并发采集时同一个传输实例会被多个 worker 同时调用。因此所有"连接池 /
   cookie 罐 / 打开器"这类**有状态**的东西都不能放在实例上，必须每线程一份
   （见 :class:`_ThreadLocal`）。把 ``requests.Session`` 放在实例上共享是并发
   采集最常见的错误：轻则 cookie 互相覆盖，重则连接池里两个线程抢同一个
   socket 直接崩。无状态的东西（超时、默认头、指纹配置）共享没问题。

两种现成实现：

* :class:`HttpTransport` —— 标准库 urllib，零依赖，TLS 指纹是 Python 的。
* :class:`CurlCffiTransport` —— 借用 curl_cffi 的浏览器 TLS/HTTP2 指纹。
  目标站按 JA3 指纹判定"是不是真浏览器"时必须换这个，否则 UA 再像也会被
  TLS 握手特征一眼识破（这是 403 / 空响应最常见的原因之一）。
"""

from __future__ import annotations

import gzip
import io
import json
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

__all__ = ["TransportError", "Response", "HttpTransport", "CurlCffiTransport",
           "Transport", "_ThreadLocal"]


class TransportError(Exception):
    """网络层面的失败（没有拿到任何 HTTP 回答）。"""


@dataclass
class Response:
    """一次 HTTP 回答。

    ``status`` 为 ``0`` 表示压根没拿到状态码（一般伴随 ``error``）。
    """

    status: int
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    elapsed: float = 0.0
    error: str | None = None
    _text: str | None = None

    # -- 便捷访问 ---------------------------------------------------------
    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    @property
    def text(self) -> str:
        """按 ``Content-Type`` 的 charset 解码，失败退 UTF-8 替换。"""
        if self._text is None:
            self._text = self._decode()
        return self._text

    def _decode(self) -> str:
        charset = None
        content_type = self.headers.get("Content-Type", "")
        for part in content_type.split(";"):
            part = part.strip()
            if part.lower().startswith("charset="):
                charset = part.split("=", 1)[1].strip().strip('"').lower()
                break
        for encoding in filter(None, (charset, "utf-8", "gb18030")):
            try:
                return self.body.decode(encoding)
            except (UnicodeDecodeError, LookupError):
                continue
        return self.body.decode("utf-8", errors="replace")

    def json(self, default: Any = None) -> Any:
        """解析 JSON；解析不出来返回 ``default``（不抛异常）。

        很多"看起来是 JSON 的接口"在限流时会返回一段 HTML 或空体。
        让 ``json()`` 抛异常会把一个可判决的情况变成一次崩溃，
        调用方反而看不出"其实是被限流了"。
        """
        try:
            return json.loads(self.text)
        except ValueError:
            return default

    def header(self, name: str, default: str | None = None) -> str | None:
        """大小写不敏感地取响应头。"""
        lowered = name.lower()
        for key, value in self.headers.items():
            if key.lower() == lowered:
                return value
        return default

    def __repr__(self) -> str:
        return f"Response(status={self.status}, bytes={len(self.body)}, url={self.url!r})"


class Transport:
    """传输协议：把 ``RequestSpec`` 换成 :class:`Response`。"""

    def fetch(self, spec: Any) -> Response:  # pragma: no cover - 协议声明
        raise NotImplementedError


class _ThreadLocal:
    """每个线程一份的懒加载对象。

    为什么不用实例属性：``urllib`` 的 ``OpenerDirector`` 与 ``curl_cffi`` 的
    ``Session`` 都持有连接池，**不是线程安全的**。并发采集时若共享一份，
    两个线程会抢同一个底层连接 —— 表现为随机的连接重置、串包的 cookie、
    或者干脆整段采集卡死在某个 socket 上。每线程一份就没有这个问题，
    代价只是连接不能跨线程复用（并发场景下这个复用本来就极少命中）。
    """

    __slots__ = ("_factory", "_local")

    def __init__(self, factory: Callable[[], Any]):
        self._factory = factory
        self._local = threading.local()

    def get(self) -> Any:
        obj = getattr(self._local, "obj", None)
        if obj is None:
            obj = self._factory()
            self._local.obj = obj
        return obj


class HttpTransport:
    """基于标准库 ``urllib`` 的传输实现（零依赖）。

    参数
    ----
    timeout:
        整体超时秒数。
    headers:
        所有请求都会带上的默认头。
    proxy:
        ``http://host:port`` 或 ``socks5://host:port``。
        SOCKS 需要额外装 ``PySocks``；装不了就换 curl_cffi 传输。
    verify_ssl:
        关掉 TLS 校验（仅用于自签证书的本地测试服务）。
    max_body:
        单次响应最多保留多少字节，防止误下大文件把内存吃满。
    """

    def __init__(self, *, timeout: float = 20.0,
                 headers: Mapping[str, str] | None = None,
                 proxy: str | None = None, verify_ssl: bool = True,
                 max_body: int = 32 * 1024 * 1024):
        self.timeout = float(timeout)
        self.headers = {"User-Agent": "crawler-kit/1.0", **dict(headers or {})}
        self.proxy = proxy
        self.verify_ssl = verify_ssl
        self.max_body = max_body
        # 打开器持有连接池，必须每线程一份
        self._opener = _ThreadLocal(self._build_opener)

    def _build_opener(self) -> urllib.request.OpenerDirector:
        handlers: list[urllib.request.BaseHandler] = []
        if self.proxy:
            parsed = urllib.parse.urlsplit(self.proxy)
            scheme = parsed.scheme.lower()
            host = f"{parsed.hostname}:{parsed.port or 1080}"
            if scheme.startswith("socks"):
                try:
                    import socks  # type: ignore[import-not-found]
                except ImportError as exc:  # pragma: no cover - 环境相关
                    raise TransportError(
                        "使用 socks 代理需要 PySocks：pip install PySocks"
                    ) from exc
                socks.set_default_proxy(
                    socks.SOCKS5 if scheme.startswith("socks5") else socks.SOCKS4,
                    parsed.hostname, parsed.port or 1080)
                socket.socket = socks.socksocket  # type: ignore[assignment]
            else:
                handlers.append(urllib.request.ProxyHandler(
                    {"http": self.proxy, "https": self.proxy}))
        if not self.verify_ssl:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            handlers.append(urllib.request.HTTPSHandler(context=context))
        return urllib.request.build_opener(*handlers)

    # -- 主入口 -----------------------------------------------------------
    def build_url(self, spec: Any) -> str:
        """把 ``RequestSpec.params`` 拼进 URL。

        单独拆出来，是为了让替换传输实现（curl_cffi 等）只需要重写"怎么发"，
        不用重新实现"怎么拼" —— 后者最容易写错（已带 query 的 URL 要用 ``&``
        而不是 ``?``，列表参数要 ``doseq``）。
        """
        url = spec.url
        if spec.params:
            separator = "&" if urllib.parse.urlsplit(url).query else "?"
            url = f"{url}{separator}{urllib.parse.urlencode(spec.params, doseq=True)}"
        return url

    def fetch(self, spec: Any) -> Response:
        """执行一次请求。**不重试、不对状态码做判断。**"""
        url = self.build_url(spec)

        body = spec.body
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False).encode("utf-8")
        elif isinstance(body, str):
            body = body.encode("utf-8")

        headers = {**self.headers, **dict(spec.headers or {})}
        if body and "Content-Type" not in {k.title() for k in headers}:
            headers.setdefault("Content-Type", "application/json; charset=utf-8")

        request = urllib.request.Request(
            url, data=body, method=spec.method.upper(), headers=headers)
        started = time.monotonic()
        try:
            with self._opener.get().open(request, timeout=spec.timeout or self.timeout) as raw:
                payload = self._read_body(raw)
                return Response(
                    status=raw.status,
                    url=raw.geturl(),
                    headers={k: v for k, v in raw.headers.items()},
                    body=payload,
                    elapsed=time.monotonic() - started,
                )
        except urllib.error.HTTPError as exc:
            # 4xx/5xx 也是回答：把状态码和正文原样带回去
            try:
                payload = self._read_body(exc)
            except Exception:
                payload = b""
            finally:
                # HTTPError 自己持有连接，不关掉会泄漏（ResourceWarning）
                exc.close()
            return Response(
                status=exc.code,
                url=getattr(exc, "url", url) or url,
                headers={k: v for k, v in (exc.headers or {}).items()},
                body=payload,
                elapsed=time.monotonic() - started,
            )
        except (urllib.error.URLError, socket.timeout, ssl.SSLError,
                ConnectionError, OSError) as exc:
            raise TransportError(f"{type(exc).__name__}: {exc}") from exc

    def _read_body(self, raw: Any) -> bytes:
        """读正文并按 ``Content-Encoding`` 解压。"""
        payload = raw.read(self.max_body)
        encoding = ""
        headers = getattr(raw, "headers", None)
        if headers is not None:
            encoding = (headers.get("Content-Encoding") or "").lower().strip()
        if encoding == "gzip":
            try:
                payload = gzip.decompress(payload)
            except OSError:
                pass
        elif encoding == "deflate":
            try:
                payload = zlib.decompress(payload, -zlib.MAX_WBITS)
            except zlib.error:
                try:
                    payload = zlib.decompress(payload)
                except zlib.error:
                    pass
        return payload


class CurlCffiTransport(HttpTransport):
    """借用 ``curl_cffi`` 的浏览器 TLS 指纹（JA3/JA4 + HTTP/2 特征）。

    为什么要它
    ----------
    很多站点的第一道防线不是 UA 而是 **TLS 握手指纹**：Python 的 ``ssl`` 发出去的
    ClientHello（密码套件顺序、扩展集合、椭圆曲线顺序）与真实浏览器不同，
    服务端据此直接返回 403 或空响应 —— 此时无论怎么改 ``User-Agent``、
    ``Accept-Language`` 都没用，因为判定发生在 HTTP 之前。

    ``curl_cffi`` 复用了 curl-impersonate 的补丁版 curl，能发出与
    Chrome / Firefox / Safari 一致的握手指纹。所以：**UA 再像也没用，
    要让 TLS 也像。**

    用法::

        Crawler(spider, Config(work_dir="data"),
                transport=CurlCffiTransport(impersonate="chrome"))

    参数
    ----
    impersonate:
        指纹目标，如 ``"chrome"`` / ``"chrome110"`` / ``"firefox"`` /
        ``"safari"``。取宽泛名（``chrome``）时随库版本升级自动跟上最新指纹，
        取精确版本号则是为了复现某个已确认可用的组合。**换指纹后一定要重跑
        一页验证**：指纹变了就等于换了个客户端，服务端行为可能跟着变。
    default_headers:
        curl_cffi 默认会补一整套浏览器头（``sec-ch-ua``、``accept-encoding``
        等）。保持 ``True`` 更真实；设为 ``False`` 时只发你自己给的头，
        用于"服务端对多余头敏感"的少数情况。
    http2:
        是否允许 HTTP/2。多数指纹场景需要开（HTTP/2 的帧特征也是指纹的一部分）。

    依赖是**可选**的：``pip install curl-cffi``。没装时构造本类会立刻抛
    ``TransportError``，而不是等到第一次请求才在并发线程里炸 —— 后者会让
    错误看起来像"网络问题"。
    """

    def __init__(self, *, impersonate: str = "chrome",
                 default_headers: bool = True, http2: bool = True,
                 **kwargs: Any):
        try:
            from curl_cffi import requests as cffi_requests
        except ImportError as exc:
            raise TransportError(
                "指纹传输需要 curl_cffi：pip install curl-cffi"
            ) from exc
        self._requests = cffi_requests
        self.impersonate = impersonate
        self.default_headers = default_headers
        self.http2 = http2
        # 复用父类的 URL 拼接与头部合并；不建 urllib 打开器
        self.timeout = float(kwargs.pop("timeout", 20.0))
        self.headers = {"User-Agent": "crawler-kit/1.0",
                        **dict(kwargs.pop("headers", None) or {})}
        self.proxy = kwargs.pop("proxy", None)
        self.verify_ssl = kwargs.pop("verify_ssl", True)
        self.max_body = kwargs.pop("max_body", 32 * 1024 * 1024)
        if kwargs:
            raise TypeError(f"不认识的参数：{sorted(kwargs)}")
        # Session 持连接池与 cookie 罐，必须每线程一份
        self._session = _ThreadLocal(self._build_session)

    def _build_session(self) -> Any:
        # 注意 ``retry=0``：curl_cffi 自带重试，必须显式关掉。
        # 否则它会在传输层悄悄重试，与编排层的重试叠加成连点 —— 正是本库
        # 第 2 条硬规则要禁止的事。
        options: dict[str, Any] = {
            "impersonate": self.impersonate,
            "default_headers": self.default_headers,
            "verify": self.verify_ssl,
            "retry": 0,
        }
        if self.http2:
            # 0.16 起用 http_version（旧版的 http2=True 参数已被移除）
            options["http_version"] = "v2"
        return self._requests.Session(**options)

    def impersonate_for(self, spec: Any) -> str:
        """本次请求用哪个指纹。

        单列成一个方法而不是让调用方改 ``self.impersonate``：传输对象被多个
        线程共享，改属性就是让两个线程互相踩；覆盖方法则天然是只读的，
        按页换指纹不需要加锁。
        """
        return self.impersonate

    def fetch(self, spec: Any) -> Response:
        """执行一次请求。语义与 :meth:`HttpTransport.fetch` 完全一致。"""
        url = self.build_url(spec)
        headers = {**self.headers, **dict(spec.headers or {})}
        fingerprint = self.impersonate_for(spec)

        body = spec.body
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False).encode("utf-8")
        elif isinstance(body, str):
            body = body.encode("utf-8")

        started = time.monotonic()
        try:
            raw = self._session.get().request(
                (spec.method or "GET").upper(), url,
                headers=headers,
                data=body,
                timeout=spec.timeout or self.timeout,
                allow_redirects=True,
                proxy=self.proxy,
                impersonate=fingerprint,
            )
        except Exception as exc:   # curl_cffi 的异常体系与 urllib 不同，统一收口
            raise TransportError(f"{type(exc).__name__}: {exc}") from exc

        payload = raw.content
        if len(payload) > self.max_body:
            payload = payload[:self.max_body]
        return Response(
            status=int(raw.status_code),
            url=str(raw.url or url),
            headers={str(k): str(v) for k, v in raw.headers.items()},
            body=payload,
            elapsed=time.monotonic() - started,
        )


class _NullTransport:
    """什么都不做的传输，用于离线单测与解析器调试。"""

    def fetch(self, spec: Any) -> Response:  # pragma: no cover - 测试辅助
        return Response(status=0, url=getattr(spec, "url", ""), error="null transport")

# -*- coding: utf-8 -*-
"""单元测试：三个插件点（spider / signer / transport）与日志。

插件点是这个框架唯一需要业务方写代码的地方，所以它们的契约必须被钉死：
"签名不发请求"、"状态码不抛异常"、"解析崩了不重试"。
"""

from __future__ import annotations

import io
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler_kit import (  # noqa: E402
    HttpTransport,
    Outcome,
    PurePythonSigner,
    RequestSpec,
    Response,
    SignerError,
    Spider,
    TransportError,
    Verdict,
    get_logger,
    json_field,
    safe_key,
    setup_logging,
    to_items,
)


class ResponseTest(unittest.TestCase):
    def test_json_returns_default_instead_of_raising(self) -> None:
        """限流时很多接口返回 HTML —— json() 抛异常会把可判决的情况变崩溃。"""
        resp = Response(status=429, url="x", body=b"<html>busy</html>")
        self.assertIsNone(resp.json())
        self.assertEqual(resp.json({"fallback": True}), {"fallback": True})

    def test_text_decodes_by_content_type_charset(self) -> None:
        body = "中文".encode("gb18030")
        resp = Response(status=200, url="x", body=body,
                        headers={"Content-Type": "text/html; charset=gb18030"})
        self.assertEqual(resp.text, "中文")

    def test_header_is_case_insensitive(self) -> None:
        resp = Response(status=200, url="x", headers={"Retry-After": "5"})
        self.assertEqual(resp.header("retry-after"), "5")
        self.assertIsNone(resp.header("X-Missing"))

    def test_ok_only_for_2xx(self) -> None:
        self.assertTrue(Response(status=204, url="x").ok)
        self.assertFalse(Response(status=301, url="x").ok)
        self.assertFalse(Response(status=0, url="x").ok)


class TransportTest(unittest.TestCase):
    def test_build_url_appends_params_with_correct_separator(self) -> None:
        t = HttpTransport()
        self.assertEqual(t.build_url(RequestSpec(url="http://a/b", params={"p": 1})),
                         "http://a/b?p=1")
        self.assertEqual(t.build_url(RequestSpec(url="http://a/b?x=1", params={"p": 1})),
                         "http://a/b?x=1&p=1", "已有 query 时必须用 & 而不是 ?")

    def test_build_url_expands_list_params(self) -> None:
        url = HttpTransport().build_url(
            RequestSpec(url="http://a/b", params={"tag": ["x", "y"]}))
        self.assertEqual(url, "http://a/b?tag=x&tag=y")

    def test_network_failure_raises_transport_error(self) -> None:
        t = HttpTransport(timeout=1.0)
        with self.assertRaises(TransportError):
            t.fetch(RequestSpec(url="http://127.0.0.1:1/never"))


class SpiderTest(unittest.TestCase):
    def test_default_pages_is_closed_range(self) -> None:
        spider = _StubSpider()
        self.assertEqual(spider.pages(1, 4), [1, 2, 3, 4])

    def test_default_classify_maps_status_to_verdict(self) -> None:
        spider = _StubSpider()
        self.assertIsNone(spider.classify(1, Response(200, "x")))
        self.assertEqual(spider.classify(1, Response(429, "x")).verdict,
                         Verdict.RATE_LIMITED)
        self.assertEqual(spider.classify(1, Response(503, "x")).verdict,
                         Verdict.RETRYABLE)
        self.assertEqual(spider.classify(1, Response(403, "x")).verdict,
                         Verdict.FATAL)
        self.assertEqual(spider.classify(1, Response(404, "x")).verdict,
                         Verdict.FATAL, "404 是真拒绝，重试没意义")
        self.assertEqual(spider.classify(1, Response(0, "x")).verdict,
                         Verdict.RETRYABLE, "没拿到状态码 = 网络问题")

    def test_signature_only_contains_page_size(self) -> None:
        """断点身份只放改变页码含义的东西 —— 别把页范围塞进来。"""
        self.assertEqual(_StubSpider().signature(), {"page_size": 20})

    def test_to_items_finds_common_container_keys(self) -> None:
        self.assertEqual(to_items({"items": [{"id": 1}]}), [{"id": 1}])
        self.assertEqual(to_items({"data": {"list": [{"id": 2}]}}), [{"id": 2}],
                         "允许一层嵌套")
        self.assertEqual(to_items({"unknown": 1}), [], "认不出来就空列表，不抛异常")
        # 标量列表也要变成记录，否则下游 store 拿到 int 会炸
        self.assertEqual(to_items({"items": [1]}), [{"value": 1}])

    def test_json_field_dot_path(self) -> None:
        self.assertEqual(json_field({"a": {"b": 1}}, "a.b"), 1)
        self.assertIsNone(json_field({"a": 1}, "a.b"), "中间断了返回默认值")
        self.assertEqual(json_field({"a": 1}, "a.b", "dflt"), "dflt")


class SignerTest(unittest.TestCase):
    def test_pure_python_signer_merges_partial_dict(self) -> None:
        spec = RequestSpec(url="http://a/b", params={"p": 1})
        signed = PurePythonSigner(lambda s: {"params": {"sign": "abc"}}).sign(spec)
        self.assertEqual(signed.params, {"p": 1, "sign": "abc"}, "params 是合并不是替换")
        self.assertEqual(signed.url, spec.url)

    def test_pure_python_signer_accepts_full_spec(self) -> None:
        def func(spec: RequestSpec) -> RequestSpec:
            return spec.replace(headers={"X-Sign": "v"})

        signed = PurePythonSigner(func).sign(RequestSpec(url="http://a/b"))
        self.assertEqual(signed.headers, {"X-Sign": "v"})

    def test_unknown_key_raises(self) -> None:
        with self.assertRaises(SignerError):
            PurePythonSigner(lambda s: {"bogus": 1}).sign(RequestSpec(url="u"))

    def test_non_callable_rejected(self) -> None:
        with self.assertRaises(TypeError):
            PurePythonSigner("not-a-function")  # type: ignore[arg-type]


class LogTest(unittest.TestCase):
    """日志本身的 bug 最难发现：它不崩，只是悄悄打印错的东西。

    注意：handler 装在 root 上，所以测试必须**临时把其它 handler 摘掉** ——
    否则同一个 LogRecord 会被多个 handler 依次格式化，测出来的是
    "两次格式化的结果"，不是"一次格式化的结果"。
    """

    def setUp(self) -> None:
        import logging
        setup_logging(level="INFO", console=False)
        self.root = logging.getLogger()
        self._saved = list(self.root.handlers)
        self.root.handlers.clear()
        self.buffer = io.StringIO()
        self.handler = logging.StreamHandler(self.buffer)
        from crawler_kit.log import _PlaceholderFormatter
        self.handler.setFormatter(_PlaceholderFormatter("%(message)s"))
        self.root.addHandler(self.handler)

    def tearDown(self) -> None:
        self.root.removeHandler(self.handler)
        self.root.handlers.clear()
        self.root.handlers.extend(self._saved)

    def test_brace_placeholder_is_formatted(self) -> None:
        """曾经把日志原样打印成 ``{}`` —— 因为只改了 record.message。"""
        get_logger("t").info("A {} B", 42)
        get_logger("t").info("C %s D", "x")
        lines = self.buffer.getvalue().strip().splitlines()
        self.assertEqual(lines[0], "A 42 B")
        self.assertEqual(lines[1], "C x D")

    def test_unmatched_placeholder_does_not_raise(self) -> None:
        get_logger("t").info("{} but only")
        get_logger("t").info("%s but none")
        self.assertIn("{} but only", self.buffer.getvalue())
        self.assertIn("%s but none", self.buffer.getvalue())

    def test_safe_key_distinguishes_boundaries(self) -> None:
        self.assertEqual(safe_key(1, 2), safe_key(1, 2))
        self.assertNotEqual(safe_key("a", "b|c"), safe_key("a|b", "c"))


class _StubSpider(Spider):
    name = "stub"

    def request(self, page: int) -> RequestSpec:
        return RequestSpec(url=f"http://a/b?page={page}")

    def parse(self, page: int, response: Response) -> Outcome:
        return Outcome(Verdict.OK, records=[])


if __name__ == "__main__":
    unittest.main(verbosity=2)

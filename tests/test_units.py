# -*- coding: utf-8 -*-
"""单元测试：断点、去重落盘、重试策略、限速器。

这些是纯逻辑，不需要网络，跑得很快 —— 端到端测试负责"服务端异常"，
这里负责"我们自己的账算得对不对"。
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler_kit import (  # noqa: E402
    Attempt,
    Checkpoint,
    FixedIntervalLimiter,
    JsonlDedupeStore,
    RetryPolicy,
    RetryRunner,
    TokenBucket,
    Verdict,
    iter_jsonl,
    parse_retry_after,
    read_json,
    read_jsonl,
    safe_key,
    write_csv,
)


class CheckpointTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_roundtrip(self) -> None:
        cp = Checkpoint(self.dir / "p.json", identity={"page_size": 20})
        self.assertIsNone(cp.load())
        self.assertTrue(cp.save(3))
        self.assertEqual(cp.load(), 3)

    def test_identity_mismatch_invalidates(self) -> None:
        path = self.dir / "p.json"
        Checkpoint(path, identity={"page_size": 20}).save(5)
        self.assertIsNone(Checkpoint(path, identity={"page_size": 50}).load())

    def test_info_does_not_participate_in_validation(self) -> None:
        """页范围属于 info：先跑 1~3 再扩到 1~6 不该让断点作废。"""
        path = self.dir / "p.json"
        Checkpoint(path, identity={"page_size": 20}, info={"last": 3}).save(3)
        cp = Checkpoint(path, identity={"page_size": 20}, info={"last": 6})
        self.assertEqual(cp.load(), 3)

    def test_monotonic_blocks_regression(self) -> None:
        path = self.dir / "p.json"
        cp = Checkpoint(path, identity={})
        cp.save(5)
        self.assertFalse(cp.save(2), "游标不该后退")
        self.assertEqual(cp.load(), 5)

    def test_corrupt_file_is_ignored(self) -> None:
        path = self.dir / "p.json"
        path.write_text("{ this is not json", encoding="utf-8")
        self.assertIsNone(Checkpoint(path, identity={}).load())

    def test_non_int_cursor_is_ignored(self) -> None:
        path = self.dir / "p.json"
        path.write_text('{"cursor": "3"}', encoding="utf-8")
        self.assertIsNone(Checkpoint(path, identity={}).load())


class StoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_dedupe_counts_and_reopen_is_idempotent(self) -> None:
        path = self.dir / "r.jsonl"
        records = [{"id": 1}, {"id": 2}, {"id": 1}]
        with JsonlDedupeStore(path, lambda r: str(r["id"])) as store:
            counts = store.add(records)
        self.assertEqual(counts, {"written": 2, "duplicates": 1})
        # 重新打开同一个文件：已见集合从日志重建，不会重复写
        with JsonlDedupeStore(path, lambda r: str(r["id"])) as store:
            counts = store.add(records)
        self.assertEqual(counts, {"written": 0, "duplicates": 3})
        self.assertEqual(len(read_jsonl(path)), 2)

    def test_half_written_last_line_is_skipped(self) -> None:
        path = self.dir / "r.jsonl"
        path.write_text('{"id": 1}\n{"id": 2}\n{"id":', encoding="utf-8")
        self.assertEqual([r["id"] for r in iter_jsonl(path)], [1, 2])

    def test_safe_key_does_not_collide(self) -> None:
        self.assertNotEqual(safe_key("a|b", "c"), safe_key("a", "b|c"))

    def test_write_csv_rewrites_whole_file(self) -> None:
        path = self.dir / "r.csv"
        write_csv(path, [{"id": 1}, {"id": 2}], ["id"])
        self.assertEqual(write_csv(path, [{"id": 3}], ["id"]), 1)
        text = path.read_text(encoding="utf-8-sig")
        self.assertNotIn("1", text.splitlines()[1] if len(text.splitlines()) > 1 else "")
        self.assertEqual(len(text.strip().splitlines()), 2, "表头 + 1 行")


class RetryTest(unittest.TestCase):
    def test_rate_limit_waits_server_hint_plus_margin(self) -> None:
        waits: list[float] = []
        responses = [Attempt(Verdict.RATE_LIMITED, wait=3.0),
                     Attempt(Verdict.OK, payload="done")]
        report = RetryRunner(RetryPolicy(margin=1.0), sleep=waits.append).run(
            lambda n: responses[n - 1])
        self.assertTrue(report.ok)
        self.assertEqual(waits, [4.0], "服务端说 3 秒 + 1 秒余量")

    def test_rate_limit_without_hint_escalates(self) -> None:
        waits: list[float] = []
        policy = RetryPolicy(fallback_wait=2.0, escalate=2.0, margin=0.0,
                             fallback_jitter=0.0, max_rate_limit_attempts=4)
        responses = [Attempt(Verdict.RATE_LIMITED)] * 3 + [Attempt(Verdict.OK)]
        report = RetryRunner(policy, sleep=waits.append).run(
            lambda n: responses[n - 1])
        self.assertTrue(report.ok)
        self.assertEqual(waits, [2.0, 4.0, 8.0], "兜底值必须递增")

    def test_fatal_stops_immediately(self) -> None:
        waits: list[float] = []
        report = RetryRunner(RetryPolicy(), sleep=waits.append).run(
            lambda n: Attempt(Verdict.FATAL, error="403"))
        self.assertFalse(report.ok)
        self.assertEqual(report.attempts, 1)
        self.assertEqual(waits, [], "FATAL 不该等待也不该重试")

    def test_retryable_respects_max_attempts(self) -> None:
        waits: list[float] = []
        report = RetryRunner(RetryPolicy(max_attempts=2), sleep=waits.append).run(
            lambda n: Attempt(Verdict.RETRYABLE, error="503"))
        self.assertFalse(report.ok)
        self.assertEqual(len(waits), 1, "2 次尝试 = 1 次退避")

    def test_parse_retry_after_priority(self) -> None:
        self.assertEqual(parse_retry_after({"errmsg": "请在 3 秒后重试"}), 3.0)
        self.assertEqual(parse_retry_after({"retry_after": 5}), 5.0)
        self.assertEqual(parse_retry_after(None, {"Retry-After": "7"}), 7.0)
        self.assertIsNone(parse_retry_after({"msg": "no hint"}))

    def test_parse_retry_after_ignores_bool(self) -> None:
        self.assertIsNone(parse_retry_after({"retry_after": True}))


class LimiterTest(unittest.TestCase):
    def test_token_bucket_rejects_bad_params(self) -> None:
        with self.assertRaises(ValueError):
            TokenBucket(capacity=0)
        with self.assertRaises(ValueError):
            TokenBucket(refill_per_sec=0)

    def test_fixed_interval_rejects_negative(self) -> None:
        with self.assertRaises(ValueError):
            FixedIntervalLimiter(-1)

    def test_policy_rejects_bad_values(self) -> None:
        with self.assertRaises(ValueError):
            RetryPolicy(max_attempts=0)
        with self.assertRaises(ValueError):
            RetryPolicy(escalate=0.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)

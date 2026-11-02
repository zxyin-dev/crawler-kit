# -*- coding: utf-8 -*-
"""落盘：事实日志、去重、CSV 导出。

核心思路是**把关注点分开**，每一件各自幂等：

| 关注点 | 做法 |
|---|---|
| 追加写会产生重复 | JSONL 是**事实日志**（只追加）；去重键从日志里重建 |
| 去重集合与日志不同步 | **不额外维护"已见集合"文件** —— 日志是唯一事实来源，单份状态不可能不同步 |
| 导出会产生重复 | CSV 每次从日志**整体重写**，不追加。输出是输入的纯函数 |
| 写了一半 | 写入后立刻 ``flush + fsync``，调用方随后推进断点就一定是安全的 |

``JsonlDedupeStore.add`` 返回 ``{"written": n, "duplicates": n}``，
让调用方能把"采到了多少"和"其中多少是重复"分开统计 ——
接口分页重叠是很常见的现象，把它显示出来比默默吞掉更有用。
"""

from __future__ import annotations

import csv
import json
import os
import threading
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

__all__ = [
    "read_jsonl",
    "iter_jsonl",
    "load_seen_keys",
    "JsonlDedupeStore",
    "write_csv",
    "safe_key",
]

KeyFunc = Callable[[Mapping[str, Any]], str]

# 用不可打印分隔符拼接复合键：用 "|" 之类的可打印字符时，
# "A|B" + "C" 与 "A" + "B|C" 会撞成同一个键。
KEY_SEP = "\x1f"


def safe_key(*parts: Any) -> str:
    """把若干字段拼成一个不会撞的复合去重键。"""
    return KEY_SEP.join("" if p is None else str(p) for p in parts)


def read_jsonl(path: str | os.PathLike) -> list[dict[str, Any]]:
    """读 JSONL，**容忍最后一行是半行**。

    进程被中断时经常留下一个写到一半的 JSON。这类残行应该被跳过，
    而不是让整个文件不可读 —— 否则一次中断就报废全部已采数据。
    """
    return list(iter_jsonl(path))


def iter_jsonl(path: str | os.PathLike) -> Iterator[dict[str, Any]]:
    """流式读 JSONL（大文件时比 :func:`read_jsonl` 省内存）。"""
    target = Path(path)
    if not target.exists():
        return
    with target.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict):
                yield record


def load_seen_keys(path: str | os.PathLike, key_func: KeyFunc) -> set[str]:
    """从已有日志重建去重键集合。"""
    return {key_func(record) for record in iter_jsonl(path)}


class JsonlDedupeStore:
    """按去重键过滤的 JSONL 追加写入器。

    参数
    ----
    key_func:
        从记录算出唯一键。复合键请用 :func:`safe_key` 构造。
    seen:
        预置的已见集合。默认从 ``path`` 里重建，因此**重新打开同一个文件
        不会重复写入**，重跑天然幂等。
    """

    def __init__(self, path: str | os.PathLike, key_func: KeyFunc,
                 seen: set[str] | None = None):
        self.path = Path(path)
        self.key_func = key_func
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.seen: set[str] = seen if seen is not None else load_seen_keys(
            self.path, key_func)
        self._fh = self.path.open("a", encoding="utf-8")
        # 自带锁：Crawler 在 _commit_lock 里调用 add，但 store 是公开类，
        # 别人直接并发调用时就没人兜底了 —— "已见集合"和文件指针都不是线程安全的。
        # 锁在类里，语义就完整；外层那把锁的职责是"写记录 + 统计 + 游标"的原子性。
        self._lock = threading.Lock()

    def add(self, records: Iterable[Mapping[str, Any]]) -> dict[str, int]:
        """写入记录，返回 ``{"written": n, "duplicates": n}``。"""
        written = duplicates = 0
        with self._lock:
            for record in records:
                key = self.key_func(record)
                if key in self.seen:
                    duplicates += 1
                    continue
                self.seen.add(key)
                self._fh.write(json.dumps(dict(record), ensure_ascii=False) + "\n")
                written += 1
            if written:
                self._fh.flush()
                os.fsync(self._fh.fileno())   # 先落盘，调用方才能安全推进断点
        return {"written": written, "duplicates": duplicates}

    def close(self) -> None:
        with self._lock:
            if not self._fh.closed:
                self._fh.close()

    def __enter__(self) -> "JsonlDedupeStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def write_csv(path: str | os.PathLike, rows: Iterable[Mapping[str, Any]],
              fieldnames: Sequence[str], *, encoding: str = "utf-8-sig") -> int:
    """**整体重写** CSV，返回写出的数据行数。

    * 整体重写（而非追加）是"输出幂等"的关键：跑一次和跑十次结果完全相同。
    * 默认 ``utf-8-sig``（带 BOM）：Excel 双击打开不乱码。无 BOM 的 UTF-8
      中文 CSV 用 Excel 直接打开会显示成乱码，这是中文环境里最常见的投诉。
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with target.open("w", encoding=encoding, newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(fieldnames),
                                extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))
            count += 1
    return count

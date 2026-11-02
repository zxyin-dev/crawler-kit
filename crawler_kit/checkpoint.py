# -*- coding: utf-8 -*-
"""断点续跑。

三条规则，每一条都对应一次真实的返工：

1. **进度不能领先于数据。**
   先落盘再推进断点。顺序反了就会出现"断点说这一页完成了、数据却没写进去"，
   而这种错误在续采时才暴露，且无法区分是漏采还是本来就没数据。

2. **只前进，不后退。**
   用 ``--restart`` 之类的方式重跑旧页时，断点不能被写回去。
   否则下一次续采会把已完成的区间重采一遍。

3. **参数变了就作废。**
   ``identity`` 是一组"这份断点属于哪个任务"的键值（比如 ``page_size``、
   页范围、批次名）。任一项对不上，断点直接忽略 —— 否则会拿一套参数下的进度
   去续另一套参数的任务，直接漏数据。

断点文件用"临时文件 + 原子替换"写，不会留下半个文件。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

__all__ = ["Checkpoint", "read_json", "atomic_write_json"]


def read_json(path: str | os.PathLike, default: Any = None) -> Any:
    """读 JSON；文件不存在或内容损坏时返回 ``default``（不抛异常）。

    容忍损坏是刻意的：断点文件损坏的代价应该是"这一轮从头来"，
    而不是"程序崩了"。
    """
    try:
        with Path(path).open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def atomic_write_json(path: str | os.PathLike, payload: Any, *, indent: int = 2) -> None:
    """原子写 JSON：先写 ``.tmp``，再 ``os.replace`` 覆盖。

    直接覆写原文件的问题是：写到一半进程被杀，留下一个语法都不合法的文件。
    ``os.replace`` 在同一文件系统内是原子的，读到的要么是旧的完整内容，
    要么是新的完整内容。
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=indent)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, target)


class Checkpoint:
    """一个整数游标 + 一组身份键的断点文件。

    参数
    ----
    path:
        断点文件路径。
    identity:
        这组断点属于哪个任务。任一项与文件里记录的不一致，断点作废。
        **只放"改变游标含义"的参数**：换了它会改变"第 N 页对应哪段数据"，
        所以必须作废。像"任务范围"这种只是本次要跑多少的东西应该放 ``info`` ——
        把范围放进 ``identity`` 会导致"先跑 1~3 页、再扩到 1~6 页"这种
        完全正常的续采被误判成换任务，然后从头重来。
    info:
        记录用字段：会写进文件，但**不参与校验**。适合放页范围、批次名、
        数据源之类的溯源信息，方便事后看懂这份断点是哪次任务的。
    monotonic:
        是否只前进不后退（默认 ``True``）。设 ``False`` 会让 ``save`` 允许回退，
        一般只在测试或特殊复位场景下需要。
    """

    def __init__(self, path: str | os.PathLike,
                 identity: Mapping[str, Any] | None = None, *,
                 info: Mapping[str, Any] | None = None, monotonic: bool = True):
        self.path = Path(path)
        self.identity = dict(identity or {})
        self.info = dict(info or {})
        self.monotonic = monotonic

    # -- 读 ---------------------------------------------------------------
    def state(self) -> dict[str, Any] | None:
        """读回完整状态（含身份键）。文件不可用或身份不符时返回 ``None``。"""
        raw = read_json(self.path)
        if not isinstance(raw, dict):
            return None
        for key, expected in self.identity.items():
            if raw.get(key) != expected:
                return None
        return raw

    def load(self) -> int | None:
        """读回游标；无断点、文件损坏或身份不符时返回 ``None``。"""
        state = self.state()
        if state is None:
            return None
        cursor = state.get("cursor")
        if isinstance(cursor, bool) or not isinstance(cursor, int):
            return None
        return cursor

    # -- 写 ---------------------------------------------------------------
    def save(self, cursor: int) -> bool:
        """写入游标。被单调性挡住时返回 ``False``。"""
        if not isinstance(cursor, int) or isinstance(cursor, bool):
            raise TypeError("cursor 必须是整数")
        if self.monotonic:
            previous = self.load()
            if previous is not None and cursor < previous:
                return False
        payload = {**self.identity, **self.info, "cursor": cursor}
        atomic_write_json(self.path, payload)
        return True

    def clear(self) -> None:
        """删除断点文件（忽略不存在的情况）。"""
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass

    def __repr__(self) -> str:
        return (f"Checkpoint({str(self.path)!r}, identity={self.identity!r}, "
                f"monotonic={self.monotonic})")

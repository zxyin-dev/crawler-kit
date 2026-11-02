# -*- coding: utf-8 -*-
"""签名插件：纯 Python 复算优先，Node 子进程桥兜底。

为什么是这个顺序
----------------
纯 Python 能算出来，就意味着**不需要运行环境**：不需要 node、不需要
几百 MB 的依赖、不需要担心版本升级后旧脚本跑不起来。所以能静态复算就静态复算。

算不出来时才走 Node 桥 —— 但桥本身也有坑，这里替调用方处理掉：

* **Windows 上不能用管道。** 沙箱/杀软会把管道句柄判为越权
  （``PermissionError: [WinError 5]``）。所以桥走"工作目录里的临时文件"：
  请求写进 ``in.json``，结果从 ``out.json`` 读。这个做法在淘宝项目里验证过。
* **子进程必须带超时。** 卡死的 node 进程会永久占住采集流程。
* **失败要能区分"签名器坏了"和"目标站拒绝"。** 前者是 ``FATAL``，
  后者可能是 ``RATE_LIMITED``；把它们混成一个异常就没法正确退避。

接口极简：``sign(spec) -> RequestSpec``。传进去的是完整请求，返回的也是完整请求，
因为签名经常要改 header、query、body 三处，只返回一个字符串是表达不了的。
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

from .spider import RequestSpec

__all__ = [
    "Signer",
    "PurePythonSigner",
    "NodeBridgeSigner",
    "make_node_signer",
    "SignerError",
]


class SignerError(RuntimeError):
    """签名过程本身失败（配置错、脚本崩、超时）。**不是目标站的回答。**"""


class Signer:
    """签名器协议：输入请求，输出请求。"""

    def sign(self, spec: RequestSpec) -> RequestSpec:  # pragma: no cover - 协议声明
        raise NotImplementedError

    def __call__(self, spec: RequestSpec) -> RequestSpec:
        return self.sign(spec)

    def __repr__(self) -> str:
        return f"<{type(self).__name__}>"


class PurePythonSigner(Signer):
    """用普通 Python 函数做签名。

    参数
    ----
    func:
        收到 :class:`RequestSpec`，返回 ``RequestSpec`` 或一个 ``dict``
        （``{"params": ..., "headers": ...}`` 之类的局部改动，框架会自动合并）。
    """

    def __init__(self, func: Callable[[RequestSpec], Any], name: str = ""):
        if not callable(func):
            raise TypeError("func 必须可调用")
        self.func = func
        self.name = name or getattr(func, "__name__", "signer")

    def sign(self, spec: RequestSpec) -> RequestSpec:
        result = self.func(spec)
        return _merge(spec, result)

    def __repr__(self) -> str:
        return f"<PurePythonSigner {self.name}>"


class NodeBridgeSigner(Signer):
    """把签名交给一个 Node 脚本。

    协议（两边都固定，避免各项目各写一套）：

    1. Python 把 ``{"url","method","params","headers","body","meta"}`` 写进
       ``<work_dir>/in.json``。
    2. 执行 ``node <script> <in.json> <out.json>``。
    3. Node 把 ``{"params","headers","body"}``（任选其一或全部）写进 ``out.json``。

    参数
    ----
    script:
        Node 脚本路径。
    work_dir:
        临时文件目录。**不要放在系统临时目录**：部分 Windows 环境下
        ``%TEMP%`` 被安全软件挡住，而工作目录里的读写是正常的。
    timeout:
        子进程超时秒数。超时即 ``SignerError``。
    node:
        可执行文件名，默认 ``node``（会先找 ``PATH``，再找常见安装位置）。
    """

    def __init__(self, script: str | os.PathLike, *, work_dir: str | os.PathLike,
                 timeout: float = 15.0, node: str | None = None,
                 extra_args: tuple[str, ...] = ()):
        self.script = Path(script)
        if not self.script.exists():
            raise SignerError(f"签名脚本不存在：{self.script}")
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = float(timeout)
        self.node = node or _find_node()
        self.extra_args = tuple(extra_args)

    # -- 主入口 -----------------------------------------------------------
    def sign(self, spec: RequestSpec) -> RequestSpec:
        stamp = f"{os.getpid()}-{int(time.time() * 1000)}-{_counter()}"
        in_path = self.work_dir / f"in-{stamp}.json"
        out_path = self.work_dir / f"out-{stamp}.json"
        try:
            in_path.write_text(json.dumps(spec.to_dict(), ensure_ascii=False),
                               encoding="utf-8")
            if out_path.exists():
                out_path.unlink()
            result = self._run(in_path, out_path)
            return _merge(spec, result)
        finally:
            for path in (in_path, out_path):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass

    def _run(self, in_path: Path, out_path: Path) -> dict[str, Any]:
        command = [self.node, str(self.script), str(in_path), str(out_path),
                   *self.extra_args]
        try:
            completed = subprocess.run(
                command, cwd=str(self.work_dir), timeout=self.timeout,
                capture_output=True, text=True,
                encoding="utf-8", errors="replace",
            )
        except subprocess.TimeoutExpired as exc:
            raise SignerError(f"Node 签名超时（{self.timeout:g}s）：{self.script}") from exc
        except OSError as exc:
            raise SignerError(f"无法启动 Node：{self.node}（{exc}）") from exc

        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "").strip()
            raise SignerError(
                f"Node 签名脚本退出码 {completed.returncode}：{detail[-800:]}")

        if not out_path.exists():
            raise SignerError(f"Node 未生成结果文件：{out_path}")
        try:
            payload = json.loads(out_path.read_text(encoding="utf-8"))
        except ValueError as exc:
            raise SignerError(f"Node 结果不是合法 JSON：{exc}") from exc
        if not isinstance(payload, dict):
            raise SignerError("Node 结果必须是 JSON 对象")
        return payload

    def __repr__(self) -> str:
        return f"<NodeBridgeSigner {self.script.name} node={self.node}>"


def make_node_signer(script: str | os.PathLike, *, work_dir: str | os.PathLike,
                     **kwargs: Any) -> NodeBridgeSigner:
    """便捷构造（保留这个函数名是为了以后能换成"自动挑纯算/桥"的策略）。"""
    return NodeBridgeSigner(script, work_dir=work_dir, **kwargs)


# -- 内部 -----------------------------------------------------------------
def _merge(spec: RequestSpec, result: Any) -> RequestSpec:
    """把签名器的返回值合并回请求。

    支持两种返回：完整 ``RequestSpec``，或局部 ``dict``。
    局部 dict 里 ``params`` / ``headers`` / ``meta`` 是**合并**而非替换 ——
    签名器通常只加几个字段，不该被迫把原有字段全部抄一遍。
    """
    if isinstance(result, RequestSpec):
        return result
    if not isinstance(result, dict):
        raise SignerError(f"签名器返回值必须是 RequestSpec 或 dict，收到 {type(result).__name__}")

    allowed = {"method", "url", "params", "headers", "body", "timeout", "meta"}
    unknown = set(result) - allowed
    if unknown:
        raise SignerError(f"签名器返回了未知字段：{sorted(unknown)}")

    merged_params = dict(spec.params)
    merged_headers = dict(spec.headers)
    merged_meta = dict(spec.meta)
    for key, value in (result.get("params") or {}).items():
        merged_params[key] = value
    for key, value in (result.get("headers") or {}).items():
        merged_headers[key] = value
    for key, value in (result.get("meta") or {}).items():
        merged_meta[key] = value

    return spec.replace(
        method=result.get("method", spec.method),
        url=result.get("url", spec.url),
        params=merged_params,
        headers=merged_headers,
        body=result.get("body", spec.body),
        timeout=result.get("timeout", spec.timeout),
        meta=merged_meta,
    )


_COUNTER = 0


def _counter() -> int:
    """同毫秒内的并发签名要去重文件名，否则两个请求会互相覆盖。"""
    global _COUNTER
    _COUNTER += 1
    return _COUNTER


def _find_node() -> str:
    """找 node 可执行文件：先 PATH，再常见安装位置。"""
    found = shutil.which("node")
    if found:
        return found
    if sys.platform == "win32":
        candidates = [
            Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "nodejs" / "node.exe",
            Path(os.environ.get("LOCALAPPDATA", "")) / "Programs" / "nodejs" / "node.exe",
        ]
    else:
        candidates = [Path("/usr/local/bin/node"), Path("/usr/bin/node")]
    for path in candidates:
        if path.exists():
            return str(path)
    raise SignerError("找不到 node 可执行文件；请传 node= 指定路径")

# -*- coding: utf-8 -*-
"""日志：零依赖、可复制、绝不因为日志把程序搞崩。

从练习项目里的 ``logkit`` 收进来的最小子集 —— 只保留采集框架真正需要的四件事：

1. **Windows 控制台默认 GBK**，打印中文/HTML 会抛 ``UnicodeEncodeError``。
   装配时统一 ``reconfigure(utf-8)``，业务代码不用再关心。
2. **两种占位符都能用**：loguru 的 ``{}`` 与标准库的 ``%s`` 混用在同一个仓库里
   是常态，本模块按「能格式化就格式化，不能就原样拼接」降级处理。
3. **日志里的行号必须指向真实调用点**，不是 logging 的调用点。
4. **写盘失败不抛异常**：降级成只打控制台，不让日志问题拖垮采集。

用法::

    from crawler_kit import setup_logging, get_logger

    setup_logging(level="INFO", log_dir="logs")   # 可选，不写也能用
    log = get_logger(__name__)
    log.info("第 {} 页完成（累计 {} 条）", page, total)

实现要点：handler 全部装在 **root logger** 上。装在自己的命名空间 logger 上
的话，``get_logger("mymodule")`` 拿到的 logger 不属于那个命名空间，
日志会一路冒泡到 root 的 ``lastResort`` —— 结果是只有 WARNING 以上能看见，
而且格式是默认的。
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Any

__all__ = ["get_logger", "setup_logging", "set_level", "LoggerAdapter"]

_CONFIGURED = False


def _safe_stream(stream: Any) -> Any:
    """把 stdout/stderr 切成 UTF-8；不支持 reconfigure 的平台原样返回。"""
    reconfigure = getattr(stream, "reconfigure", None)
    if reconfigure is not None:
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError, AttributeError):
            pass
    return stream


def _apply_args(message: str, args: tuple) -> str:
    """按占位符风格选格式化方式；对不上就原样拼接。"""
    try:
        if "{" in message and "}" in message:
            return message.format(*args)
        if "%" in message:
            return message % args
    except (IndexError, KeyError, ValueError, TypeError):
        pass
    try:
        return message + " " + " ".join(str(a) for a in args)
    except Exception:
        return message


class _PlaceholderFormatter(logging.Formatter):
    """同时支持 ``{}`` 与 ``%s``，任何格式化失败都退回原样拼接。

    格式化结果必须写回 ``record.msg`` 并把 ``record.args`` 清空：
    只改 ``record.message`` 没用 —— ``Formatter.format`` 会重新调用
    ``getMessage()`` 覆盖它，结果日志里原样打印出 ``{}``。
    """

    def format(self, record: logging.LogRecord) -> str:
        try:
            message = record.getMessage()
        except Exception:
            message = str(record.msg)
        args = record.__dict__.pop("fmt_args", None)
        if args:
            message = _apply_args(message, args)
        record.msg = message
        record.args = ()
        record.message = message
        return super().format(record)


class _CallSiteFilter(logging.Filter):
    """把 ``pathname/lineno`` 改回真实调用点（跳过本模块自身的栈帧）。"""

    _HERE = os.path.normcase(os.path.abspath(__file__))

    def filter(self, record: logging.LogRecord) -> bool:
        pathname = os.path.normcase(os.path.abspath(record.pathname or ""))
        if pathname == self._HERE:
            frame = sys._getframe(1)
            while frame is not None:
                filename = os.path.normcase(os.path.abspath(frame.f_code.co_filename))
                if filename != self._HERE:
                    record.pathname = frame.f_code.co_filename
                    record.filename = os.path.basename(frame.f_code.co_filename)
                    record.lineno = frame.f_lineno
                    record.funcName = frame.f_code.co_name
                    break
                frame = frame.f_back
        return True


def setup_logging(*, level: str = "INFO", log_dir: str | os.PathLike | None = None,
                  log_file: str = "crawler.log", backup_count: int = 7,
                  console: bool = True, quiet: bool = False) -> logging.Logger:
    """装配 root 日志。重复调用是安全的（只装配一次，之后只调级别）。"""
    global _CONFIGURED
    root = logging.getLogger()
    if _CONFIGURED:
        set_level("ERROR" if quiet else level)
        return root

    root.setLevel(logging.DEBUG)
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.handlers.clear()

    formatter = _PlaceholderFormatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    callsite = _CallSiteFilter()

    if console and not quiet:
        console_handler = logging.StreamHandler(_safe_stream(sys.stdout))
        console_handler.setFormatter(formatter)
        console_handler.addFilter(callsite)   # 过滤器装在 handler 上才对所有 logger 生效
        root.addHandler(console_handler)

    if log_dir:
        path = Path(log_dir) / log_file
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.handlers.TimedRotatingFileHandler(
                filename=str(path), when="midnight", interval=1,
                backupCount=backup_count, encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            file_handler.addFilter(callsite)
            root.addHandler(file_handler)
        except OSError:
            # 落盘不可用只降级，不影响采集主流程
            pass

    _CONFIGURED = True
    set_level("ERROR" if quiet else level)
    return root


def set_level(level: str) -> None:
    root = logging.getLogger()
    numeric = getattr(logging, level.upper(), logging.INFO)
    for handler in root.handlers:
        handler.setLevel(numeric)


class LoggerAdapter:
    """``logger.info("x {}", a)`` 的薄包装：参数原样传下去，由 Formatter 决定格式。"""

    __slots__ = ("_log", "_bound")

    def __init__(self, log: logging.Logger, bound: dict[str, Any] | None = None):
        self._log = log
        self._bound = bound or {}

    # -- 内部 -------------------------------------------------------------
    def _emit(self, level: int, message: str, args: tuple, **kwargs: Any) -> None:
        if not self._log.isEnabledFor(level):
            return
        exc_info = kwargs.pop("exc_info", None)
        stacklevel = kwargs.pop("stacklevel", 2)
        extra = dict(self._bound)
        if args:
            extra["fmt_args"] = args
        self._log.log(level, message, exc_info=exc_info, stacklevel=stacklevel,
                      extra=extra)

    # -- 级别 -------------------------------------------------------------
    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._emit(logging.DEBUG, message, args, **kwargs)

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._emit(logging.INFO, message, args, **kwargs)

    def success(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._emit(logging.INFO, message, args, **kwargs)

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._emit(logging.WARNING, message, args, **kwargs)

    def error(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._emit(logging.ERROR, message, args, **kwargs)

    def exception(self, message: str, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("exc_info", True)
        self._emit(logging.ERROR, message, args, **kwargs)

    def critical(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._emit(logging.CRITICAL, message, args, **kwargs)

    def bind(self, **fields: Any) -> "LoggerAdapter":
        return LoggerAdapter(self._log, {**self._bound, **fields})


def get_logger(name: str = "crawler_kit") -> LoggerAdapter:
    """取一个带调用点修正与占位符兼容的 logger。"""
    if not _CONFIGURED:
        setup_logging()
    return LoggerAdapter(logging.getLogger(name))

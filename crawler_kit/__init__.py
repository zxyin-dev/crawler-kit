# -*- coding: utf-8 -*-
"""crawler-kit —— 自用采集框架。

从三个真实项目（sign-lab / ratelimit-kit / gdzy-collector）和十余个练习项目里
沉淀出来的同一条主线：**采集器的难点不是"能跑一次"，而是"能长期跑"。**

五条硬规则，每条都对应一次真实返工：

1. **限速必须全局**（``limiter``）
   —— "每个请求前 sleep 一下"在并发下会叠乘成 N 倍速度，等于没限速。
2. **重试只由编排层决策**（``retry`` + ``crawler``）
   —— 请求层自己重试会与框架自动重连叠加成连点，越重试越打不开。
3. **进度不能领先于数据**（``checkpoint``）
   —— 先 fsync 落盘，再推进断点；只推进"连续完成"的部分。
4. **输出要是输入的纯函数**（``store``）
   —— JSONL 只追加、去重键从日志重建、CSV 整体重写，跑一次和跑十次结果相同。
5. **HTTP 状态码是数据，不是异常**（``transport``）
   —— 4xx 抛异常就丢掉了"这是限流还是真拒绝"的信息，只能一概重试。

插件点只有三个，其余全部内建：

* ``spider``  —— 怎么生成第 N 页请求、怎么解析响应（业务唯一要写的东西）
* ``signer``  —— 纯 Python 复算优先，Node 子进程桥作兜底
* ``transport`` —— 默认 urllib（零依赖），需要 TLS 指纹时换成 curl_cffi 实现

快速上手
--------
```python
from crawler_kit import Crawler, Config, Spider, RequestSpec, Outcome

class MySpider(Spider):
    name = "demo"
    page_size = 20

    def request(self, page: int) -> RequestSpec:
        return RequestSpec(url=f"http://127.0.0.1:8899/items?page={page}")

    def parse(self, page, response) -> Outcome:
        return Outcome(records=response.json()["items"])

Crawler(MySpider(), Config(work_dir="data")).run(first=1, last=10)
```
"""

from __future__ import annotations

from .checkpoint import Checkpoint, atomic_write_json, read_json
from .crawler import Config, Crawler, PageStat, RunReport
from .limiter import (FixedIntervalLimiter, Limiter, NoLimiter, RateLimitGate,
                      TokenBucket, make_limiter)
from .log import get_logger, setup_logging
from .retry import (
    Attempt,
    Backoff,
    RetryPolicy,
    RetryReport,
    RetryRunner,
    Verdict,
    parse_retry_after,
)
from .signer import (
    NodeBridgeSigner,
    PurePythonSigner,
    Signer,
    SignerError,
    make_node_signer,
)
from .spider import (
    Outcome,
    RequestSpec,
    Response,
    Spider,
    StdoutSpider,
    json_field,
    to_items,
)
from .store import (
    JsonlDedupeStore,
    iter_jsonl,
    load_seen_keys,
    read_jsonl,
    safe_key,
    write_csv,
)
from .transport import (CurlCffiTransport, HttpTransport, Transport,
                        TransportError)

__version__ = "1.1.1"

__all__ = [
    "__version__",
    # 编排
    "Crawler",
    "Config",
    "RunReport",
    "PageStat",
    # 插件
    "Spider",
    "StdoutSpider",
    "RequestSpec",
    "Outcome",
    "Response",
    "Verdict",
    "Signer",
    "PurePythonSigner",
    "NodeBridgeSigner",
    "make_node_signer",
    "SignerError",
    # 限速
    "Limiter",
    "TokenBucket",
    "RateLimitGate",
    "FixedIntervalLimiter",
    "make_limiter",
    # 重试
    "Attempt",
    "RetryPolicy",
    "RetryReport",
    "RetryRunner",
    "Backoff",
    "parse_retry_after",
    # 落盘
    "Checkpoint",
    "JsonlDedupeStore",
    "read_jsonl",
    "iter_jsonl",
    "load_seen_keys",
    "write_csv",
    "safe_key",
    "read_json",
    "atomic_write_json",
    # 传输
    "HttpTransport",
    "CurlCffiTransport",
    "Transport",
    "TransportError",
    # 工具
    "json_field",
    "to_items",
    "get_logger",
    "setup_logging",
]

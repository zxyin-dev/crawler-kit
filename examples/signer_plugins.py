# -*- coding: utf-8 -*-
"""示例：签名插件。纯 Python 复算优先，Node 桥兜底。

这是整个框架里最容易被做错的一环，所以两个要点写在这里：

**要点一：签名不该"顺手发请求"。**
很多现成写法里，签名函数自己把请求发了（因为要拿 cookie、要试探）。
那样一来签名和采集就绑死，限速、退避、断点全部绕过。这里的契约是
``sign(spec) -> spec``：只改请求，不发请求。

**要点二：优先纯 Python 复算。**
Node 桥能跑，但有三个长期成本：目标方换算法时要重下脚本、每次调用多 30~80ms
子进程开销、部署机上要有 node。所以只要算法能复算（MD5/HMAC/AES 这类），
就用 :class:`PurePythonSigner`。

什么时候不得不走 Node 桥：算法被混淆成 JSVMP、或者依赖浏览器环境
（``window``/``document``/``navigator``）。那时把整个脚本搬进 node 比补环境便宜。

跑法::

    python examples/signer_plugins.py           # 只验证纯 Python 路径
    python examples/signer_plugins.py --node     # 额外验证 Node 桥（需装 node）
"""

from __future__ import annotations

import hashlib
import hmac
import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler_kit import (  # noqa: E402
    Config,
    Crawler,
    Outcome,
    PurePythonSigner,
    RequestSpec,
    SignerError,
    Spider,
    Verdict,
    make_node_signer,
    setup_logging,
)

SECRET = "demo-secret"


# ---------------------------------------------------------------------------
# 纯 Python 签名：把算法复算成 Python
# ---------------------------------------------------------------------------
def sign_params(spec: RequestSpec) -> dict:
    """给 ``params`` 加 ``ts`` 和 ``sign``。

    规则（假设目标站是这样）：把参数按键名排序拼成 ``k=v&k=v``，
    再拼上密钥做 HMAC-SHA256，取十六进制小写。

    真实项目里这条规则是从 JS 里读出来的；一旦读出来，Python 版本就比
    "每次调 node 跑那段混淆代码"稳定得多 —— 它不会因为对方发新版脚本而失效。
    """
    params = dict(spec.params or {})
    params["ts"] = "1700000000"        # 真实项目用 int(time.time())
    canonical = "&".join(f"{k}={params[k]}" for k in sorted(params))
    digest = hmac.new(SECRET.encode(), canonical.encode(), hashlib.sha256)
    return {"params": {**params, "sign": digest.hexdigest()}}


# ---------------------------------------------------------------------------
# Node 桥：脚本放在工作目录，通过临时文件交换数据
# ---------------------------------------------------------------------------
NODE_SCRIPT = r"""
// node sign.js in.json out.json
// 读入 {url, method, params, headers, body}，写出同样的结构（可只写改动部分）。
const fs = require('fs');
const crypto = require('crypto');

const [, , inPath, outPath] = process.argv;
const req = JSON.parse(fs.readFileSync(inPath, 'utf8'));

const params = { ...(req.params || {}), ts: String(Date.now()).slice(0, 10) };
const canonical = Object.keys(params).sort()
  .map(k => `${k}=${params[k]}`).join('&');
const sign = crypto.createHmac('sha256', 'demo-secret')
  .update(canonical).digest('hex');

fs.writeFileSync(outPath, JSON.stringify({ params: { ...params, sign } }));
"""


class DemoSpider(Spider):
    """假业务插件：只要证明签名被插进去了。"""

    name = "signer-demo"

    def request(self, page: int) -> RequestSpec:
        return RequestSpec(url="http://example.com/api", params={"page": page})

    def parse(self, page: int, response) -> Outcome:
        return Outcome(Verdict.OK, records=[])


def check_pure_python() -> int:
    print("=" * 60)
    print("纯 Python 签名")
    signer = PurePythonSigner(sign_params, name="hmac-sha256")
    spec = RequestSpec(url="http://example.com/api", params={"page": 1})
    signed = signer.sign(spec)
    print("  原始 params:", spec.params)
    print("  签名后 params:", signed.params)
    assert "sign" in signed.params, "签名没插进去"
    assert signed.url == spec.url, "签名不该改 URL"
    print("  OK")
    return 0


def check_node_bridge(work_dir: Path) -> int:
    print("=" * 60)
    print("Node 桥")
    node = shutil.which("node")
    if node is None:
        print("  跳过：PATH 里没有 node")
        return 0

    script = work_dir / "sign.js"
    script.write_text(NODE_SCRIPT, encoding="utf-8")
    signer = make_node_signer(script, work_dir=work_dir, timeout=10.0)
    spec = RequestSpec(url="http://example.com/api", params={"page": 2})
    signed = signer.sign(spec)
    print("  签名后 params:", signed.params)
    assert "sign" in signed.params
    print("  OK")
    return 0


def check_node_failure_is_loud(work_dir: Path) -> int:
    """脚本出错时必须抛 SignerError，而不是悄悄返回未签名的请求。

    悄悄返回未签名请求的后果是：目标站返回 403，被判决成 FATAL，
    于是所有页都失败、断点一动不动，而日志里看不出是签名脚本坏了。
    """
    print("=" * 60)
    print("Node 脚本出错时必须显式失败")
    node = shutil.which("node")
    if node is None:
        print("  跳过：PATH 里没有 node")
        return 0

    bad = work_dir / "bad.js"
    bad.write_text("throw new Error('boom');", encoding="utf-8")
    signer = make_node_signer(bad, work_dir=work_dir, timeout=10.0)
    try:
        signer.sign(RequestSpec(url="http://example.com/api"))
    except SignerError as exc:
        print("  如预期抛 SignerError:", str(exc).splitlines()[0])
        print("  OK")
        return 0
    print("  失败：没有抛异常")
    return 1


def main() -> int:
    setup_logging(level="WARNING")
    failures = check_pure_python()
    with tempfile.TemporaryDirectory() as tmp:
        work_dir = Path(tmp)
        failures += check_node_bridge(work_dir)
        failures += check_node_failure_is_loud(work_dir)
    print("=" * 60)
    print("全部通过" if not failures else f"{failures} 项失败")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

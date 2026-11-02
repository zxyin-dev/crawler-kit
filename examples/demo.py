# -*- coding: utf-8 -*-
"""最小可跑示例：对着本地假站采 4 页。

跑法::

    python examples/demo.py

第一次跑完会得到 ``examples/_demo_data/`` 下的四个文件。
再跑一次就是续采 —— 应该输出"跳过 4 页、写入 0 条"。
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tests"))

from crawler_kit import (  # noqa: E402
    Config,
    Crawler,
    Outcome,
    RequestSpec,
    Spider,
    Verdict,
    setup_logging,
)
from mock_target import MockTarget  # noqa: E402


class DemoSpider(Spider):
    """把假站的分页接口包成业务插件。"""

    name = "demo"
    page_size = 10
    end_page = 4

    def __init__(self, target: MockTarget):
        self.target = target

    def request(self, page: int) -> RequestSpec:
        return RequestSpec(url=f"{self.target.url()}?page={page}")

    def parse(self, page: int, response) -> Outcome:
        payload = response.json() or {}
        return Outcome(Verdict.OK, records=payload.get("items", []))


def main() -> int:
    setup_logging(level="INFO", log_dir=Path(__file__).parent / "_demo_data" / "logs")

    with MockTarget(total=35) as target:
        spider = DemoSpider(target)
        config = Config(
            work_dir=Path(__file__).parent / "_demo_data",
            min_interval=0.5,          # 假站在本地，但依然走完整限速路径
        )
        report = Crawler(spider, config).run(first=1, last=spider.end_page)

    print()
    print("报告：", report.summary())
    print("CSV：", report.csv_path)
    return 0 if not report.failed_pages else 1


if __name__ == "__main__":
    raise SystemExit(main())

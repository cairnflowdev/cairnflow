# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""An order service: durable state, a durable timer, a side effect that runs once.

    python example/order_service.py          # the SDK listens on :9080
    cairn register http://127.0.0.1:9080
    cairn invoke OrderService submit --key order-42 --input '{"amount": 100}'
"""

from __future__ import annotations

import asyncio
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cairnflow import Context, TerminalError, VirtualObject, serve  # noqa: E402

orders = VirtualObject("OrderService")


@dataclass
class Order:
    amount: int


def charge(amount: int) -> str:
    """The side effect: a payment provider in real life."""
    print(f"charging {amount}")
    return f"receipt-{amount}"


@orders.handler()
async def submit(ctx: Context, order: Order) -> dict[str, object]:
    if order.amount <= 0:
        raise TerminalError("amount must be positive", 400)  # bad data: retrying is useless
    total = (await ctx.get("total")) or 0  # None = the key does not exist
    await ctx.sleep(1)  # durable: the timer lives in the store
    receipt = await ctx.run("charge", charge, order.amount)  # runs once per durable result
    ctx.set("total", total + order.amount)
    return {"receipt": receipt, "total": total + order.amount}


@orders.handler(kind="shared")
async def total(ctx: Context) -> int:
    return (await ctx.get("total")) or 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve([orders], port=9080))

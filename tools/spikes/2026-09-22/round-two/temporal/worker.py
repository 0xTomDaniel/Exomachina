"""One unchanged worker serves every accepted document revision in this spike."""

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from factory import FactoryRun, assign, deliver, review


async def main() -> None:
    client = await Client.connect(os.environ["EXO_TEMPORAL_ADDRESS"], namespace="exomachina")
    async with Worker(
        client,
        task_queue="factory-interpreter",
        workflows=[FactoryRun],
        activities=[assign, review, deliver],
    ):
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())

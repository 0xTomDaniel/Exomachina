"""One unchanged Python interpreter worker serves every published definition."""

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from adapter import assign, release, review, synthesize, typed_join
from factory import FactoryRun


async def main() -> None:
    client = await Client.connect(os.environ["EXO_TEMPORAL_ADDRESS"], namespace="exomachina")
    async with Worker(
        client,
        task_queue="arbitration-temporal",
        workflows=[FactoryRun],
        activities=[assign, typed_join, synthesize, review, release],
        workflow_failure_exception_types=[ValueError],
    ):
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())

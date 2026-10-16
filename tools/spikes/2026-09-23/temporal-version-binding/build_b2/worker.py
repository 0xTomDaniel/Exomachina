"""One immutable interpreter build polls one Worker Deployment Version."""

import asyncio
import os
import signal
from pathlib import Path

from temporalio.client import Client
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.worker import Worker, WorkerDeploymentConfig

from adapter import assign, release, review, synthesize_v2, typed_join
from binding import DEPLOYMENT, QUEUE, source_digest
from factory import BUILD_ID, FactoryRun


async def main() -> None:
    expected = os.environ["EXO_WORKER_SOURCE_DIGEST"]
    if source_digest(Path(__file__).resolve().parent) != expected:
        raise ValueError("worker source does not match published build")
    client = await Client.connect(os.environ["EXO_TEMPORAL_ADDRESS"], namespace="exomachina")
    stopped = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stopped.set)
    async with Worker(
        client,
        task_queue=QUEUE,
        workflows=[FactoryRun],
        activities=[assign, typed_join, synthesize_v2, review, release],
        deployment_config=WorkerDeploymentConfig(
            version=WorkerDeploymentVersion(DEPLOYMENT, BUILD_ID),
            use_worker_versioning=True,
            default_versioning_behavior=VersioningBehavior.PINNED),
    ):
        await stopped.wait()


if __name__ == "__main__":
    asyncio.run(main())

"""One immutable interpreter build polls one Worker Deployment Version."""

import asyncio
import os
import signal
from pathlib import Path

from temporalio.client import Client
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.worker import Worker, WorkerDeploymentConfig

from adapter import assign, release, review, synthesize, typed_join
from binding import DEPLOYMENT, NAMESPACE, QUEUE, build_id_for, source_digest
from buildinfo import BUILD_ID, SOURCE_DIGEST
from factory import FactoryRun


async def main() -> None:
    if (not BUILD_ID or not SOURCE_DIGEST or BUILD_ID != build_id_for(SOURCE_DIGEST)
            or source_digest(Path(__file__).resolve().parent) != SOURCE_DIGEST):
        raise ValueError("worker source does not match published build")
    client = await Client.connect(os.environ["EXO_TEMPORAL_ADDRESS"], namespace=NAMESPACE)
    stopped = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stopped.set)
    async with Worker(
        client,
        task_queue=QUEUE,
        workflows=[FactoryRun],
        activities=[assign, typed_join, synthesize, review, release],
        workflow_failure_exception_types=[ValueError],
        deployment_config=WorkerDeploymentConfig(
            version=WorkerDeploymentVersion(DEPLOYMENT, BUILD_ID),
            use_worker_versioning=True,
            default_versioning_behavior=VersioningBehavior.PINNED),
    ):
        await stopped.wait()


if __name__ == "__main__":
    asyncio.run(main())

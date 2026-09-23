"""Pinned no-Docker Hatchet embedded startup smoke for this spike."""

import os
import threading
import time

from hatchet_sdk import ClientConfig, Context, EmbeddedHatchetConfig, EmptyModel, Hatchet


hatchet = Hatchet.from_embedded(
    ClientConfig(
        embedded=EmbeddedHatchetConfig(
            version="v0.107.0",
            postgres_data_dir=os.environ["HATCHET_SPIKE_DATA_DIR"],
            start_api=False,
        )
    )
)


@hatchet.task(name="exomachina-round-two-smoke")
def greet(input: EmptyModel, ctx: Context) -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    worker = hatchet.worker("exomachina-round-two-smoke-worker", workflows=[greet])
    threading.Thread(target=worker.start, daemon=True).start()
    time.sleep(2)
    try:
        print(greet.run(EmptyModel()), flush=True)
    finally:
        hatchet.stop_embedded()
    os._exit(0)

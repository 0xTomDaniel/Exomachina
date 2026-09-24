"""Immutable worker build identity supplied when its process starts."""
import os


BUILD_ID = os.environ.get("EXO_WORKER_BUILD_ID", "")
SOURCE_DIGEST = os.environ.get("EXO_WORKER_SOURCE_DIGEST", "")

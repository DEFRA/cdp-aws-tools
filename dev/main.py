"""Local-only entrypoint: the app backed by an in-memory SQS fake.

The Docker image only copies app/, so this never ships.
"""

from functools import lru_cache

from app.main import app
from app.sqs import queues
from dev.sqs_stub import InMemorySqsStub

queues.get_sqs_client = lru_cache(InMemorySqsStub.from_env)

__all__ = ["app"]

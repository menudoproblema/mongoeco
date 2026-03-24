from pathlib import Path


__path__ = [str(Path(__file__).resolve().parent.parent / "src" / "mongoeco")]

from .api import AsyncMongoClient, MongoClient
from ._version import __version__
from .session import ClientSession
from .types import ObjectId


__all__ = ["AsyncMongoClient", "MongoClient", "ClientSession", "ObjectId", "__version__"]

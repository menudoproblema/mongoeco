"""Ephemeral constructor injection: exercise owner code with explicit profiles."""
import os
from functools import wraps

def pytest_configure(config):
    import cosecha.provider.mongodb.provider as provider
    import mochuelo_testkit.utils as testkit
    from mongoeco.engines import MemoryEngine, SQLiteEngine
    dialect = os.environ.get('CONSUMER_MONGODB_DIALECT', '9.0')
    profile = os.environ.get('CONSUMER_PYMONGO_PROFILE', '4.18')
    sync = provider.MongoEcoClient
    asynchronous = testkit.AsyncMongoClient
    @wraps(sync)
    def sync_factory(*args, **kwargs):
        kwargs.update(mongodb_dialect=dialect, pymongo_profile=profile)
        client = sync(*args, **kwargs)
        assert client.mongodb_dialect.key == dialect
        assert client.pymongo_profile.key == profile
        return client
    @wraps(asynchronous)
    def async_factory(*args, **kwargs):
        kwargs.update(mongodb_dialect=dialect, pymongo_profile=profile)
        if not args and 'engine' not in kwargs:
            kwargs['engine'] = SQLiteEngine() if os.environ.get('CONSUMER_TESTKIT_ENGINE') == 'sqlite' else MemoryEngine()
        client = asynchronous(*args, **kwargs)
        assert client.mongodb_dialect.key == dialect
        assert client.pymongo_profile.key == profile
        return client
    provider.MongoEcoClient = sync_factory
    testkit.AsyncMongoClient = async_factory

import json

import msgspec

from mongoeco import (
    MongoClient,
    export_cxp_catalog,
    export_cxp_profile_catalog,
    export_cxp_profile_support_catalog,
)
from mongoeco.engines.memory import MemoryEngine


def _pretty(value: object) -> str:
    return json.dumps(msgspec.to_builtins(value), indent=2)


def main() -> None:
    print('Canonical CXP catalog:')
    print(_pretty(export_cxp_catalog()))
    print('\nReusable CXP profiles:')
    print(_pretty(export_cxp_profile_catalog()))
    print('\nReusable CXP profile support:')
    print(_pretty(export_cxp_profile_support_catalog()))

    with MongoClient(MemoryEngine()) as client:
        collection = client.get_database('demo').get_collection('items')
        collection.insert_many(
            [
                {'_id': 1, 'score': 8},
                {'_id': 2, 'score': 3},
            ]
        )
        explain = collection.aggregate([{'$match': {'score': {'$gte': 8}}}]).explain()
        print('\nCXP projection from aggregate(...).explain():')
        print(_pretty(explain['cxp']))


if __name__ == '__main__':
    main()

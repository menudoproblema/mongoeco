import json

import msgspec

from mongoeco import (
    MongoClient,
    export_cxp_catalog,
    export_cxp_operation_catalog,
    export_cxp_profile_catalog,
    export_cxp_profile_support_catalog,
)
from mongoeco.engines.memory import MemoryEngine


def _pretty(value: object) -> str:
    return json.dumps(msgspec.to_builtins(value), indent=2)


def _require_profile(profile_name: str) -> None:
    support = export_cxp_profile_support_catalog()[profile_name]
    if bool(support['supported']):
        print(f'profile {profile_name!r}: supported')
        return
    print(f'profile {profile_name!r}: unsupported')
    for message in support['validation']['messages']:
        print(f'  - {message}')


def main() -> None:
    print('Canonical CXP catalog:')
    print(_pretty(export_cxp_catalog()))
    print('\nReusable CXP profiles:')
    print(_pretty(export_cxp_profile_catalog()))
    print('\nReusable CXP profile support:')
    print(_pretty(export_cxp_profile_support_catalog()))
    print('\nOperation-centric CXP view:')
    print(
        _pretty(
            {
                'find': export_cxp_operation_catalog()['find'],
                'update_one': export_cxp_operation_catalog()['update_one'],
                'aggregate': export_cxp_operation_catalog()['aggregate'],
            }
        )
    )
    print('\nSimple profile gating:')
    _require_profile('mongodb-core')
    _require_profile('mongodb-text-search')
    _require_profile('mongodb-search')

    with MongoClient(MemoryEngine()) as client:
        collection = client.get_database('demo').get_collection('items')
        collection.insert_many(
            [
                {'_id': 1, 'score': 8},
                {'_id': 2, 'score': 3},
            ]
        )
        find_explain = collection.find({'score': {'$gte': 3}}).explain()
        explain = collection.aggregate([{'$match': {'score': {'$gte': 8}}}]).explain()
        write_metadata = export_cxp_catalog()['capabilities']['write']['metadata'][
            'operationMetadata'
        ]['update_one']
        print('\nCXP projection from find(...).explain():')
        print(_pretty(find_explain['cxp']))
        print('\nCXP projection from aggregate(...).explain():')
        print(_pretty(explain['cxp']))
        print('\nWrite operation metadata for tooling:')
        print(_pretty(write_metadata))


if __name__ == '__main__':
    main()

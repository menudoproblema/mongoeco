import asyncio,json,sys
from pathlib import Path
root=Path('/Users/uve/Proyectos/mongoeco');sys.path.insert(0,str(root))
from mongoeco import MongoClient,AsyncMongoClient
from tests.support import ENGINE_FACTORIES
from tests.differential.write_identity_cases import IDENTITY_CASES
WRITE_OPERATION_CASES = [c for c in IDENTITY_CASES if c.id.startswith(("null_id_", "regex_id_", "embedded_regex_id_", "falsy_id_"))]
from tests.support_write_parity import observe_write_case,observe_async_write_case
expected=json.loads(Path('/tmp/mongoeco-id-edges/native-8.0/write_identity_mongodb_8_0.json').read_text())['observations']
results=[]
for name,factory in ENGINE_FACTORIES.items():
    with MongoClient(factory(),mongodb_dialect='8.0',pymongo_profile='4.19') as client:
        for case in WRITE_OPERATION_CASES:
            actual=observe_write_case(client.baseline,case.id,case)
            results.append({'engine':name,'facade':'sync','case':case.id,'matches':actual==expected[case.id]})
    async def check():
        async with AsyncMongoClient(factory(),mongodb_dialect='8.0',pymongo_profile='4.19') as client:
            for case in WRITE_OPERATION_CASES:
                actual=await observe_async_write_case(client.baseline,case.id,case)
                results.append({'engine':name,'facade':'async','case':case.id,'matches':actual==expected[case.id]})
    asyncio.run(check())
Path('/tmp/mongoeco-id-edges/baseline.json').write_text(json.dumps({'runtime':__import__('mongoeco').__file__,'runs':results},indent=2)+'\n')
print(len(results),'observations;',sum(not row['matches'] for row in results),'divergences')

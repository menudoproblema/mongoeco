import sys,json,asyncio,hashlib
from pathlib import Path
sys.path.insert(0,'/Users/uve/Proyectos/mongoeco')
from mongoeco import MongoClient,AsyncMongoClient
from tests.support import ENGINE_FACTORIES
from tests.differential.write_identity_cases import IDENTITY_CASES
from tests.differential.write_query_cases import RAW_QUERY_CASES
from tests.support_write_parity import observe_write_case,observe_async_write_case,observe_raw_write,observe_async_raw_write
out=Path('/tmp/mongoeco-id-conversion');root=Path('/Users/uve/Proyectos/mongoeco')
cases=[c for c in IDENTITY_CASES if c.id.startswith('id_conversion_')]
rows=[]
async def arun(factory,case,raw):
 async with AsyncMongoClient(factory(),mongodb_dialect='8.0',pymongo_profile='4.19') as c:
  return await (observe_async_raw_write if raw else observe_async_write_case)(c.baseline,case.id,case)
for raw,corpus,filename in ((False,cases,'write_identity'),(True,[c for c in cases if c.requests[0].method!='bulk_write'],'write_identity')):
 refs=json.loads((root/f'tests/fixtures/{filename}_mongodb_8_0.json').read_text())['raw_observations' if raw else 'observations']
 for name,factory in ENGINE_FACTORIES.items():
  for surface in ('sync','async'):
   for case in corpus:
    try:
     if surface=='sync':
      with MongoClient(factory(),mongodb_dialect='8.0',pymongo_profile='4.19') as c:actual=(observe_raw_write if raw else observe_write_case)(c.baseline,case.id,case)
     else:actual=asyncio.run(arun(factory,case,raw))
    except Exception as exc: actual={'observer_exception':type(exc).__name__,'message':str(exc)}
    rows.append({'case':case.id,'engine':name,'surface':surface,'raw':raw,'matches':actual==refs[case.id],'actual':actual,'expected':refs[case.id]})
(out/'baseline.json').write_text(json.dumps({'artifact_sha256':hashlib.sha256(Path('/tmp/mongoeco-query-id/dist/mongoeco-4.9.1-py3-none-any.whl').read_bytes()).hexdigest(),'observations':rows},indent=2)+'\n')
print(len(rows),sum(not r['matches'] for r in rows),'divergences')

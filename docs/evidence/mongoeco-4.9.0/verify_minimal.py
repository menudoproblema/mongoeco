import asyncio,importlib.util,json,sys
from pathlib import Path
import mongoeco
from mongoeco import MongoClient,AsyncMongoClient
from mongoeco.engines import MemoryEngine,SQLiteEngine
assert 'site-packages' in Path(mongoeco.__file__).resolve().parts
absent=['pymongo','bson','orjson','hypothesis']
for name in absent:assert importlib.util.find_spec(name) is None,name
pipeline=[{'$densify':{'field':'n','range':{'step':1,'bounds':[0,3]}}},{'$sort':{'n':1}}]
checks=[]
for engine_type in (MemoryEngine,SQLiteEngine):
 for dialect,profile in [('7.0','4.9'),('9.0','4.18')]:
  with MongoClient(engine_type(),mongodb_dialect=dialect,pymongo_profile=profile) as client:
   c=client.minimal.records;c.insert_many([{'_id':i,'n':i} for i in (-1,1,4)])
   assert [doc['n'] for doc in c.aggregate(pipeline)]==[-1,0,1,2,4]
   assert c.create_index('_id')=='_id_1'
  async def exercise():
   async with AsyncMongoClient(engine_type(),mongodb_dialect=dialect,pymongo_profile=profile) as client:
    c=client.minimal.records;await c.insert_many([{'_id':i,'n':i} for i in (-1,1,4)])
    assert [doc['n'] for doc in await c.aggregate(pipeline).to_list()]==[-1,0,1,2,4]
  asyncio.run(exercise());checks.append({'engine':engine_type.__name__,'dialect':dialect,'profile':profile,'sync_async':'passed'})
result={'version':mongoeco.__version__,'module':mongoeco.__file__,'optional_modules_absent':absent,'checks':checks}
Path(sys.argv[1]).write_text(json.dumps(result,indent=2)+'\n');print(result)

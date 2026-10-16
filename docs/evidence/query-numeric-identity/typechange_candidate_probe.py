import json,sys
from pathlib import Path
import pymongo
from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from bson import json_util,Int64,Decimal128
out=Path('/tmp/mongoeco-query-id');rows=[]
def probe(client):
 coll=client.review_numeric_type.c
 results=[]
 try:
  for v in (1.0,Int64(1),Decimal128('1')):
   for mode in ('set','replace','pipeline'):
    coll.drop();coll.insert_one({'_id':1,'x':0})
    try:
     r=coll.replace_one({'_id':1},{'_id':v,'x':1}) if mode=='replace' else coll.update_one({'_id':1},[{'$set':{'_id':v,'x':1}}] if mode=='pipeline' else {'$set':{'_id':v,'x':1}})
     result={'matched':r.matched_count,'modified':r.modified_count}
    except Exception as exc:result={'family':type(exc).__name__,'code':getattr(exc,'code',None)}
    results.append({'type':type(v).__name__,'mode':mode,'result':result,'documents':list(coll.find({}))})
 finally:client.drop_database('review_numeric_type')
 return results
for target,port in [('7.0',37567),('8.0',37568),('9.0',37569)]:
 with pymongo.MongoClient(f'mongodb://127.0.0.1:{port}',serverSelectionTimeoutMS=3000) as c:rows.append({'target':target,'engine':'native','observations':probe(c)})
 for engine in (MemoryEngine,SQLiteEngine):
  with MongoClient(engine(),mongodb_dialect=target,pymongo_profile='4.19') as c:rows.append({'target':target,'engine':engine.__name__,'observations':probe(c)})
(out/'typechange-followup.json').write_text(json_util.dumps(rows,json_options=json_util.CANONICAL_JSON_OPTIONS,indent=2)+'\n')
print('Independent explicit ID type changes recorded; follow-up outside duplicate/query fix')

import hashlib,json,platform,sys,time,statistics,resource,tempfile
from pathlib import Path
from unittest.mock import patch
import mongoeco
from mongoeco import MongoClient
from mongoeco.engines import MemoryEngine,SQLiteEngine
from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
assert 'site-packages' in Path(mongoeco.__file__).resolve().parts
size=20000; documents=[{'_id':i,'n':i} for i in range(size)]
pipeline=[{'$set':{'next':{'$add':['$n',1]}}}]
expected=[{**doc,'next':doc['n']+1} for doc in documents]
result={'version':mongoeco.__version__,'module':mongoeco.__file__,'python':platform.python_version(),'size':size,'warmup':1,'repetitions':5,'dialect':'7.0','profile':'4.9','batchSize':128,'oracle':'full document multiset including multiplicity; no sort stage means natural order is not promised','harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'dataset_sha256':hashlib.sha256(json.dumps(documents).encode()).hexdigest(),'rss_limit':'ru_maxrss is a cumulative process high-water mark, not per-workload peak or a memory bound','engines':{}}
for name,engine in [('memory',MemoryEngine()),('sqlite',SQLiteEngine())]:
 before=set(Path(tempfile.gettempdir()).glob('mongoeco-*'))
 with MongoClient(engine,mongodb_dialect='7.0',pymongo_profile='4.9') as client:
  coll=client.test.records;coll.insert_many(documents)
  cursor=coll.aggregate(pipeline,batch_size=128)
  try:explanation=cursor.explain()
  finally:cursor.close()
  assert explanation['streaming_batch_execution'],explanation
  calls=[];original=AsyncAggregationCursor._open_batch_stream
  async def observed(self):calls.append(True);return await original(self)
  def consume():
   cursor=coll.aggregate(pipeline,batch_size=128)
   try:return list(cursor)
   finally:cursor.close()
  with patch.object(AsyncAggregationCursor,'_open_batch_stream',observed):
   assert sorted(consume(),key=lambda d:d["_id"])==expected
   wall=[];cpu=[]
   for _ in range(5):
    t=time.perf_counter_ns();c=time.process_time_ns();actual=consume();cpu.append(time.process_time_ns()-c);wall.append(time.perf_counter_ns()-t);assert sorted(actual,key=lambda d:d["_id"])==expected
  assert len(calls)==6,calls
  result['engines'][name]={'plan':explanation,'open_batch_stream_calls':len(calls),'result_sha256':hashlib.sha256(json.dumps(sorted(actual,key=lambda d:d["_id"])).encode()).hexdigest(),'wall_ns':wall,'cpu_ns':cpu,'wall_median_ns':statistics.median(wall),'cpu_median_ns':statistics.median(cpu),'process_maxrss':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
 after=set(Path(tempfile.gettempdir()).glob('mongoeco-*'));assert after==before,after-before
Path(sys.argv[1]).write_text(json.dumps(result,indent=2)+'\n')
print(result['version'],{k:(v['open_batch_stream_calls'],v['wall_median_ns']) for k,v in result['engines'].items()})

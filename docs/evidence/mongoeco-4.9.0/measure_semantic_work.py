import hashlib,json,sys,time,statistics,platform,os
from pathlib import Path
from unittest.mock import patch
import mongoeco
from mongoeco import MongoClient,IndexModel
from mongoeco.engines import MemoryEngine,SQLiteEngine
from mongoeco.core.aggregation import preparation,scalar_expressions

assert 'site-packages' in Path(mongoeco.__file__).resolve().parts
output=Path(sys.argv[1]);dialect=sys.argv[2] if len(sys.argv)>2 else '7.0'
documents=[{'_id':i,'n':i,'input':'10'} for i in range(1000)]
pipeline=[{'$match':{'$expr':{'$eq':['$n','$$wanted']}}},{'$project':{'n':1}}]
convert=[{'$project':{'v':{'$convert':{'input':'$input','to':'int'}}}}]
query={'$expr':{'$eq':[{'$convert':{'input':'$input','to':'int'}},10]}}
result={'schema':'mongoeco-semantic-work-measurement/1','version':mongoeco.__version__,'module':mongoeco.__file__,'python':platform.python_version(),'dialect':dialect,'profile':'4.9','size':1000,'warmup':1,'repetitions':5,'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'dataset_sha256':hashlib.sha256(json.dumps(documents,sort_keys=True).encode()).hexdigest(),'engines':{}}
for backend,engine_type in (('memory',MemoryEngine),('sqlite',SQLiteEngine)):
 engine=engine_type()
 with MongoClient(engine,mongodb_dialect=dialect,pymongo_profile='4.9') as client:
  records=client.test.records; records.insert_many(documents);records.create_index('n')
  actions={
   'prepared_aggregation':lambda:list(records.aggregate(pipeline,let={'wanted':2})),
   'ordinary_convert':lambda:list(records.aggregate(convert)),
   'direct_find_convert':lambda:list(records.find(query)),
   'ordinary_index_recreation':lambda:records.create_index('n'),
  }
  expected={'prepared_aggregation':[{'_id':2,'n':2}], 'ordinary_convert':[{'_id':i,'v':10} for i in range(1000)], 'direct_find_convert':documents,'ordinary_index_recreation':'n_1'}
  counts={}
  with patch.object(preparation,'_require_stage',wraps=preparation._require_stage) as stages, patch.object(preparation,'_copy_spec',wraps=preparation._copy_spec) as copies:
   assert actions['prepared_aggregation']()==expected['prepared_aggregation'];counts['stage_parse_calls']=stages.call_count;counts['copy_walk_calls']=copies.call_count
  if hasattr(preparation,'validate_convert_spec'):
   with patch.object(preparation,'validate_convert_spec',wraps=preparation.validate_convert_spec) as static,patch.object(scalar_expressions,'validate_convert_spec',wraps=scalar_expressions.validate_convert_spec) as direct:
    assert actions['ordinary_convert']()==expected['ordinary_convert'];counts['convert_static_validations']=static.call_count;counts['convert_direct_validations']=direct.call_count
   with patch.object(scalar_expressions,'validate_convert_spec',wraps=scalar_expressions.validate_convert_spec) as direct:
    assert actions['direct_find_convert']()==expected['direct_find_convert'];counts['find_convert_direct_validations']=direct.call_count
  else:
   counts.update(convert_static_validations=None,convert_direct_validations=None,find_convert_direct_validations=None)
   counts['convert_counter_limit']='Published 4.8.1 uses inline validation; the new helper counters are unavailable, not zero.'
  with patch.object(engine,'list_indexes',wraps=engine.list_indexes) as reads:
   assert actions['ordinary_index_recreation']()==expected['ordinary_index_recreation'];counts['ordinary_index_list_reads']=reads.call_count
  with patch.object(engine,'list_indexes',wraps=engine.list_indexes) as reads:
   records.create_indexes([IndexModel(f'k{i}') for i in range(3)]);counts['ordinary_batch_list_reads']=reads.call_count
  measurements={}
  for name,action in actions.items():
   assert action()==expected[name]
   wall=[];cpu=[]
   for _ in range(5):
    start=time.perf_counter_ns();cpu_start=time.process_time_ns();actual=action();cpu.append(time.process_time_ns()-cpu_start);wall.append(time.perf_counter_ns()-start)
    assert actual==expected[name],name
   measurements[name]={'wall_ns':wall,'cpu_ns':cpu,'wall_median_ns':statistics.median(wall),'cpu_median_ns':statistics.median(cpu),'result_sha256':hashlib.sha256(json.dumps(expected[name],sort_keys=True).encode()).hexdigest()}
  result['engines'][backend]={'counts':counts,'measurements':measurements}
output.write_text(json.dumps(result,indent=2)+'\n');print(result['version'],dialect,{k:v['counts'] for k,v in result['engines'].items()})

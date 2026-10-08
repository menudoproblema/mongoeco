from pathlib import Path
from unittest.mock import patch
import runpy,sys,json
import mongoeco.driver.execution as execution
import mongoeco.driver._runtime_attempts as attempts
base=Path('/private/tmp/mongoeco-490-20261008');calls=[]
async def unexpected(*args,**kwargs):
 calls.append(True)
 raise AssertionError('Changed driver pipeline reached: prior supplementary evidence cannot be reused')
with patch.object(execution,'execute_request_pipeline',unexpected),patch.object(attempts,'execute_request_pipeline',unexpected):
 for label,script,args in [('semantic7','measure_semantic_work.py',['7.0']),('semantic9','measure_semantic_work.py',['9.0']),('streaming','measure_streamed_aggregation.py',[])]:
  sys.argv=[str(base/script),str(base/f'causal-{label}-observations.json'),*args]
  runpy.run_path(str(base/script),run_name='__main__')
  print(label,'changed module unexecuted',flush=True)
assert not calls
(base/'performance-supplemental-causal-reuse.json').write_text(json.dumps({'changed_execution_pipeline_calls':0,'profiles':['7.0','9.0'],'engines':['memory','sqlite'],'verified':['preparation','ordinary_convert','direct_find_convert','ordinary_index_recreation','actual_streaming_batches'],'prior_harnesses_unchanged':True,'measurement_outputs':'causal-*-observations.json; diagnostic fresh samples do not replace the archived three-round comparison'},indent=2)+'\n')

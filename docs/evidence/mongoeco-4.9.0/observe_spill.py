import sys,json,tempfile
from pathlib import Path
from unittest.mock import patch
import mongoeco
assert 'site-packages' in Path(mongoeco.__file__).resolve().parts
from mongoeco.core.aggregation.spill import AggregationSpillPolicy
sys.path.insert(0,'/Users/uve/Proyectos/mongoeco')
from benchmarks.engines.mongoeco_mem import MongoecoMemoryEngine
from benchmarks.engines.mongoeco_sql import MongoecoSQLEngine
from benchmarks.runners.workloads import aggregation_spill_diagnostics
calls=[]; original=AggregationSpillPolicy.open_group_spool

def observed(self,*args,**kwargs):
 calls.append({'threshold':self.threshold,'phase':'open_group_spool'})
 return original(self,*args,**kwargs)
results={};before=set(Path(tempfile.gettempdir()).glob('mongoeco-*'))
with patch.object(AggregationSpillPolicy,'open_group_spool',observed):
 for name,engine_type in [('memory',MongoecoMemoryEngine),('sqlite',MongoecoSQLEngine)]:
  start=len(calls);metrics=aggregation_spill_diagnostics(engine_type(),20000)
  results[name]={'group_spool_open_calls':calls[start:],'tasks':{k:v.get('metadata') for k,v in metrics.items()}}
assert results['memory']['group_spool_open_calls'], 'Memory spill must be observed, not inferred from a threshold'
assert results['sqlite']['group_spool_open_calls'], 'SQLite Python group fallback must actually spill'
after=set(Path(tempfile.gettempdir()).glob('mongoeco-*'));assert after==before,after-before
Path(sys.argv[1]).write_text(json.dumps({'version':mongoeco.__version__,'size':20000,'results':results,'new_temp_files_after_cleanup':[]},indent=2)+'\n')
print(mongoeco.__version__,{k:len(v['group_spool_open_calls']) for k,v in results.items()})

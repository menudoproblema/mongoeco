from pathlib import Path
import sys,json,zipfile,hashlib,os,tempfile
from unittest.mock import patch
import mongoeco
import mongoeco.driver.execution as execution
import mongoeco.driver._runtime_attempts as attempts
base=Path('/private/tmp/mongoeco-490-20261008');old=base/'dist-delivery-first/mongoeco-4.9.0-py3-none-any.whl';new=base/'dist-callback-first/mongoeco-4.9.0-py3-none-any.whl'
with zipfile.ZipFile(old) as a,zipfile.ZipFile(new) as b:
 files=[p for p in a.namelist() if p.startswith('mongoeco/') and not p.endswith('/')]
 changed=[p for p in files if a.read(p)!=b.read(p)]
assert changed==['mongoeco/driver/execution.py'],changed
sys.path.insert(0,'/Users/uve/Proyectos/mongoeco')
from benchmarks.engines.mongoeco_mem import MongoecoMemoryEngine
from benchmarks.engines.mongoeco_sql import MongoecoSQLEngine
from benchmarks.runners.workloads import simple_aggregation,materializing_aggregation,aggregation_spill_diagnostics,secondary_lookup_indexed,cursor_consumption
calls=[]
async def unexpected(*args,**kwargs):
 calls.append(True)
 raise AssertionError('Changed driver pipeline reached by performance workload; timings must be repeated')
rows=[]
with patch.object(execution,'execute_request_pipeline',unexpected),patch.object(attempts,'execute_request_pipeline',unexpected):
 for engine_name,engine_type in [('memory',MongoecoMemoryEngine),('sqlite',MongoecoSQLEngine)]:
  for workload in [simple_aggregation,materializing_aggregation,aggregation_spill_diagnostics,secondary_lookup_indexed,cursor_consumption]:
   workload(engine_type(),20000);rows.append({'engine':engine_name,'workload':workload.__name__,'size':20000,'changed_execution_pipeline_calls':0})
assert not calls
receipt={'measured_wheel_sha256':hashlib.sha256(old.read_bytes()).hexdigest(),'final_wheel_sha256':hashlib.sha256(new.read_bytes()).hexdigest(),'only_changed_package_files':changed,'actual_final_package_import':mongoeco.__file__,'unchanged_measured_modules_byte_identical':len(files)-len(changed),'workloads':rows,'decision':'Original timed samples remain applicable to the same effective workload implementation; final driver pipeline proven unexecuted. Verification run is not substituted for warmup/five-repetition timing.'}
(base/'performance-causal-reuse.json').write_text(json.dumps(receipt,indent=2)+'\n');print(receipt)

import subprocess,os,json,concurrent.futures
from pathlib import Path
out=Path('/tmp/mongoeco-query-id');root=Path('/Users/uve/Proyectos/mongoeco')
def run(v):
 py=str(Path('/tmp/mongoeco-python315')/('venv'if v=='315'else'py'+v)/'bin/python');cmd=[py,'-m','pytest','-q','tests/integration/api/test_numeric_identity_conservation.py','-W','error::ResourceWarning','--tb=short']
 with(out/f'test-cleanup-{v}.log').open('w')as log:p=subprocess.run(cmd,cwd=root,env={**os.environ,'MONGOECO_TEST_INSTALLED_ARTIFACT':'1'},stdout=log,stderr=subprocess.STDOUT)
 print(v,p.returncode,flush=True);return {'python':v,'command':cmd,'exit_code':p.returncode,'log':f'test-cleanup-{v}.log'}
with concurrent.futures.ThreadPoolExecutor(3)as pool:rows=list(pool.map(run,('313','314','315')))
(out/'test-cleanup-results.json').write_text(json.dumps(rows,indent=2)+'\n');assert all(r['exit_code']==0 for r in rows)

import concurrent.futures,subprocess,os
from pathlib import Path
out=Path('/tmp/mongoeco-query-id');root=Path('/Users/uve/Proyectos/mongoeco')
def run(target):
 env={**os.environ,'MONGOECO_REAL_MONGODB_URI':f'mongodb://127.0.0.1:{37567+int(target[0])-7}'}
 with (out/f'capture-api_option-{target}.log').open('w') as log:
  p=subprocess.run(['/tmp/mongoeco-python315/venv/bin/python','scripts/capture_api_option_parity.py','--target',target,'--output',f'tests/fixtures/api_options_mongodb_{target.replace(".","_")}.json'],cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT)
 print(target,p.returncode,flush=True);return p.returncode
with concurrent.futures.ThreadPoolExecutor(3) as pool: assert not any(pool.map(run,('7.0','8.0','9.0')))

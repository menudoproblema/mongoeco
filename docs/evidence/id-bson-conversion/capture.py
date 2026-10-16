import concurrent.futures,subprocess,os
from pathlib import Path
root=Path('/Users/uve/Proyectos/mongoeco');out=Path('/tmp/mongoeco-id-conversion')
def run(target):
 with(out/f'capture-write-{target}.log').open('w')as log:
  result=subprocess.run(['/tmp/mongoeco-python315/venv/bin/python','scripts/capture_write_parity.py','--target',target,'--output-dir','tests/fixtures','--histories-dir',str(out/'histories')],cwd=root,env={**os.environ,'MONGOECO_REAL_MONGODB_URI':f'mongodb://127.0.0.1:{37567+int(target[0])-7}'},stdout=log,stderr=subprocess.STDOUT)
 print(target,result.returncode,flush=True);return result.returncode
with concurrent.futures.ThreadPoolExecutor(3)as pool:assert not any(pool.map(run,('7.0','8.0','9.0')))

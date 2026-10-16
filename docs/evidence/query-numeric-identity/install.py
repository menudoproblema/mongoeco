import subprocess,json,concurrent.futures
from pathlib import Path
out=Path('/tmp/mongoeco-query-id')
def install(v):
 py=str(Path('/tmp/mongoeco-python315')/('venv' if v=='315' else 'py'+v)/'bin/python')
 with (out/f'install-{v}.log').open('w') as log:
  for cmd in ([py,'-m','pip','install','--no-deps','--force-reinstall',str(out/'dist/mongoeco-4.9.1-py3-none-any.whl')],[py,'-m','pip','check']):subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,check=True)
 (out/f'packages-{v}.json').write_bytes(subprocess.check_output([py,'-m','pip','list','--format=json']))
 print(v,'installed and pip check passed',flush=True)
with concurrent.futures.ThreadPoolExecutor(3) as pool:list(pool.map(install,('313','314','315')))

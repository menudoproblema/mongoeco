import subprocess,concurrent.futures
from pathlib import Path
root=Path('/Users/uve/Proyectos/mongoeco');out=Path('/tmp/mongoeco-id-conversion')
def run(v):
 for gate in ('pytest','contracts'):
  subprocess.run([str(root/'.venv/bin/python'),str(out/'run_gate.py'),gate,v],check=True,cwd=root)
with concurrent.futures.ThreadPoolExecutor(3) as pool:list(pool.map(run,('313','314','315')))

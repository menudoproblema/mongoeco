from concurrent.futures import ThreadPoolExecutor
import json,os,subprocess
from pathlib import Path
out=Path('/tmp/mongoeco-id-edges');root=Path('/Users/uve/Proyectos/mongoeco')

def exercise(version):
    python=str(Path('/tmp/mongoeco-python315')/('venv' if version=='315' else 'py'+version)/'bin/python')
    for gate in ('unittest','smoke-wheel','smoke-sdist'):
        subprocess.run([str(root/'.venv/bin/python'),str(out/'run_gate.py'),gate,version],cwd=root,check=True)
    results=[]
    for engine in ('MemoryEngine','SQLiteEngine'):
        command=[python,'-m','mongoeco.conformance','mongoeco.engines:'+engine,'--profile','spi-v2-core,spi-v2-atomicity,spi-v2-clock,spi-v2-snapshots,spi-v2-change-delivery,search-v1','--require-success','--format','json','--output',str(out/f'conformance-{engine}-{version}.json')]
        with (out/f'conformance-{engine}-{version}.log').open('w') as log:r=subprocess.run(command,cwd=out,stdout=log,stderr=subprocess.STDOUT)
        results.append({'command':command,'exit_code':r.returncode,'engine':engine})
        if r.returncode:raise RuntimeError(command)
    (out/f'conformance-{version}-result.json').write_text(json.dumps(results,indent=2)+'\n')
    if version=='314':
        subprocess.run([str(root/'.venv/bin/python'),str(out/'run_gate.py'),'deep',version],cwd=root,check=True)
        for engine in ('memory-sync','sqlite-sync'):
            command=[python,'-m','benchmarks.run','--engine',engine,'--size','250','--warmup','0','--repetitions','1','--workload','search_diagnostics','--workload','vector_search_diagnostics','--format','json']
            with (out/f'benchmark-{engine}-314.json').open('w') as log:subprocess.run(command,cwd=root,stdout=log,check=True)
    print('POST-MATRIX',version,'PASS',flush=True)
with ThreadPoolExecutor(max_workers=3) as pool:
    list(pool.map(exercise,('313','314','315')))

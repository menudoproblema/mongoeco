import os,subprocess,shutil
from pathlib import Path
out=Path('/tmp/mongoeco-write-ops')
env={**os.environ,'SOURCE_DATE_EPOCH':subprocess.check_output(['git','log','-1','--pretty=%ct'],text=True).strip()}
for suffix in ('first','second'):
    with (out/f'repro-{suffix}.log').open('w') as log:
        subprocess.run(['.venv/bin/python','-m','build','--no-isolation','--sdist','--wheel','--outdir',str(out/f'dist-{suffix}')],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
        subprocess.run(['.venv/bin/python','scripts/normalize_sdist.py',str(out/f'dist-{suffix}/mongoeco-4.9.1.tar.gz')],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
for name in ('mongoeco-4.9.1-py3-none-any.whl','mongoeco-4.9.1.tar.gz'):
    assert (out/'dist-first'/name).read_bytes()==(out/'dist-second'/name).read_bytes()
    shutil.copyfile(out/'dist-first'/name,out/'dist'/name)
print('Both final distributions reproduce byte for byte')

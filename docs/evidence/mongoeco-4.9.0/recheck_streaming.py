from pathlib import Path
import subprocess,os
b=Path('/private/tmp/mongoeco-490-20261008')
for round,lanes in [(2,['candidate','baseline']),(3,['baseline','candidate'])]:
 for lane in lanes:
  tmp=b/f'tmp-streaming-recheck-{round}-{lane}';tmp.mkdir(exist_ok=True)
  with (b/f'streaming-recheck-{round}-{lane}.log').open('w') as log:
   p=subprocess.run([str(b/f'benchmark-{lane}-env/bin/python'),str(b/'measure_streamed_aggregation.py'),str(b/f'streaming-recheck-{round}-{lane}.json')],cwd='/private/tmp',env=dict(os.environ,TMPDIR=str(tmp)),stdout=log,stderr=subprocess.STDOUT)
  print(round,lane,p.returncode,flush=True)
  if p.returncode:raise SystemExit(p.returncode)

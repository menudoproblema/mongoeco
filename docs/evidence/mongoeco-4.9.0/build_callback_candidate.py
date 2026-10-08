from pathlib import Path
import hashlib,json,os,shutil,subprocess,sys
root=Path('/Users/uve/Proyectos/mongoeco')
base=Path('/private/tmp/mongoeco-490-20261008')
(base/'evidence').mkdir(exist_ok=True)
files=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=root).decode().split('\0')
epoch=subprocess.check_output(['git','log','-1','--format=%ct'],cwd=root,text=True).strip()
for lane in ['first','second']:
 checkout=base/f'build-source-callback-{lane}'
 if checkout.exists(): raise RuntimeError(f'Refusing to overwrite {checkout}')
 checkout.mkdir()
 for relative in files:
  if not relative: continue
  p=root/relative
  if p.is_file():
   target=checkout/relative
   target.parent.mkdir(parents=True,exist_ok=True)
   shutil.copy2(p,target)
 python=Path('/private/tmp/mongoeco-support-20261008')/f'build-env-{lane}/bin/python'
 env=dict(os.environ,SOURCE_DATE_EPOCH=epoch)
 with (base/'evidence'/f'build-callback-{lane}.log').open('w') as log:
  subprocess.run([str(python),'-m','build','--no-isolation','--sdist','--wheel','--outdir',str(base/f'dist-callback-{lane}')],cwd=checkout,stdout=log,stderr=subprocess.STDOUT,env=env,check=True)
  subprocess.run([str(python),str(root/'scripts/normalize_sdist.py'),str(base/f'dist-callback-{lane}'/'mongoeco-4.9.0.tar.gz')],stdout=log,stderr=subprocess.STDOUT,env=env,check=True)
 print('Built and normalized isolated',lane,flush=True)
checks={}
for p in (base/'dist-callback-first').iterdir():
 if not p.is_file(): continue
 q=base/'dist-callback-second'/p.name
 assert p.read_bytes()==q.read_bytes(),p.name
 checks[p.name]=hashlib.sha256(p.read_bytes()).hexdigest()
print('Identical artifacts',checks,flush=True)
(base/'evidence'/'distribution-callback-hashes.json').write_text(json.dumps(checks,indent=2)+'\n')
subprocess.run(['/private/tmp/mongoeco-support-20261008/build-env-first/bin/python','-m','twine','check',*[str(p) for p in (base/'dist-callback-first').iterdir()]],check=True)

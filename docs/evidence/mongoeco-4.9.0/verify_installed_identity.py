import json,sys,hashlib,zipfile,importlib.metadata,platform
from pathlib import Path
import mongoeco
wheel=Path(sys.argv[1]); output=Path(sys.argv[2]); package=Path(mongoeco.__file__).resolve().parent
assert 'site-packages' in package.parts,package
with zipfile.ZipFile(wheel) as z:
 entries={}
 for n in z.namelist():
  if n.startswith('mongoeco/') and not n.endswith('/'):
   raw=z.read(n); p=package.parent/n
   assert p.read_bytes()==raw,n
   entries[n]=hashlib.sha256(raw).hexdigest()
result={'python':platform.python_version(),'version':mongoeco.__version__,'module_file':str(package/'__init__.py'),'wheel':str(wheel),'wheel_sha256':hashlib.sha256(wheel.read_bytes()).hexdigest(),'package_files':entries,'distributions':dict(sorted((d.metadata['Name'].lower(),d.version) for d in importlib.metadata.distributions()))}
output.write_text(json.dumps(result,indent=2)+'\n')
print(result['version'],result['module_file'],result['wheel_sha256'],len(entries))

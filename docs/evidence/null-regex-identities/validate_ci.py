import ast,hashlib,json,re,subprocess,tomllib,zipfile
from pathlib import Path
from packaging.requirements import Requirement
import yaml
root=Path('/Users/uve/Proyectos/mongoeco');out=Path('/tmp/mongoeco-id-edges')
config=tomllib.loads((root/'pyproject.toml').read_text())
assert config['project']['requires-python']=='>=3.13'
supported=[value.rsplit(' :: ',1)[1] for value in config['project']['classifiers'] if re.fullmatch(r'Programming Language :: Python :: 3\.\d+',value)]
workflow=yaml.safe_load((root/'.github/workflows/ci.yml').read_text());job=workflow['jobs']['test']
assert job['strategy']['matrix']['python-version']==supported==['3.13','3.14','3.15']
assert not job.get('continue-on-error',False)
assert 'test' in workflow['jobs']['publish']['needs']
assert job['env']['MONGOECO_TEST_INSTALLED_ARTIFACT']=='1'
steps=job['steps']
assert next(step for step in steps if step['name']=='Run complete pytest suite')['if']=="matrix.python-version != '3.14'"
assert next(step for step in steps if step['name']=='Enforce installed package coverage floor')['if']=="matrix.python-version == '3.14'"
count=0;pyblocks=0
for wf in (root/'.github/workflows').glob('*.yml'):
    data=yaml.safe_load(wf.read_text())
    for item in data.get('jobs',{}).values():
        for step in item.get('steps',[]):
            body=step.get('run')
            if body:
                parsed=subprocess.run(['bash','-n'],input=body,text=True,capture_output=True)
                assert parsed.returncode==0,(wf,step['name'],parsed.stderr)
                for python in re.findall(r"python - <<'PY'\n(.*?)\nPY",body,re.S):
                    ast.parse(python);pyblocks+=1
                count+=1
wheel=out/'dist/mongoeco-4.9.1-py3-none-any.whl'
with zipfile.ZipFile(wheel) as archive:
    metadata=archive.read('mongoeco-4.9.1.dist-info/METADATA').decode()
    assert 'Requires-Python: >=3.13' in metadata
    for version in supported:assert f'Classifier: Programming Language :: Python :: {version}' in metadata
requirements=[Requirement(text) for text in config['project']['dependencies']]
for name,minimum,pin in [('usearch','2.26.4','2.26.4'),('shapely','2.2','2.2.0')]:
    req=next(req for req in requirements if req.name==name)
    assert req.marker is None and minimum in req.specifier
    constraint=[line for line in (root/'requirements/ci-constraints.txt').read_text().splitlines() if line.startswith(name+'==')]
    assert constraint==[name+'=='+pin]
    for version in ('313','314','315'):
        packages={p['name'].lower():p['version'] for p in json.loads((out/f'packages-{version}.json').read_text())}
        assert packages[name]==pin
sha=lambda file:hashlib.sha256(file.read_bytes()).hexdigest()
for name in ('mongoeco-4.9.1-py3-none-any.whl','mongoeco-4.9.1.tar.gz'):
    assert sha(out/'dist-first'/name)==sha(out/'dist-second'/name)==sha(out/'dist'/name)
report={'python_versions':supported,'release_gate_required':True,'installation_minimum':'>=3.13','common_pins':{'usearch':'2.26.4','shapely':'2.2.0'},'shell_blocks_parsed':count,'python_blocks_parsed':pyblocks,'artifacts_reproduce':True,'wheel_sha256':sha(wheel),'sdist_sha256':sha(out/'dist/mongoeco-4.9.1.tar.gz')}
(out/'ci-validation.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))

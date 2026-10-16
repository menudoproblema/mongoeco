import json
import os
from pathlib import Path
import subprocess
import sys

root = Path('/Users/uve/Proyectos/mongoeco')
out = Path('/tmp/mongoeco-write-ops')
gate, version = sys.argv[1:]
python = str(Path('/tmp/mongoeco-python315') / ('venv' if version == '315' else f'py{version}') / 'bin/python')
label = gate + '-' + version
tmp = out / ('tmp-' + label)
tmp.mkdir(exist_ok=True)
env = {**os.environ, 'MONGOECO_TEST_INSTALLED_ARTIFACT': '1', 'TMPDIR': str(tmp)}
commands = {
    'pytest': [[python, '-m', 'pytest', '-q', '--tb=short']],
    'unittest': [[python, '-m', 'unittest', 'discover', '-s', 'tests', '-p', 'test*.py']],
    'contracts': [
        [python, 'scripts/check_public_typing.py'],
        [python, 'scripts/smoke_external_engine.py'],
        [python, 'scripts/update_public_api_manifest.py', '--check'],
        [python, 'scripts/update_compat_snapshots.py'],
        [python, 'scripts/check_lint_ratchet.py', '--base-ref', 'v4.9.1'],
    ],
    'deep': [[python, '-m', 'pytest', '-q', 'tests/property', '--hypothesis-profile=deep']],
    'smoke-wheel': [[python, 'scripts/smoke_installed_wheel.py', '--wheel', str(out/'dist/mongoeco-4.9.1-py3-none-any.whl')]],
    'smoke-sdist': [[python, 'scripts/smoke_installed_wheel.py', '--sdist', str(out/'dist/mongoeco-4.9.1.tar.gz')]],
}
if gate == 'pytest' and version == '314':
    env['COVERAGE_FILE'] = str(out/'coverage314')
    commands[gate][0] += ['--cov=mongoeco', '--cov-report=term', '--cov-report=json:'+str(out/'coverage314.json')]
results = []
for index, command in enumerate(commands[gate]):
    name = label + (f'-{index}' if len(commands[gate])>1 else '')
    with (out / (name + '.log')).open('w') as log:
        result = subprocess.run(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
    results.append({'command': command, 'exit_code': result.returncode, 'log': name + '.log'})
    print(name, result.returncode, flush=True)
    if result.returncode:
        break
if gate == 'contracts' and all(result['exit_code'] == 0 for result in results):
    for extension in ('json', 'md'):
        name = f'compat_catalog_exchange_snapshot.{extension}'
        assert (root/'dist/compat-catalog-current'/name).read_bytes() == (root/'tests/snapshots/compat-current'/name).read_bytes()
(out/(label+'-result.json')).write_text(json.dumps(results,indent=2)+'\n')
raise SystemExit(any(result['exit_code'] for result in results))

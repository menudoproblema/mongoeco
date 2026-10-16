import hashlib
import json
import re
import shutil
import subprocess
import tarfile
import zipfile
from pathlib import Path

root = Path('/Users/uve/Proyectos/mongoeco')
tmp = Path('/tmp/mongoeco-write-ops')
out = root / 'docs/evidence/write-operations'
out.mkdir(parents=True, exist_ok=True)

def sha(file):
    return hashlib.sha256(file.read_bytes()).hexdigest()

def save(name, value):
    (out / name).write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')

def git(*args):
    return subprocess.check_output(['git', *args], cwd=root, text=True).strip()

wheel = tmp / 'dist/mongoeco-4.9.1-py3-none-any.whl'
sdist = tmp / 'dist/mongoeco-4.9.1.tar.gz'
assert sha(wheel) == '9030e0e05a0c0e34a8087ca4a6f120afc90a94abecb94f22c29b7e85172e4947'
runtime = {
    str(p.relative_to(root)): sha(p)
    for p in sorted((root / 'src/mongoeco').rglob('*'))
    if p.is_file() and (p.suffix in ('.py', '.json') or p.name == 'py.typed')
}
assert len(runtime) == 275
with zipfile.ZipFile(wheel) as archive:
    for path, digest in runtime.items():
        assert hashlib.sha256(archive.read(str(Path(path).relative_to('src')))).hexdigest() == digest, path
sdist_compared = 0
with tarfile.open(sdist) as archive:
    for member in archive.getmembers():
        relative = Path(*Path(member.name).parts[1:])
        current = root / relative
        if member.isfile() and current.is_file() and 'mongoeco.egg-info' not in relative.parts:
            assert archive.extractfile(member).read() == current.read_bytes(), str(relative)
            sdist_compared += 1
save('runtime-sources.json', runtime)
paths = set(git('diff', '--name-only', 'HEAD').splitlines()) | set(git('ls-files', '--others', '--exclude-standard').splitlines())
workset = {p: sha(root / p) for p in sorted(paths) if not p.startswith('docs/evidence/') and (root / p).is_file()}
save('workset-sources.json', workset)
files = {
    'baseline.json', 'baseline.log', 'baseline.py', 'ci-validation.json', 'ci-validation.log',
    'lint-final.log', 'format-final.log', 'twine-final.log', 'repro-first.log', 'repro-second.log',
    'build-result.log', 'focal-final.log', 'repaired-global-cases.log', 'consumer-canary.json',
    'consumer.log', 'validate_ci.py', 'run_gate.py', 'post_matrix.py', 'build_final.py', 'finalize.py',
}
environments = []
gates = []
for version in ('313', '314', '315'):
    python = Path('/tmp/mongoeco-python315') / ('venv' if version == '315' else 'py' + version) / 'bin/python'
    query = 'import json,sys,sysconfig,sqlite3,importlib.metadata as m,mongoeco; print(json.dumps({"python":sys.version,"version":list(sys.version_info[:3]),"module":mongoeco.__file__,"gil_disabled":sysconfig.get_config_var("Py_GIL_DISABLED"),"sqlite":sqlite3.sqlite_version,"usearch":m.version("usearch"),"shapely":m.version("shapely"),"pymongo":m.version("pymongo")}))'
    environment = json.loads(subprocess.check_output([str(python), '-c', query], text=True))
    assert 'site-packages' in Path(environment['module']).parts
    assert environment['usearch'] == '2.26.4' and environment['shapely'] == '2.2.0' and environment['pymongo'] == '4.19.0'
    for path, digest in runtime.items():
        installed = Path(environment['module']).parent / Path(path).relative_to('src/mongoeco')
        assert sha(installed) == digest, (version, path)
    match = re.search(r'(\d+) passed, (\d+) skipped, (\d+) subtests passed', (tmp / f'pytest-{version}.log').read_text())
    assert match and tuple(map(int, match.groups())) == (11562, 31, 14445)
    unit = (tmp / f'unittest-{version}.log').read_text()
    assert 'Ran 3613 tests' in unit and 'OK (skipped=3)' in unit
    for gate in ('pytest', 'unittest', 'contracts', 'smoke-wheel', 'smoke-sdist', 'conformance'):
        name = f'{gate}-{version}-result.json'
        results = json.loads((tmp / name).read_text())
        assert results and all(r['exit_code'] == 0 for r in results), name
        gates.append({'python': version, 'gate': gate, 'runs': results})
        files.add(name)
        files.update(r['log'] for r in results if 'log' in r)
    for engine in ('MemoryEngine', 'SQLiteEngine'):
        name = f'conformance-{engine}-{version}.json'
        report = json.loads((tmp / name).read_text())
        assert report['passed'] and report['summary']['failed'] == report['summary']['error'] == 0
        files.update({name, name.replace('.json', '.log')})
    files.update({f'packages-{version}.json', f'install-final-{version}.log'})
    environments.append({**environment, 'pytest': {'passed': 11562, 'expected_skips': 31, 'subtests_passed': 14445}, 'unittest': {'tests': 3613, 'expected_skips': 3}, 'installed_runtime_files_compared': len(runtime)})

coverage = json.loads((tmp / 'coverage314.json').read_text())['totals']
assert coverage['percent_covered'] >= 99
save('coverage-totals-314.json', coverage)
assert '9 passed' in (tmp / 'deep-314.log').read_text()
assert all(r['exit_code'] == 0 for r in json.loads((tmp / 'deep-314-result.json').read_text()))
files.update({'deep-314.log', 'deep-314-result.json'})
for engine in ('memory-sync', 'sqlite-sync'):
    name = f'benchmark-{engine}-314.json'
    json.loads((tmp / name).read_text())
    files.add(name)
assert '83 changed Python files' in (tmp / 'lint-final.log').read_text()
assert '7 files already formatted' in (tmp / 'format-final.log').read_text()
assert (tmp / 'twine-final.log').read_text().count('PASSED') == 2
ci = json.loads((tmp / 'ci-validation.json').read_text())
assert ci['wheel_sha256'] == sha(wheel) and ci['sdist_sha256'] == sha(sdist)
for name in (wheel.name, sdist.name):
    assert (tmp / 'dist-first' / name).read_bytes() == (tmp / 'dist-second' / name).read_bytes() == (tmp / 'dist' / name).read_bytes()

baseline = json.loads((tmp / 'baseline.json').read_text())
assert len(baseline['runs']) == 168 and not any(r['matches'] for r in baseline['runs'])
old = json.loads((root / 'docs/evidence/python-support/validation.json').read_text())['candidate']
assert sha(Path(old['wheel_path'])) == old['wheel_sha256']
consumer = json.loads((tmp / 'consumer-canary.json').read_text())
assert consumer['overlay']['wheel_sha256'] == sha(wheel)
assert consumer['overlay']['consumer_source_modified'] is False and len(consumer['runs']) == 16
native_replays = [r for run in consumer['runs'] for r in run['native']]
assert len(native_replays) == 48 and all(r['parity'] for r in native_replays)
assert all(run['semantic_correctors_invoked'] == {'$numberInt': '0'} for run in consumer['runs'])

native = []
fixture_hashes = {}
for target, port in [('7.0', 37567), ('8.0', 37568), ('9.0', 37569)]:
    suffix = target.replace('.', '_')
    operation = None
    for prefix, count in [('write_parity', 170), ('write_identity', 82), ('write_operations', 42), ('write_raw', 8), ('api_options', 104)]:
        relative = f'tests/fixtures/{prefix}_mongodb_{suffix}.json'
        capture = json.loads((root / relative).read_text())
        assert capture['native_facades'] == ['sync', 'async'] and len(capture['observations']) == count
        assert capture['sdk'] == '4.19.0' and capture['fcv'] == {'version': target}
        for source, digest in capture.get('source_sha256', {}).items():
            assert sha(root / source) == digest
        if 'observer_sha256' in capture:
            assert sha(root / 'tests/support_write_parity.py') == capture['observer_sha256']
            assert sha(root / 'tests/differential/write_cases.py') == capture['catalog_sha256']
        if prefix == 'write_operations':
            assert sha(root / 'tests/differential/write_operation_cases.py') == capture['operation_catalog_sha256']
            operation = capture
        fixture_hashes[relative] = sha(root / relative)
    assert '42 operation payloads; native sync/async equal; cleanup verified' in (tmp / f'recapture-{target}.log').read_text()
    assert '104 cases per facade; native sync/async equal; cleanup verified' in (tmp / f'api-capture-{target}.log').read_text()
    files.update({f'recapture-{target}.log', f'api-capture-{target}.log'})
    shutil.copyfile(tmp / f'final-histories/native_{suffix}.json', out / f'histories-native_{suffix}.json')
    native.append({'target': target, 'version': operation['build_info']['version'], 'fcv': operation['fcv'], 'sdk': operation['sdk'], 'native_facades': operation['native_facades'], 'uri': f'mongodb://127.0.0.1:{port}', 'write_capture_command': ['.venv/bin/python', 'scripts/capture_write_parity.py', '--target', target, '--output-dir', str(tmp / f'final-{target}'), '--histories-dir', str(tmp / 'final-histories')], 'api_capture_command': ['.venv/bin/python', 'scripts/capture_api_option_parity.py', '--target', target, '--output', str(tmp / f'api-options-{target}.json')], 'environment_key': 'MONGOECO_REAL_MONGODB_URI', 'cleanup_verified': True})
save('native-fixture-sha256.json', fixture_hashes)
for name in files:
    shutil.copyfile(tmp / name, out / name)
(out / 'SHA256SUMS').write_text(sha(wheel) + '  ' + wheel.name + '\n' + sha(sdist) + '  ' + sdist.name + '\n')
report = {
    'schema': 'mongoeco-write-operation-validation/v1',
    'status': 'implementation_complete_release_decision_pending',
    'date': '2026-10-15',
    'candidate': {'git_head': git('rev-parse', 'HEAD'), 'uncommitted_workset': True, 'workset_manifest': 'workset-sources.json', 'workset_sha256': sha(out / 'workset-sources.json'), 'workset_manifest_excludes': ['docs/evidence/**'], 'runtime_manifest': 'runtime-sources.json', 'runtime_sha256': sha(out / 'runtime-sources.json'), 'wheel_sha256': sha(wheel), 'sdist_sha256': sha(sdist), 'wheel_path': str(wheel), 'sdist_path': str(sdist), 'package_version': '4.9.1', 'disposition': 'Unpublished workset build; distinct from published 4.9.1. No release number selected.', 'runtime_files_compared': len(runtime), 'sdist_checkout_files_compared': sdist_compared},
    'contract': 'docs/write-error-operations.md; direct user authorization',
    'baseline': {'wheel_sha256': old['wheel_sha256'], 'native_target': '8.0', 'observations': 168, 'divergences': 168, 'evidence': 'baseline.json', 'runtime_path_reused_after_baseline_for_final_candidate': True},
    'corpus': {'new_cases': 42, 'native_observations': 252, 'direct_observations': 504, 'wire_observations': 504, 'native_facades': ['sync', 'async'], 'local_facades': ['sync', 'async'], 'engines': ['MemoryEngine', 'SQLiteEngine'], 'pymongo_profile_new_corpus': '4.19', 'prior_write_and_identity_profiles': ['4.9', '4.19'], 'item_codes': [2, 14, 53, 66, 121, 11000], 'bulk_envelope_code': 65, 'command_code': 40415, 'command_code_name': 'IDLUnknownField', 'observed': ['error family and code', 'writeErrors index/code and op presence/type/BSON', 'bulk counters and upserted IDs', 'caller generated IDs and aliases', 'persisted state and partial effects', 'raw command result and absence of driver op enrichment'], 'recaptured_per_server_per_native_facade': {'write': 170, 'identity': 82, 'operations': 42, 'raw': 8, 'api_options': 104}, 'native_histories': 180, 'fixture_hashes': 'native-fixture-sha256.json', 'negative_observer_regression_added': True, 'conservation_tests': 21, 'focal_passed': 542, 'all_consumed_again_by_final_full_pytest': True},
    'native_servers': native,
    'related_repairs': ['Missing hint errors retain code 2, including SQLite collated fallback scans.', 'SQLite single insert validates before duplicate _id, retaining validation bypass.', 'MongoDB 7 rejects classic update sort as a whole-command parse error 40415, preserving prior batches.', 'Raw update and replacement propagate sort on MongoDB 8 and 9.', 'Nonnumeric $inc operand retains code 14.'],
    'environments': environments,
    'common_dependency_pins': {'usearch': '2.26.4', 'shapely': '2.2.0', 'python_specific_exceptions': []},
    'policy': {'python_313_support_through': '2027-10', 'minimum': '>=3.13'},
    'gates': gates,
    'coverage_python_314': coverage,
    'additional_gates': {'deep_property_python_314_passed': 9, 'benchmarks_python_314': ['memory-sync Search/vector smoke', 'sqlite-sync Search/vector smoke'], 'twine': 'wheel and sdist passed', 'distribution_reproduction': 'two sequential builds, normalized sdist, byte-identical', 'lint_base': 'v4.9.1', 'lint_changed_python_files': 83, 'format_selected_files': 7, 'ci_validation': ci},
    'consumer': {'flows': 16, 'native_replays': 48, 'parity': True, 'wheel_sha256': sha(wheel), 'semantic_correctors_invoked': 0, 'permanent_source_changes': False, 'report': 'consumer-canary.json'},
    'review': {'type': 'self_review', 'diff_reviewed': True, 'documentation_aligned': True, 'required_candidate_gates_passed': True, 'prior_authorized_work_preserved': True, 'unrelated_new_changes_observed': False, 'initial_global_findings_resolved': ['Existing valid-sort tests now explicitly use MongoDB 8 instead of default 7.', 'SQLite preflight only validates hint existence; specialized text-index planning retains its own checks.'], 'remaining_findings_in_scope': []},
    'cleanup': {'native_temporary_databases_absent_before_shutdown': True, 'only_admin_config_local_remained': True, 'three_test_servers_stopped': True, 'temporary_server_dbpaths_removed': True},
    'limits': ['Finite corpus; no universal parity claim beyond the observed scenarios.', 'Local macOS arm64 runs; hosted GitHub jobs configured but not executed here.', 'The old SDK profile matrix is historical evidence; not rerun in this private payload repair slice.', 'No release number, tag, publication or permanent consumer modification.', 'Prior evidence directories remain historical; Python 3.13 support policy is now October 2027.'],
}
save('validation.json', report)
save('evidence-sha256.json', {p.name: sha(p) for p in sorted(out.iterdir()) if p.is_file() and p.name != 'evidence-sha256.json'})
print(json.dumps({'status': report['status'], 'workset_sha256': report['candidate']['workset_sha256'], 'wheel_sha256': sha(wheel), 'coverage': coverage['percent_covered'], 'evidence_files': len(list(out.iterdir()))}, indent=2))

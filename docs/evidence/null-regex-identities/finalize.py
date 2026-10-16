import hashlib
import json
import re
import shutil
import subprocess
import tarfile
import zipfile
from pathlib import Path

root = Path('/Users/uve/Proyectos/mongoeco')
tmp = Path('/tmp/mongoeco-id-edges')
out = root / 'docs/evidence/null-regex-identities'
out.mkdir(parents=True, exist_ok=True)

def sha(file):
    return hashlib.sha256(file.read_bytes()).hexdigest()

def save(name, value):
    (out / name).write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')

def git(*args):
    return subprocess.check_output(['git', *args], cwd=root, text=True).strip()

wheel = tmp / 'dist/mongoeco-4.9.1-py3-none-any.whl'
sdist = tmp / 'dist/mongoeco-4.9.1.tar.gz'
assert sha(wheel) == 'fe4ea943e6b1d7f72fbe8f5fc68630840b68b3d8ed9ce075aae8822e0034b932'
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
    'build-result.log', 'focal-final.log', 'repaired-test-double.log', 'consumer-canary.json',
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
    assert match and tuple(map(int, match.groups())) == (13670, 31, 14445)
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
    environments.append({**environment, 'pytest': {'passed': 13670, 'expected_skips': 31, 'subtests_passed': 14445}, 'unittest': {'tests': 3613, 'expected_skips': 3}, 'installed_runtime_files_compared': len(runtime)})

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
assert '88 changed Python files' in (tmp / 'lint-final.log').read_text()
assert '10 files already formatted' in (tmp / 'format-final.log').read_text()
assert (tmp / 'twine-final.log').read_text().count('PASSED') == 2
ci = json.loads((tmp / 'ci-validation.json').read_text())
assert ci['wheel_sha256'] == sha(wheel) and ci['sdist_sha256'] == sha(sdist)
for name in (wheel.name, sdist.name):
    assert (tmp / 'dist-first' / name).read_bytes() == (tmp / 'dist-second' / name).read_bytes() == (tmp / 'dist' / name).read_bytes()

baseline = json.loads((tmp / 'baseline.json').read_text())
assert len(baseline['runs']) == 244 and sum(not r['matches'] for r in baseline['runs']) == 208
old = json.loads((root / 'docs/evidence/write-operations/validation.json').read_text())['candidate']
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
    for prefix, count in [('write_parity', 170), ('write_identity', 143), ('write_operations', 42), ('write_raw', 8), ('api_options', 104)]:
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
        if prefix == 'write_identity':
            assert sha(root / 'tests/differential/write_identity_cases.py') == capture['identity_catalog_sha256']
            assert len(capture['raw_observations']) == 26
        fixture_hashes[relative] = sha(root / relative)
    assert '26 identity commands; 42 operation payloads; native sync/async equal; cleanup verified' in (tmp / f'capture-{target}.log').read_text()
    assert '104 cases per facade; native sync/async equal; cleanup verified' in (tmp / f'api-capture-{target}.log').read_text()
    files.update({f'capture-{target}.log', f'api-capture-{target}.log'})
    shutil.copyfile(tmp / f'histories/native_{suffix}.json', out / f'histories-native_{suffix}.json')
    native.append({'target': target, 'version': operation['build_info']['version'], 'fcv': operation['fcv'], 'sdk': operation['sdk'], 'native_facades': operation['native_facades'], 'uri': f'mongodb://127.0.0.1:{port}', 'write_capture_command': ['.venv/bin/python', 'scripts/capture_write_parity.py', '--target', target, '--output-dir', str(tmp / f'native-{target}'), '--histories-dir', str(tmp / 'histories')], 'api_capture_command': ['.venv/bin/python', 'scripts/capture_api_option_parity.py', '--target', target, '--output', str(tmp / f'api-options-{target}.json')], 'environment_key': 'MONGOECO_REAL_MONGODB_URI', 'cleanup_verified': True})
save('native-fixture-sha256.json', fixture_hashes)
files.update({'profiles-raw.json', 'profiles-summary.json', 'profiles.log', 'cleanup-before-shutdown.json', 'runtime-diff.patch', 'typing-source-final.log'})
for name in files:
    shutil.copyfile(tmp / name, out / name)
(out / 'SHA256SUMS').write_text(sha(wheel) + '  ' + wheel.name + '\n' + sha(sdist) + '  ' + sdist.name + '\n')
profile_raw = json.loads((tmp / 'profiles-raw.json').read_text())
assert set(profile_raw) == {'4.9.2', '4.11.3', '4.13.2', '4.17.0', '4.18.2', '4.19.0'}
assert json.loads((tmp / 'profiles-summary.json').read_text()) == json.loads((root / 'tests/fixtures/pymongo_profile_matrix.json').read_text())
assert not list(tmp.glob('tmp*/mongod.log'))
ports = subprocess.run(['lsof', '-nP', '-iTCP:37567', '-iTCP:37568', '-iTCP:37569', '-sTCP:LISTEN'], capture_output=True, text=True)
assert ports.returncode == 1 and not ports.stdout
report = {
    'schema': 'mongoeco-null-regex-identity-validation/v1',
    'status': 'implementation_complete_release_decision_pending',
    'date': '2026-10-15',
    'candidate': {
        'git_head': git('rev-parse', 'HEAD'), 'uncommitted_workset': True,
        'workset_manifest': 'workset-sources.json', 'workset_sha256': sha(out / 'workset-sources.json'),
        'workset_manifest_excludes': ['docs/evidence/**'],
        'runtime_manifest': 'runtime-sources.json', 'runtime_sha256': sha(out / 'runtime-sources.json'),
        'wheel_sha256': sha(wheel), 'sdist_sha256': sha(sdist), 'wheel_path': str(wheel), 'sdist_path': str(sdist),
        'package_version': '4.9.1', 'disposition': 'Unpublished workset build; distinct from published 4.9.1. No release number selected.',
        'runtime_files_compared': len(runtime), 'sdist_checkout_files_compared': sdist_compared,
    },
    'contract': 'docs/null-and-regex-identities.md; direct user authorization to correct both observed defects',
    'baseline': {'wheel_sha256': old['wheel_sha256'], 'native_target': '8.0', 'observations': 244, 'divergences': 208, 'evidence': 'baseline.json', 'runtime_path_reused_after_baseline_for_final_candidate': True},
    'corpus': {
        'previous_identity_cases': 82, 'new_identity_cases': 61, 'total_identity_cases': 143, 'raw_identity_views': 26,
        'new_native_helper_observations': 366, 'new_native_raw_observations': 156,
        'total_native_identity_observations': 1014, 'total_direct_identity_observations': 4056, 'total_wire_identity_observations': 2028,
        'local_facades': ['sync', 'async'], 'native_facades': ['sync', 'async'], 'engines': ['MemoryEngine', 'SQLiteEngine'],
        'direct_pymongo_profiles': ['4.9', '4.19'], 'wire_profile': '4.19', 'native_sdk': '4.19.0',
        'native_item_codes': [53, 54, 66, 121, 11000], 'bulk_envelope_code': 65,
        'observed': ['error family/code/codeName where native supplies it', 'writeErrors original indices, codes and op BSON', 'did_upsert presence and value', 'individual and bulk counters', 'upserted arrays and null IDs', 'raw upserted:null presence', 'BEFORE/AFTER values', 'persisted state and ordered/unordered partial effects', 'caller ownership and recovery writes'],
        'controls': ['_id zero, empty string and false', 'matched and no-match null ID', 'document IDs containing nested regex'],
        'recaptured_per_server_per_native_facade': {'write': 170, 'identity': 143, 'operations': 42, 'raw': 8, 'raw_identity': 26, 'api_options': 104},
        'native_histories': 180, 'fixture_hashes': 'native-fixture-sha256.json',
        'focal_passed': 4093, 'conservation_tests': 19, 'observer_tests': 6, 'all_consumed_again_by_final_full_pytest': True,
    },
    'implementation': {
        'upsert_identity': 'Optional keyword-only bool did_upsert; legacy non-null-ID constructors infer true. Engines preserve actual match counts, individual facade maps null-upsert count to native PyMongo representation, bulk/raw use the flag.',
        'events': 'Flag controls applied-outcome validation, commit sequence, insert publication and SQLite outbox; a null-ID upsert publishes exactly one insert and a following no-op publishes none.',
        'regex_identity': 'Shared validation rejects root internal/BSON/Python regex with code 53; replacement array upserts retain code 54; nested regex document IDs remain valid.',
        'public_contract': 'UpdateResult optional keyword-only parameter, field and member added to curated manifest; static type export keeps did_upsert bool; SPI version and old positional constructors unchanged.',
    },
    'native_servers': native, 'environments': environments, 'gates': gates, 'coverage_python_314': coverage,
    'common_dependency_pins': {'usearch': '2.26.4', 'shapely': '2.2.0', 'python_specific_exceptions': []},
    'policy': {'python_313_support_through': '2027-10', 'minimum': '>=3.13'},
    'additional_gates': {
        'deep_property_python_314_passed': 9, 'benchmarks_python_314': ['memory-sync Search/vector smoke', 'sqlite-sync Search/vector smoke'],
        'twine': 'wheel and sdist passed', 'distribution_reproduction': 'two sequential builds, normalized sdist, byte-identical',
        'lint_base': 'v4.9.1', 'lint_changed_python_files': 88, 'format_selected_files': 10, 'ci_validation': ci,
        'isolated_sdk_versions': list(profile_raw), 'isolated_sdk_probe_count': sum(len(v) for v in profile_raw.values()),
        'isolated_sdk_summary_matches_existing_fixture': True,
    },
    'consumer': {'flows': 16, 'native_replays': 48, 'parity': True, 'wheel_sha256': sha(wheel), 'semantic_correctors_invoked': 0, 'permanent_source_changes': False, 'report': 'consumer-canary.json'},
    'review': {
        'type': 'self_review', 'runtime_delta_from_previous_wheel': 'runtime-diff.patch',
        'diff_reviewed': True, 'documentation_aligned': True, 'required_candidate_gates_passed': True,
        'prior_authorized_work_preserved': True, 'unrelated_new_changes_observed': False,
        'initial_findings_resolved': ['Native regex replacement upsert uses 53, while array replacement upsert uses 54.', 'Raw command test double now returns real UpdateResult instead of SimpleNamespace without did_upsert.'],
        'remaining_findings_in_scope': [],
    },
    'cleanup': {'temporary_databases_absent_before_shutdown': True, 'receipt': 'cleanup-before-shutdown.json', 'test_servers_stopped': True, 'temporary_server_dbpaths_removed': True},
    'limits': [
        'Finite corpus, not universal compatibility for all possible IDs or operations.',
        'Local macOS arm64 runs; hosted GitHub jobs configured but not executed here.',
        'Native data parity uses SDK 4.19.0; six older/current SDKs are separately checked for public API profile admission.',
        'No release number selection, tag, publication or permanent consumer changes.',
        'Prior evidence directories retain historical candidates; current Python 3.13 support ends October 2027.',
    ],
}
save('validation.json', report)
save('evidence-sha256.json', {p.name: sha(p) for p in sorted(out.iterdir()) if p.is_file() and p.name != 'evidence-sha256.json'})
print(json.dumps({'status': report['status'], 'workset_sha256': report['candidate']['workset_sha256'], 'wheel_sha256': sha(wheel), 'coverage': coverage['percent_covered'], 'evidence_files': len(list(out.iterdir()))}, indent=2))

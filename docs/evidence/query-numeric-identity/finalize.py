import hashlib,json,re,shutil,subprocess,tarfile,zipfile
from pathlib import Path
root=Path('/Users/uve/Proyectos/mongoeco');tmp=Path('/tmp/mongoeco-query-id');out=root/'docs/evidence/query-numeric-identity'
out.mkdir(parents=True,exist_ok=True)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(n,v):(out/n).write_text(json.dumps(v,indent=2,ensure_ascii=False)+'\n')
def git(*args):return subprocess.check_output(['git',*args],cwd=root,text=True).strip()
wheel=tmp/'dist/mongoeco-4.9.1-py3-none-any.whl';sdist=tmp/'dist/mongoeco-4.9.1.tar.gz'
runtime={str(p.relative_to(root)):sha(p)for p in sorted((root/'src/mongoeco').rglob('*'))if p.is_file()and(p.suffix in('.py','.json')or p.name=='py.typed')}
with zipfile.ZipFile(wheel)as z:
 for p,digest in runtime.items():assert hashlib.sha256(z.read(str(Path(p).relative_to('src')))).hexdigest()==digest,p
compared=0
with tarfile.open(sdist)as t:
 for m in t.getmembers():
  p=Path(*Path(m.name).parts[1:]);current=root/p
  if m.isfile()and current.is_file()and'mongoeco.egg-info'not in p.parts:
   assert t.extractfile(m).read()==current.read_bytes(),str(p);compared+=1
save('runtime-sources.json',runtime)
paths=set(git('diff','--name-only','HEAD').splitlines())|set(git('ls-files','--others','--exclude-standard').splitlines())
save('workset-sources.json',{p:sha(root/p)for p in sorted(paths)if not p.startswith('docs/evidence/')and(root/p).is_file()})
gates=[];envs=[];counts=[];files=set()
for v in ('313','314','315'):
 py=Path('/tmp/mongoeco-python315')/('venv'if v=='315'else'py'+v)/'bin/python'
 info=json.loads(subprocess.check_output([str(py),'-c','import json,sys,sqlite3,sysconfig,importlib.metadata as m,mongoeco;print(json.dumps({"python":sys.version,"module":mongoeco.__file__,"sqlite":sqlite3.sqlite_version,"gil_disabled":sysconfig.get_config_var("Py_GIL_DISABLED"),"usearch":m.version("usearch"),"shapely":m.version("shapely"),"pymongo":m.version("pymongo")}))'],text=True))
 assert 'site-packages'in Path(info['module']).parts
 assert(info['usearch'],info['shapely'],info['pymongo'])==('2.26.4','2.2.0','4.19.0')
 for p,digest in runtime.items():assert sha(Path(info['module']).parent/Path(p).relative_to('src/mongoeco'))==digest,(v,p)
 match=re.search(r'(\d+) passed, (\d+) skipped, (?:\d+ warnings, )?(\d+) subtests passed',(tmp/f'pytest-{v}.log').read_text());assert match,v
 count=tuple(map(int,match.groups()));assert count[0]>18000 and count[1]==31 and count[2]>=14445;counts.append(count)
 unit=(tmp/f'unittest-{v}.log').read_text();assert'Ran 3613 tests'in unit and'OK (skipped=3)'in unit
 info['pytest']={'passed':count[0],'expected_skips':count[1],'subtests_passed':count[2]};info['unittest']={'tests':3613,'expected_skips':3};envs.append(info)
 for gate in ('pytest','unittest','contracts','smoke-wheel','smoke-sdist','conformance'):
  name=f'{gate}-{v}-result.json';runs=json.loads((tmp/name).read_text());assert runs and all(r['exit_code']==0 for r in runs),name
  gates.append({'python':v,'gate':gate,'runs':runs});files.add(name);files.update(r['log']for r in runs if'log'in r)
 files.update({f'packages-{v}.json',f'install-{v}.log'})
 for engine in ('MemoryEngine','SQLiteEngine'):
  name=f'conformance-{engine}-{v}.json';j=json.loads((tmp/name).read_text());assert j['passed']and j['summary']['failed']==j['summary']['error']==0;files.update({name,name.replace('.json','.log')})
assert len(set(counts))==1
coverage=json.loads((tmp/'coverage314.json').read_text())['totals'];assert coverage['percent_covered']>=99;save('coverage-totals-314.json',coverage)
assert '9 passed'in(tmp/'deep-314.log').read_text();assert all(r['exit_code']==0 for r in json.loads((tmp/'deep-314-result.json').read_text()));files.update({'deep-314.log','deep-314-result.json'})
for name in ('benchmark-memory-sync-314.json','benchmark-sqlite-sync-314.json'):json.loads((tmp/name).read_text());files.add(name)
assert 'lint ratchet passed'in(tmp/'lint-final.log').read_text();assert '10 files already formatted'in(tmp/'format-final.log').read_text();assert(tmp/'twine-final.log').read_text().count('PASSED')==2
ci=json.loads((tmp/'ci-validation.json').read_text());assert ci['wheel_sha256']==sha(wheel)and ci['sdist_sha256']==sha(sdist)
for name in(wheel.name,sdist.name):assert(tmp/'dist-first'/name).read_bytes()==(tmp/'dist-second'/name).read_bytes()==(tmp/'dist'/name).read_bytes()
baseline=json.loads((tmp/'baseline.json').read_text());assert len(baseline['observations'])==524 and sum(not r['matches']for r in baseline['observations'])==288
consumer=json.loads((tmp/'consumer-canary.json').read_text());assert consumer['overlay']['wheel_sha256']==sha(wheel)and not consumer['overlay']['consumer_source_modified']and len(consumer['runs'])==16
replays=[r for run in consumer['runs']for r in run['native']];assert len(replays)==48 and all(r['parity']for r in replays)
assert all(run['semantic_correctors_invoked']=={'$numberInt':'0'}for run in consumer['runs'])
profiles=json.loads((tmp/'sdk-results.json').read_text());assert set(profiles)=={'4.9.2','4.11.3','4.13.2','4.17.0','4.18.2','4.19.0'}
assert json.loads((tmp/'sdk-summary.json').read_text())==json.loads((root/'tests/fixtures/pymongo_profile_matrix.json').read_text())
fixture_hashes={};native=[]
for target in('7.0','8.0','9.0'):
 suffix=target.replace('.','_')
 for prefix,count in(('write_parity',170),('write_identity',246),('write_operations',42),('write_raw',8),('write_queries',28),('api_options',104)):
  relative=f'tests/fixtures/{prefix}_mongodb_{suffix}.json';j=json.loads((root/relative).read_text());assert len(j['observations'])==count and j['native_facades']==['sync','async']and j['sdk']=='4.19.0'and j['fcv']=={'version':target}
  for source,digest in j.get('source_sha256',{}).items():assert sha(root/source)==digest
  for key,source in(('observer_sha256','tests/support_write_parity.py'),('catalog_sha256','tests/differential/write_cases.py'),('identity_catalog_sha256','tests/differential/write_identity_cases.py'),('operation_catalog_sha256','tests/differential/write_operation_cases.py'),('query_catalog_sha256','tests/differential/write_query_cases.py')):
   if key in j:assert j[key]==sha(root/source),(relative,key)
  if prefix=='write_identity':assert len(j['raw_observations'])==110
  fixture_hashes[relative]=sha(root/relative)
 native.append({'target':target,'version':j['build_info']['version'],'fcv':j['fcv'],'sdk':j['sdk'],'sync_async_equal':True,'environment':{'MONGOECO_REAL_MONGODB_URI':f'mongodb://127.0.0.1:{37567+int(target[0])-7}'},'write_capture_command':['/tmp/mongoeco-python315/venv/bin/python','scripts/capture_write_parity.py','--target',target,'--output-dir','tests/fixtures','--histories-dir',str(tmp/'histories')],'api_capture_command':['/tmp/mongoeco-python315/venv/bin/python','scripts/capture_api_option_parity.py','--target',target,'--output',f'tests/fixtures/api_options_mongodb_{suffix}.json']})
 assert '110 identity commands; 28 query commands;'in(tmp/f'capture-write-{target}.log').read_text()
 assert '104 cases per facade; native sync/async equal; cleanup verified'in(tmp/f'capture-api_option-{target}.log').read_text()
 files.update({f'capture-write-{target}.log',f'capture-api_option-{target}.log'})
 shutil.copyfile(tmp/f'histories/native_{suffix}.json',out/f'histories-native_{suffix}.json')
save('native-fixture-sha256.json',fixture_hashes)
cleanup=json.loads((tmp/'native-cleanup.json').read_text());assert all(r['temporary_dbpath_removed']and not Path(r['dbpath']).exists()for r in cleanup)
legacy=root/'tests/fixtures/sqlite/numeric-identities-before-fix.sqlite';legacy_meta=json.loads(legacy.with_suffix('.json').read_text());assert sha(legacy)==legacy_meta['sha256']
cleanup_runs=json.loads((tmp/'test-cleanup-results.json').read_text());assert len(cleanup_runs)==3 and all(r['exit_code']==0 for r in cleanup_runs)
cleanup_receipt=json.loads((tmp/'test-cleanup.json').read_text());assert cleanup_receipt['final_sha256']==sha(root/cleanup_receipt['file'])
files.update({'test-cleanup.json','test-cleanup.diff','test-cleanup-results.json','test_cleanup.py','test-cleanup.log',*(r['log']for r in cleanup_runs)})
files.update({'baseline.json','baseline.log','baseline.py','legacy.py','ci-validation.json','ci-validation.log','lint-final.log','format-final.log','twine-final.log','repro-first.log','repro-second.log','build.log','install.py','updated-legacy-expectations.log','no-scan-regression.log','no-scan-installed-final.log','consumer-canary.json','consumer.log','sdk-results.json','sdk-summary.json','sdk.log','native-cleanup.json','typechange-followup.json','typechange-followup.log','typechange_candidate_probe.py','run_gate.py','full_matrix.py','post_matrix.py','build_final.py','validate_ci.py','capture_options.py','finalize.py','runtime.diff'})
for name in files:shutil.copyfile(tmp/name,out/name)
for v in ('313','314','315'):shutil.copyfile(tmp/'rejected-id-scan'/f'pytest-{v}.log',out/f'rejected-pytest-{v}.log')
report={'schema':'mongoeco-write-query-numeric-identity-validation/v1','status':'implementation_complete_release_decision_pending','date':'2026-10-15','candidate':{'git_head':git('rev-parse','HEAD'),'uncommitted_workset':True,'workset_manifest':'workset-sources.json','workset_sha256':sha(out/'workset-sources.json'),'workset_manifest_excludes':['docs/evidence/**'],'runtime_manifest':'runtime-sources.json','runtime_sha256':sha(out/'runtime-sources.json'),'runtime_files_compared':len(runtime),'sdist_checkout_files_compared':compared,'wheel_sha256':sha(wheel),'sdist_sha256':sha(sdist),'wheel_path':str(wheel),'sdist_path':str(sdist),'package_version':'4.9.1','disposition':'Unpublished workset build; distinct from published 4.9.1. No release number selected.'},'contract':'docs/write-query-and-numeric-identity.md; direct user authorization for both fixes with existing SQLite file compatibility','baseline':{'wheel_sha256':baseline['artifact_sha256'],'native_target':'8.0','observations':524,'divergences':288,'evidence':'baseline.json'},'corpus':{'previous_identity_cases':143,'new_identity_cases':103,'identity_cases':246,'raw_identity_views':110,'raw_query_cases':28,'direct_observations':9216,'wire_observations':4608,'facades':['sync','async'],'engines':['MemoryEngine','SQLiteEngine'],'direct_profiles':['4.9','4.19'],'native_sdk':'4.19.0','query_error_codes':[14,40414],'duplicate_code':11000,'bulk_envelope_code':65,'observed':['family/code/codeName','individual/raw/bulk counters','writeErrors indices and op BSON','persisted BSON, partial prefix and unordered suffix','equivalent-ID selection, upserts and recovery','change publication and unchanged SQLite physical keys/schema'],'recaptured_per_server_per_facade':{'write':170,'identity':246,'operations':42,'raw':8,'raw_identity':110,'query':28,'api_options':104},'fixture_hashes':'native-fixture-sha256.json'},'native_servers':native,'environments':envs,'gates':gates,'test_cleanup_followup':{'receipt':'test-cleanup.json','runs':cleanup_runs,'full_suite_reuse':'The unchanged wheel passed the complete suite in all three environments. Only read-only snapshot connection teardown in one test helper changed afterward; entire affected module passed again in all three environments with ResourceWarning treated as error. Assertions, specimens and test IDs unchanged.'},'coverage_python_314':coverage,'additional_gates':{'deep_passed':9,'benchmarks':['memory-sync Search/vector','sqlite-sync Search/vector'],'reproducible_builds':True,'twine':'wheel/sdist passed','lint_base':'v4.9.1','format_files':10,'isolated_sdk_probes':sum(len(v)for v in profiles.values()),'sdk_probe_reuse':'SDK admission/profiles and probe sources unchanged by the final private Memory/SQLite scan guards; reused under causal independence, no API/catalog changes.','sdk_summary_matches_fixture':True,'ci_validation':ci},'consumer':{'flows':16,'native_replays':48,'parity':True,'semantic_correctors_invoked':0,'permanent_source_changes':False,'report':'consumer-canary.json'},'sqlite_compatibility':{'physical_key_schema_serialization_changes':False,'new_fixture':str(legacy.relative_to(root)),'fixture_sha256':sha(legacy),'generator_wheel_sha256':legacy_meta['generator_wheel_sha256'],'checks':['two facades','equivalent numeric lookup','duplicate error 11000','matched upsert','secondary index','unchanged schema/physical keys','reopen','two connections','legacy 4.5 fixture with Search and outbox'],'repairs_existing_duplicate_rows':False},'review':{'initial_findings_resolved':['Old tests encoded distinct int/float identities; expectations now require native 11000.', 'Numeric identity validation no longer scans an unchanged ID during indexed updates; existing candidate-access budgets retained.', 'Differential workflow explicitly continues the pytest command across all newly added test files.'],'type':'self_review','diff_reviewed':True,'documentation_aligned':True,'required_candidate_gates_passed':True,'prior_authorized_work_preserved':True,'unrelated_new_changes_observed':False,'remaining_findings_in_scope':[],'separate_followup':'Explicit numeric _id BSON type changes by replacement/pipeline; see typechange-followup.json. Not part of query/duplicate correction.'},'superseded_candidate':{'wheel_sha256':'6fd9957d265f71fb4ffb341b99a11284f42bf490a10531024be75e02b4c94993','disposition':'Rejected for two indexed Memory update regressions; archived in /tmp/mongoeco-query-id/rejected-id-scan. Not used as final gate evidence.'},'cleanup':{'receipt':'native-cleanup.json','test_databases_absent':True,'servers_stopped':True,'temporary_dbpaths_removed':True},'limits':['Finite local macOS arm64 corpus; hosted CI jobs configured, not executed here.','Native data parity uses PyMongo 4.19.0; six supported SDKs separately probed.','No migration or automatic fusion of pre-existing duplicate identities.','Explicit BSON type-changing _id replacements/pipelines remain a documented follow-up.','No commit, release number selection, tag or publication. Python 3.13 support through October 2027.']}
save('validation.json',report)
(out/'SHA256SUMS').write_text(sha(wheel)+'  '+wheel.name+'\n'+sha(sdist)+'  '+sdist.name+'\n')
save('evidence-sha256.json',{p.name:sha(p)for p in sorted(out.iterdir())if p.is_file()and p.name!='evidence-sha256.json'})
print(json.dumps({'workset_sha256':report['candidate']['workset_sha256'],'wheel_sha256':sha(wheel),'coverage':coverage['percent_covered'],'pytest':counts[0],'evidence_files':len(list(out.iterdir()))},indent=2))

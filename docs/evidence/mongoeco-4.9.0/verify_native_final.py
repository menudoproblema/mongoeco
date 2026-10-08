import json,sys,hashlib
from pathlib import Path
root=Path('/Users/uve/Proyectos/mongoeco');base=Path('/private/tmp/mongoeco-490-20261008');sys.path.insert(0,str(root))
from scripts.capture_differential_replay_golden import _capture_expectation
summary={}
for major in [7,8,9]:
 d=json.loads((base/f'native{major}-wheel-final.json').read_text());assert d['successful'] and d['tests_run']==d['expected_tests']==29 and not d['skipped'] and not d['errors'] and not d['failures']
 row={'server':d['runtime']['build_info']['version'],'fcv':d['runtime']['fcv'],'pymongo':d['runtime']['pymongo'],'strict_tests':29,'unexpected_skips':0,'corpora':{}}
 for lane,stem in [('review','review_improvements'),('version-deltas','version_deltas'),('semantic-guarantees','semantic_guarantees'),('index-guarantees','index_guarantees')]:
  p=base/f'native{major}-{lane}-final.json';fresh=json.loads(p.read_text());expected=json.loads((root/f'tests/fixtures/mongodb_{stem}_{major}_0.json').read_text())
  for field in ['source','targetDialect','corpus','case_manifests','cases']:assert _capture_expectation(fresh[field])==_capture_expectation(expected[field]),(major,lane,field)
  assert fresh['runtime']['fcv']=={'version':f'{major}.0'}
  row['corpora'][lane]={'cases':len(fresh['cases']),'fresh_capture_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'corpus':fresh['corpus']}
 assert sum(x['cases'] for x in row['corpora'].values())==284;summary[str(major)]=row
(base/'native-final-verification.json').write_text(json.dumps(summary,indent=2)+'\n');print('29 strict cases and 284 recaptures per version verified')

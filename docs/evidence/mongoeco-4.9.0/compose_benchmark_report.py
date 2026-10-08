import sys,json,hashlib,subprocess
from pathlib import Path
sys.path.insert(0,'/Users/uve/Proyectos/mongoeco')
from benchmarks.contracts import build_report_document,compare_reports,validate_report
from benchmarks.report import render_markdown_report
from benchmarks.runners.metrics import RSS_PEAK_SAMPLING_INTERVAL_MS
root=Path('/Users/uve/Proyectos/mongoeco'); raw=Path(sys.argv[1]);output=Path(sys.argv[2]);baseline=Path(sys.argv[3]) if len(sys.argv)>3 else None
workloads=('simple_aggregation','materializing_aggregation','aggregation_spill_diagnostics','secondary_lookup_indexed','cursor_consumption')
results=json.loads(raw.read_text());assert 'schema' not in results
report=build_report_document(results=results,size=20000,warmup=1,repetitions=5,workload_names=workloads,project_root=root,git_revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),git_dirty=True,rss_sampling_interval_ms=RSS_PEAK_SAMPLING_INTERVAL_MS)
report['provenanceComposition']={'raw_metrics':str(raw),'raw_sha256':hashlib.sha256(raw.read_bytes()).hexdigest(),'method':'benchmarks.run calls the same run_benchmarks as benchmarks.report. Metadata composed after measurements in the same unchanged installed environment; no timing/result rewriting.'}
prior=json.loads(baseline.read_text()) if baseline else None
issues=compare_reports(report,prior) if prior else validate_report(report)
report['contractIssues']=issues
output.write_text(json.dumps(report,indent=2)+'\n')
output.with_suffix('.md').write_text(render_markdown_report(results=results,size=20000,warmup=1,repetitions=5,baseline=prior,project_root=root,workload_names=workloads,report_document=report))
print('contract issues:',issues)
if issues:raise SystemExit(1)

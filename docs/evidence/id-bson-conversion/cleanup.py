import pymongo,json
from pathlib import Path
out=Path('/tmp/mongoeco-id-conversion');rows=[]
for target,port in [('7.0',37567),('8.0',37568),('9.0',37569)]:
 with pymongo.MongoClient(f'mongodb://127.0.0.1:{port}',serverSelectionTimeoutMS=3000) as c:
  dbs=c.list_database_names();assert set(dbs)<={'admin','config','local'},dbs
  rows.append({'target':target,'version':c.server_info()['version'],'port':port,'databases':dbs,'dbpath':c.admin.command('getCmdLineOpts')['parsed']['storage']['dbPath']})
(out/'native-cleanup.json').write_text(json.dumps(rows,indent=2)+'\n')
(out/'stop').touch()
print('All native test databases removed; temporary server shutdown requested')

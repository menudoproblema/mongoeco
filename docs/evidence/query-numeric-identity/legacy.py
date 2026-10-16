import asyncio,sys,json,sqlite3,hashlib,shutil
from pathlib import Path
from bson import Int64,Decimal128
from mongoeco import AsyncMongoClient
from mongoeco.engines.sqlite import SQLiteEngine
root=Path('/Users/uve/Proyectos/mongoeco')
out=root/'tests/fixtures/sqlite/numeric-identities-before-fix.sqlite'
async def create():
 async with AsyncMongoClient(SQLiteEngine(str(out))) as c:
  for n,identifier in enumerate((1,Int64(2),3.0,Decimal128('4'),{'n':5})):
   coll=c.legacy[f'items{n}'];await coll.create_index('value');await coll.insert_one({'_id':identifier,'value':0})
asyncio.run(create())
with sqlite3.connect(out) as conn:
 schema=conn.execute("SELECT type,name,sql FROM sqlite_master ORDER BY type,name").fetchall()
 rows=conn.execute('SELECT coll_name,storage_key,document FROM documents ORDER BY coll_name').fetchall()
meta={'fixture':out.name,'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),'generator_wheel_sha256':hashlib.sha256(Path('/tmp/mongoeco-id-edges/dist/mongoeco-4.9.1-py3-none-any.whl').read_bytes()).hexdigest(),'generator_source':'workset anterior a las dos correcciones, instalado desde wheel; no es una nueva publicación 4.9.1','schema':schema,'rows':rows}
out.with_suffix('.json').write_text(json.dumps(meta,indent=2)+'\n')
print('legacy created',meta['sha256'])

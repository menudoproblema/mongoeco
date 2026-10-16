import asyncio,sys,json,sqlite3,shutil,subprocess,hashlib
from pathlib import Path
from mongoeco import AsyncMongoClient
from mongoeco.engines.sqlite import SQLiteEngine
from bson import Int64,Decimal128
out=Path('/tmp/mongoeco-id-conversion');root=Path('/Users/uve/Proyectos/mongoeco')
file=out/'roundtrip.sqlite';shutil.copyfile(root/'tests/fixtures/sqlite/numeric-identities-before-fix.sqlite',file)
converted=[1.0,2,Int64(3),4.0,{'n':Int64(5)}]
async def convert():
 async with AsyncMongoClient(SQLiteEngine(str(file)))as c:
  for n,v in enumerate(converted):
   r=await c.legacy[f'items{n}'].update_one({},[{'$set':{'_id':{'$literal':v}}}]);assert r.modified_count==1
asyncio.run(convert())
code='''import asyncio,json,sys
from bson import Int64, json_util
from mongoeco import MongoClient,AsyncMongoClient
from mongoeco.engines.sqlite import SQLiteEngine
values=[1.0,2,Int64(3),4.0,{'n':Int64(5)}]
canonical=lambda v:json_util.dumps(v,json_options=json_util.CANONICAL_JSON_OPTIONS)
with MongoClient(SQLiteEngine(sys.argv[1]))as c:
 for n,v in enumerate(values):
  doc=c.legacy[f'items{n}'].find_one({'_id':v})
  assert canonical(doc['_id'])==canonical(v)
  assert c.legacy[f'items{n}'].count_documents({'value':0},hint='value_1')==1
async def run():
 async with AsyncMongoClient(SQLiteEngine(sys.argv[1]))as c:
  for n,v in enumerate(values):
   assert canonical((await c.legacy[f'items{n}'].find_one({'_id':v}))['_id'])==canonical(v)
asyncio.run(run())
import mongoeco
print(json.dumps({'old_reader_module':mongoeco.__file__,'rows_per_facade':5,'sync_async_equal':True,'secondary_indexes_valid':True}))
'''
from zipfile import ZipFile
old_runtime = out/'old-reader/lib/python3.14/site-packages'
with ZipFile('/tmp/mongoeco-query-id/dist/mongoeco-4.9.1-py3-none-any.whl') as archive:
 compared = 0
 for name in archive.namelist():
  if name.startswith('mongoeco/') and not name.endswith('/'):
   assert (old_runtime/name).read_bytes() == archive.read(name), name
   compared += 1
assert compared == 275
python=out/'old-reader/bin/python' 
result=subprocess.run([str(python),'-c',code,str(file)],cwd=out,capture_output=True,text=True,check=True)
(out/'legacy-roundtrip.json').write_text(json.dumps({'old_reader_wheel_sha256':hashlib.sha256(Path('/tmp/mongoeco-query-id/dist/mongoeco-4.9.1-py3-none-any.whl').read_bytes()).hexdigest(),'new_writer_wheel_sha256':hashlib.sha256((out/'dist/mongoeco-4.9.1-py3-none-any.whl').read_bytes()).hexdigest(),'fixture_origin':'tests/fixtures/sqlite/numeric-identities-before-fix.sqlite','old_reader_runtime_files_compared':compared,'result':json.loads(result.stdout)},indent=2)+'\n')
print(result.stdout)

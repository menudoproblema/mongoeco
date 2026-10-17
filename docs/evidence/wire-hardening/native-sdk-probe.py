import asyncio,hashlib,json,os,sys
from pathlib import Path
import pymongo
from pymongo import monitoring

class Pool(monitoring.ConnectionPoolListener):
    def __init__(self):self.ids=[]
    def connection_checked_out(self,event):self.ids.append(event.connection_id)
    def ignore(self,event):pass
    pool_created=pool_ready=pool_cleared=pool_closed=connection_created=connection_ready=connection_closed=connection_check_out_started=connection_check_out_failed=connection_checked_in=ignore

def settings(pool):return dict(event_listeners=[pool],maxPoolSize=1,retryReads=False,retryWrites=False)
def point(data):return {'configureFailPoint':'failCommand','mode':{'times':1},'data':data}
def observation(error):return {'class':type(error).__name__,'code':error.code,'errmsg_present':'errmsg' in error.details}
uri=os.environ['MONGOECO_REAL_MONGODB_URI'];database='wire_sdk_diagnostics';results={}
pool=Pool()
with pymongo.MongoClient(uri,**settings(pool)) as client:
    client.drop_database(database);items=client[database].items
    items.insert_one({'_id':'a','value':0})
    client.admin.command(point({'failCommands':['findAndModify'],'errorCode':50}))
    try:items.find_one_and_update({'_id':'a'},{'$inc':{'value':1}})
    except pymongo.errors.ExecutionTimeout as error:timeout=observation(error)
    assert items.find_one({'_id':'a'})['value']==0
    items.find_one_and_update({'_id':'a'},{'$inc':{'value':1}})
    client.admin.command(point({'failCommands':['insert'],'writeConcernError':{'code':64,'codeName':'WriteConcernFailed','errmsg':'controlled concern failure'}}))
    try:items.insert_one({'_id':'b'})
    except pymongo.errors.WriteConcernError as error:concern=observation(error)
    assert items.find_one({'_id':'b'})=={'_id':'b'}
    assert client.admin.command('ping')['ok']==1
    results['sync']={'timeout':timeout,'concern':concern,'documents':list(items.find().sort('_id')),'physical_connections':len(set(pool.ids))}
    client.drop_database(database)

async def run():
    pool=Pool()
    async with pymongo.AsyncMongoClient(uri,**settings(pool)) as client:
        items=client[database].items
        await items.insert_one({'_id':'a','value':0})
        await client.admin.command(point({'failCommands':['findAndModify'],'errorCode':50}))
        try:await items.find_one_and_update({'_id':'a'},{'$inc':{'value':1}})
        except pymongo.errors.ExecutionTimeout as error:timeout=observation(error)
        assert (await items.find_one({'_id':'a'}))['value']==0
        await items.find_one_and_update({'_id':'a'},{'$inc':{'value':1}})
        await client.admin.command(point({'failCommands':['insert'],'writeConcernError':{'code':64,'codeName':'WriteConcernFailed','errmsg':'controlled concern failure'}}))
        try:await items.insert_one({'_id':'b'})
        except pymongo.errors.WriteConcernError as error:concern=observation(error)
        assert await items.find_one({'_id':'b'})=={'_id':'b'}
        assert (await client.admin.command('ping'))['ok']==1
        result={'timeout':timeout,'concern':concern,'documents':await items.find().sort('_id').to_list(None),'physical_connections':len(set(pool.ids))}
        await client.drop_database(database)
        return result
results['async']=asyncio.run(run());assert results['sync']==results['async'];assert results['sync']['physical_connections']==1
results['probe_source_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
Path(sys.argv[1]).write_text(json.dumps(results,indent=2)+'\n')

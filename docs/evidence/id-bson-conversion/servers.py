import subprocess,tempfile,os,json,time
from pathlib import Path
from contextlib import ExitStack
import pymongo
root=Path('/Users/uve/Proyectos/mongoeco')
with ExitStack() as stack:
 for i,target in enumerate(('7.0','8.0','9.0')):
  data=stack.enter_context(tempfile.TemporaryDirectory(dir='/tmp/mongoeco-id-conversion'))
  port=37567+i
  p=subprocess.Popen([('/tmp/mongoeco-write-parity/mongodb7/mongodb-macos-aarch64-7.0.43/bin/mongod' if target=='7.0' else f'/opt/homebrew/opt/mongodb-community@{target}/bin/mongod'),'--dbpath',data,'--bind_ip','127.0.0.1','--port',str(port),'--nounixsocket','--logpath',data+'/mongod.log'],stdout=subprocess.DEVNULL)
  stack.callback(lambda p=p:p.wait(timeout=25));stack.callback(p.terminate)
  client=stack.enter_context(pymongo.MongoClient(f'mongodb://127.0.0.1:{port}',serverSelectionTimeoutMS=700))
  for _ in range(30):
   try:client.server_info();break
   except pymongo.errors.ServerSelectionTimeoutError:
    if p.poll() is not None:raise RuntimeError('server died')
  else:raise RuntimeError('server did not start')
  env={**os.environ,'MONGOECO_REAL_MONGODB_URI':f'mongodb://127.0.0.1:{port}','MONGOECO_REAL_MONGODB_TARGET':target}
 print('SERVERS READY ports37567/8/9',flush=True)
 while not Path('/tmp/mongoeco-id-conversion/stop').exists():time.sleep(1)

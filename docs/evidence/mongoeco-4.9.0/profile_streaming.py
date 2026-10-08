import cProfile,pstats,sys,json
from pathlib import Path
from mongoeco import MongoClient
from mongoeco.engines import MemoryEngine
with MongoClient(MemoryEngine()) as client:
 c=client.test.records;c.insert_many([{'_id':i,'n':i} for i in range(20000)])
 def action():
  cur=c.aggregate([{'$set':{'next':{'$add':['$n',1]}}}],batch_size=128)
  try:return list(cur)
  finally:cur.close()
 action(); profiler=cProfile.Profile();profiler.enable();action();profiler.disable();profiler.dump_stats(sys.argv[1]);pstats.Stats(profiler).strip_dirs().sort_stats('cumtime').print_stats(35)

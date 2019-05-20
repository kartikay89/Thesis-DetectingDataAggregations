"""
Parallel processing to run heuristics

"""
# from Heuristic2 import AggFunction
# Heuristic2.reload(AggFunction)
from Heuristic2 import aggregationFunc
import multiprocessing as mp
print("Number of processors: ", mp.cpu_count())

# Init multiprocessing.pool()
pool = mp.Pool(mp.cpu_count())

pool.apply(aggregationFunc)

pool.close()

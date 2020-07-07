import pandas as pd
import numpy as np
import functools
import os
import timeit
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("pyspark_benchmark")\
        .getOrCreate()

def run(df):
  sdf = spark.createDataFrame(df)
  sdf.collect()

for _ in range(int(os.getenv('NUM_ITERS'))):
  df = pd.DataFrame(np.random.randint(-2147483648, 2147483647,size=(100000, 10)), columns=list('ABCDEFGHIJ'))
  t = timeit.Timer(functools.partial(run, df))
  print(t.timeit(number = 1))

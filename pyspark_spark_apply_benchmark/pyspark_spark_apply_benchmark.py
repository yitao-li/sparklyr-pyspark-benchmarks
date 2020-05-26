import pandas as pd
import numpy as np
import timeit
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

spark = SparkSession\
        .builder\
        .appName("pyspark_benchmark")\
        .getOrCreate()

df = pd.DataFrame(np.random.randint(-2147483648, 2147483647,size=(1000, 10)), columns=list('ABCDEFGHIJ'))
sdf = spark.createDataFrame(df)

@udf("double")
def squared(s):
      return s * s

def run():
    global sdf
    squared_cols = [squared(col).alias(col) for col in df.columns]
    return sdf.select(*squared_cols).collect()

print(timeit.timeit(run, number = 1))

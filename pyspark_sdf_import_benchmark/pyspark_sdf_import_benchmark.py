import pandas as pd
import numpy as np
import timeit
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("pyspark_benchmark")\
        .getOrCreate()

df = pd.DataFrame(np.random.randint(-2147483648, 2147483647,size=(100000, 10)), columns=list('ABCDEFGHIJ'))

def run():
    global df
    sdf = spark.createDataFrame(df)
    return sdf

print(timeit.timeit(run, number = 1))

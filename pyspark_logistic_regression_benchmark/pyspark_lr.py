from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import functions as fn

import timeit

spark = SparkSession\
        .builder\
        .appName("pyspark_lr")\
        .getOrCreate()

def run():
  dataset = spark.read.format("parquet")\
    .load("hdfs:///user/spark/warehouse/lr-data-100000.parquet")\
    .withColumn('binary_label',
      fn.when(fn.col("label") >= 0, 1)\
      .otherwise(0)
    )
  assembler = VectorAssembler(
    inputCols=["c_{}".format(x) for x in range(0, 24)],
    outputCol="features")
  dataset = assembler.transform(dataset)

  lr = LogisticRegression(featuresCol = 'features', labelCol='binary_label')
  model = lr.fit(dataset)

for _ in range(10):
  print(timeit.timeit(run, number = 1))

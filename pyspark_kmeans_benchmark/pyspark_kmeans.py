from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

import timeit

spark = SparkSession\
        .builder\
        .appName("pyspark_kmeans")\
        .getOrCreate()

def run():
  dataset = spark.read.format("parquet").load("hdfs:///user/spark/warehouse/kmeans-data.parquet")
  assembler = VectorAssembler(
    inputCols=["c{}".format(x) for x in range(0, 14)],
    outputCol="features")
  dataset = assembler.transform(dataset)

  kmeans = KMeans().setK(3).setSeed(1)
  model = kmeans.fit(dataset)
  predictions = model.transform(dataset)
  evaluator = ClusteringEvaluator()

  silhouette = evaluator.evaluate(predictions)
  # print("Silhouette with squared euclidean distance = " + str(silhouette))

  centers = model.clusterCenters()
  # print("Cluster Centers: ")
  # for center in centers:
  #   print(center)

for _ in range(10):
  print(timeit.timeit(run, number = 1))

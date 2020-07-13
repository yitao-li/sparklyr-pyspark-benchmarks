library(sparklyr)

sc <- spark_connect(master = "yarn-client", spark_home = "/usr/lib/spark", scala_version = "2.12")

for (x in seq(10))
  cat(system.time({
    sdf <- spark_read_parquet(sc, path = "hdfs:///user/spark/warehouse/kmeans-data.parquet")
    res <- ml_kmeans(sdf, ~., k = 3L, seed = 1L)
  })[[3]], "\n")

# print(res)

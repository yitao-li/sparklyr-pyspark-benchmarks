library(sparklyr)

sc <- spark_connect(master = "yarn-client", spark_home = "/usr/lib/spark", scala_version = "2.12")
sdf <- spark_read_parquet(sc, path = "hdfs:///user/spark/warehouse/kmeans-data.parquet")

cat(system.time({
res <- ml_kmeans(sdf, ~., k = 3L, max_iter = 10000, tol = 1e-8)
})[[3]], "\n")

# print(res)

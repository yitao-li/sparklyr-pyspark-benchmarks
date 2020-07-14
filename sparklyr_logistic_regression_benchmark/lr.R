library(sparklyr)

sc <- spark_connect(master = "yarn-client", spark_home = "/usr/lib/spark", scala_version = "2.12")

cat(system.time({
sdf <- spark_read_parquet(sc, "hdfs:///user/spark/warehouse/lr-data-40000.parquet") %>%
  dplyr::mutate(label = (ifelse(label >= 0, 1L, 0L)))

res <- ml_logistic_regression(sdf, label ~ .)
})[[3]], "\n")

# print(res)

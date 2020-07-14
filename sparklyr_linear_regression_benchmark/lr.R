library(sparklyr)

sc <- spark_connect(master = "yarn-client", spark_home = "/usr/lib/spark", scala_version = "2.12")

# data_dir <- "/tmp/spark-bench-test/lr-data.json"
# json_files <- list.files(data_dir, pattern = "*.json")
# 
# df <- data.frame()
# 
# for (json_file in json_files) {
#   rows <- readLines(file.path(data_dir, json_file))
#   for (row in rows) {
#     data <- rjson::fromJSON(row)
#     df <- rbind(df, c(data$label, data$features$values))
#   }
# }
# colnames(df) <- c("label", paste0("c_", seq(0, 23)))
# 
# sdf <- sdf_copy_to(sc, df, overwrite = TRUE)

# spark_write_parquet(sdf, "hdfs:///user/spark/warehouse/lr-data-100000.parquet")

cat(system.time({
sdf <- spark_read_parquet(sc, "hdfs:///user/spark/warehouse/lr-data-100000.parquet")
res <- ml_linear_regression(sdf, label ~ .)
})[[3]], "\n")

# print(res)

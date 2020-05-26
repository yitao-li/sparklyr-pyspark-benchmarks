library(sparklyr)

spark_master <- Sys.getenv("SPARK_MASTER")
spark_home <- Sys.getenv("SPARK_HOME")

df <- data.frame(lapply(seq(10), function(c) sapply(runif(n = 100000, min = -2147483648, max = 2147483647), as.integer)))
colnames(df) <- c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J")

sc <- spark_connect(master = spark_master, spark_home = spark_home)

cat(system.time(sdf_copy_to(sc, df, overwrite = TRUE))[[3]], "\n")

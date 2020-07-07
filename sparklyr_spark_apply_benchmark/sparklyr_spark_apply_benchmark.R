library(sparklyr)

spark_master <- Sys.getenv("SPARK_MASTER")
spark_home <- Sys.getenv("SPARK_HOME")
enable_arrow <- Sys.getenv("ENABLE_ARROW")
num_iters <- as.integer(Sys.getenv("NUM_ITERS"))

config <- spark_config()
if (identical(enable_arrow, "TRUE")) {
  config$`sparklyr.arrow` <- TRUE
  invisible(assertthat::assert_that(require("arrow")))
}

sc <- spark_connect(master = spark_master, spark_home = spark_home, config = config, scala_version = "2.12")
 
for (r in seq(num_iters)) {
  df <- data.frame(lapply(seq(10), function(c) sapply(runif(n = 100000, min = -5000, max = 5000), as.integer)))
  colnames(df) <- c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J")
  cat(system.time({
                  sdf <- sdf_copy_to(sc, df, overwrite = TRUE, repartition = 10L)
                  res <- spark_apply(sdf, memory = TRUE, function(df) df * df, packages = FALSE) %>%
                  sdf_collect()
  })[[3]], "\n")
}

# print(res)

library(sparklyr)

spark_master <- Sys.getenv("SPARK_MASTER")
spark_home <- Sys.getenv("SPARK_HOME")
enable_arrow <- Sys.getenv("ENABLE_ARROW")

df <- data.frame(lapply(seq(10), function(c) sapply(runif(n = 100000, min = -2147483648, max = 2147483647), as.integer)))
colnames(df) <- c("A", "B", "C", "D", "E", "F", "G", "H", "I", "J")

config <- spark_config()
if (identical(enable_arrow, "TRUE")) {
  config$`sparklyr.arrow` <- TRUE
  invisible(assertthat::assert_that(require("arrow")))
}

sc <- spark_connect(master = spark_master, spark_home = spark_home, config = config)
sdf <- sdf_copy_to(sc, df, overwrite = TRUE)

cat(system.time(
                spark_apply(sdf,
                            function(df) {
                              for (col in colnames(df))
                                df[[col]] <- sapply(df[[col]], function(x) x * x)
                            }) %>%
                sdf_collect()
)[[3]], "\n")

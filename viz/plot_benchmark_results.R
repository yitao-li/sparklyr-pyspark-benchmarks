library(ggplot2)

df <- tibble::tibble(
  workload = c(
    rep("pyspark_sdf_import", 100),
    rep("sparklyr_sdf_import", 100)
  ),
  value = c(
    as.numeric(readLines("pyspark_sdf_import_result")),
    as.numeric(readLines("sparklyr_sdf_import_result"))
  )
)

ggplot2::ggplot(data = df, aes(x = workload, y = value, fill=workload)) +
  geom_violin() +
  coord_flip()

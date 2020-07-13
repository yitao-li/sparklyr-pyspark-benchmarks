rm -rf /tmp/spark-bench-test/kmeans-data.parquet
/opt/spark-bench/bin/spark-bench.sh gen_kmeans_data_workload
hadoop fs -rmr /user/spark/warehouse/kmeans-data.parquet
hadoop fs -copyFromLocal /tmp/spark-bench-test/kmeans-data.parquet /user/spark/warehouse

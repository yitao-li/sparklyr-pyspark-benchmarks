rm -rf /tmp/spark-bench-test/lr-data.json
/opt/spark-bench/bin/spark-bench.sh gen_lr_data_workload
hadoop fs -rmr /user/spark/warehouse/lr-data.json
hadoop fs -copyFromLocal /tmp/spark-bench-test/lr-data.json /user/spark/warehouse

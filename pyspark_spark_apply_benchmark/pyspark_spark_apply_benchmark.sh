#!/bin/bash

set -euf -o pipefail

rm -f /tmp/pyspark_spark_apply_result

for _ in $(seq 1 $NUM_ITERS); do
  "$SPARK_HOME"/bin/spark-submit \
    --master "$SPARK_MASTER" \
    "$(dirname "$(readlink -f "$0")")"/pyspark_spark_apply_benchmark.py | tee -a /tmp/pyspark_spark_apply_result
done

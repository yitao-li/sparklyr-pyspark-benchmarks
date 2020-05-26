#!/bin/bash

set -euf -o pipefail

rm -f /tmp/pyspark_sdf_copy_result

for _ in $(seq 1 $NUM_ITERS); do
  "$SPARK_HOME"/bin/spark-submit \
    --master "$SPARK_MASTER" \
    "$(dirname "$(readlink -f "$0")")"/pyspark_sdf_copy_benchmark.py | tee -a /tmp/pyspark_sdf_copy_result
done

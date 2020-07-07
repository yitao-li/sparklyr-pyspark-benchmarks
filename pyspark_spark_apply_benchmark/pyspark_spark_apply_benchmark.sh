#!/bin/bash

set -euf -o pipefail

rm -f /tmp/pyspark_spark_apply_result

"$SPARK_HOME"/bin/spark-submit \
  --master "$SPARK_MASTER" \
  "$(dirname "$(readlink -f "$0")")"/pyspark_spark_apply_benchmark.py 2>/dev/null | tee -a /tmp/pyspark_spark_apply_result

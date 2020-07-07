#!/bin/bash

set -euf -o pipefail

rm -f /tmp/pyspark_sdf_import_and_collect_result

"$SPARK_HOME"/bin/spark-submit \
  --master "$SPARK_MASTER" \
  "$(dirname "$(readlink -f "$0")")"/pyspark_sdf_import_and_collect_benchmark.py 2>/dev/null | tee -a /tmp/pyspark_sdf_import_and_collect_result

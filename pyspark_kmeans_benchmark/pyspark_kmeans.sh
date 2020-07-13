#!/bin/bash

set -euf -o pipefail

SPARK_HOME='/usr/lib/spark'
SPARK_MASTER='yarn-client'
OUTPUT='/tmp/pyspark_kmeans.out'

rm -f "$OUTPUT"

"$SPARK_HOME"/bin/spark-submit \
  --master "$SPARK_MASTER" \
  "$(dirname "$(readlink -f "$0")")"/pyspark_kmeans.py | tee -a "$OUTPUT"

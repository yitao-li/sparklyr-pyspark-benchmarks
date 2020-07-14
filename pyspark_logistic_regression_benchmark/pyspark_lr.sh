#!/bin/bash

set -euf -o pipefail

SPARK_HOME='/usr/lib/spark'
SPARK_MASTER='yarn-client'
OUTPUT='/tmp/pyspark_lr.out'

rm -f "$OUTPUT"

"$SPARK_HOME"/bin/spark-submit \
  --master "$SPARK_MASTER" \
  "$(dirname "$(readlink -f "$0")")"/pyspark_lr.py 2>/dev/null | tee -a "$OUTPUT"

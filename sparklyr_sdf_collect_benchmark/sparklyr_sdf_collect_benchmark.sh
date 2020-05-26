#!/bin/bash

set -euf -o pipefail

rm -f /tmp/sparklyr_sdf_collect_result

for _ in $(seq 1 $NUM_ITERS); do
  Rscript "$(dirname "$(readlink -f "$0")")"/sparklyr_sdf_collect_benchmark.R | tee -a /tmp/sparklyr_sdf_collect_result
done

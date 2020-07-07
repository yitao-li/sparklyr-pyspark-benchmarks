#!/bin/bash

set -euf -o pipefail

rm -f /tmp/sparklyr_spark_apply_result

Rscript "$(dirname "$(readlink -f "$0")")"/sparklyr_spark_apply_benchmark.R | tee -a /tmp/sparklyr_spark_apply_result

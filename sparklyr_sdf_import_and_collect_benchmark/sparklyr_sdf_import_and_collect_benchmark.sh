#!/bin/bash

set -euf -o pipefail

rm -f /tmp/sparklyr_sdf_import_and_collect_result

Rscript "$(dirname "$(readlink -f "$0")")"/sparklyr_sdf_import_and_collect_benchmark.R | tee -a /tmp/sparklyr_sdf_import_and_collect_result

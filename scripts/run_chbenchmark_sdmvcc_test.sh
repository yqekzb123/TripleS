#!/bin/bash
set -e

cd "$(dirname "$0")"
python3 run_experiments.py chbenchmark_sdmvcc_test

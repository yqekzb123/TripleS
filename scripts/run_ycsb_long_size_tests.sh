#!/bin/bash
set -e
cd "$(dirname "$0")"
python3 run_experiments.py ycsb_sdmvcc_long_size

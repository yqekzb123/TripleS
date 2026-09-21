#!/bin/bash
set -e
cd "$(dirname "$0")"
python3 run_experiments.py ycsb_sdpcc_watermark_mode bomb_sdpcc_watermark_mode

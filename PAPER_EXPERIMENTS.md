# TripleS paper experiments

This directory contains only the experiment matrix used by the paper.  Each
invocation creates a timestamped directory under `results/`; every repetition
is retained as an independent run.

## Common configuration

- Build optimization: `-O2`.
- Warm-up: 30 seconds. Measurement window: 30 seconds.
- Default nodes: 2. Scaling experiments override this with 2, 4, 6, and 8.
- Calvin and SDMVCC: `THD_CNT=15` in every experiment.
- Aria: `THD_CNT=16` in every experiment.
- Caracal: `THD_CNT=16` in every experiment.
- Default scheduler count: 3.
- A1 keeps `THD_CNT=15`, so `THD_CNT + 1 = 16` is the fixed total scheduler
  plus executor budget. Scheduler count varies from 1 through 15; the executor
  count reported by the collector is `16 - scheduler_count`.
- Aria batch size: 3000.

## SSS experiment commands

Run from `~/TripleS/SSS`:

```bash
python3 run_experiments.py paper_t1_ycsb_skew
python3 run_experiments.py paper_t2_ycsb_write
python3 run_experiments.py paper_t3_ycsb_dist
python3 run_experiments.py paper_t4_tpcc_warehouses

python3 run_experiments.py paper_h0_motivation
python3 run_experiments.py paper_h1_ycsb_long_ratio
python3 run_experiments.py paper_h2_ycsb_long_size
python3 run_experiments.py paper_h3_bomb_long_ratio
python3 run_experiments.py paper_h4_bomb_long_size

python3 run_experiments.py paper_a1_scheduler_ycsb
python3 run_experiments.py paper_a1_scheduler_bomb
python3 run_experiments.py paper_a2_read_intent_ycsb
python3 run_experiments.py paper_a2_read_intent_bomb
python3 run_experiments.py paper_a3_gc
python3 run_experiments.py paper_a4_coalescing_ycsb
python3 run_experiments.py paper_a4_coalescing_bomb

python3 run_experiments.py paper_s1_scaling_ycsb
python3 run_experiments.py paper_s1_scaling_bomb
```

T1–T4 and H0–H4 include Calvin, Aria, and SDMVCC. A1 includes Calvin and
SDMVCC. A2–A4 and S1 contain SDMVCC only.

## Caracal comparison commands

Run the matching T1–T4 and H0–H4 command names from
`~/TripleS/Caracal`. A1–A4 and S1 are SDMVCC implementation studies,
so they are intentionally absent from Caracal's registry.

## Experiment matrix

| ID | Parameter | Values | Panels |
| --- | --- | --- | --- |
| T1 | YCSB skew | 0.1, 0.3, ..., 1.5 | throughput, P50, P99, rollbacks |  ✅️可跑
| T2 | YCSB write ratio | 0, 0.2, ..., 1.0 | throughput, P50, P99, rollbacks | ✅️可跑
| T3 | distributed transaction ratio | 0.1, 0.2, ..., 1.0 | throughput, P50, P99, rollbacks | ✅️可跑
| T4 | TPC-C warehouses per node | 8, 16, 32, 64, 128 | throughput, P50, P99, rollbacks | ❗️现在Aria/SDMVCC性能为0
| H0 | pure YCSB / mixed BoMB | two workload cases | short throughput, short P99, stall time, stall ratio | ✅️可跑
| H1 | YCSB long ratio | 0.01, 0.05, 0.10, 0.20 | short/long throughput and P99 | ❗️SDMVCC性能不如Calvin
| H2 | YCSB long requests | 100, 500, 1000, 5000 | short/long throughput and P99 | ❗️SDMVCC性能不如Calvin
| H3 | BoMB L1 request percent | 0.1, 0.5, 1, 5, 10 | short/long throughput and P99 | ✅️可跑  
| H4 | BoMB target products | 10, 25, 50, 100, 200 | short/long throughput and P99 | ✅️可跑 ❗️现在Aria在target products多以后，性能为0
| A1 | scheduler count | 1 through 15 | YCSB/BoMB throughput and P99 | ✅️可跑
| A2 | eager / lazy read intent | two modes | YCSB/BoMB throughput and P99 | ✅️可跑
| A3 | read-intent GC | off/on × H4 sizes | 6 panels including metadata lengths | ✅️可跑
| A4 | distributed watermark | off/on | YCSB/BoMB throughput and watermark wait | ✅️可跑
| S1 | node count | 2, 4, 6, 8 | 6 YCSB/BoMB scalability panels | ❗️必须可跑，还没跑

`A3: GC disabled` is the current no-reclamation baseline. The codebase does
not currently provide a separate conventional scanning-GC implementation, so
the collector does not mislabel this baseline as “conventional GC”.

## Collect results

Run from SSS after any round:

```bash
python3 collect_paper_results.py \
  --results ~/TripleS/SSS/results \
  --caracal-results ~/TripleS/Caracal/results \
  --output ~/TripleS/paper_results
```

The output contains `paper_results.xlsx`, complete and per-group CSV files,
one SVG per experiment group, `missing.csv`, and `manifest.json`. Missing data
is written as `MISSING`; measured or protocol-defined zero remains numeric
zero. Long and short throughput uses completed transactions divided by the
measurement interval. P99 is marked `INSUFFICIENT` when its class has fewer
than 100 samples. Raw configuration and node metrics remain in CSV for audit.


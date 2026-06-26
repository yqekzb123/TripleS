#!/usr/bin/env python3
"""
Compute average retry count from files containing lines like:
sdocc_retry_cnts
,rcnt0=0,rcnt1=44967,rcnt2=130939,rcnt3=1758,...

Usage:
  python3 avg_retries.py file1.out [file2.out ...]
Options:
  --include-zero    include rcnt0 in averaging (default: exclude rcnt0)
  --precision N     decimal places for output (default: 6)
"""
import argparse
import re
import sys
from pathlib import Path
from typing import Tuple

RCNT_RE = re.compile(r'rcnt(\d+)=([0-9]+)')

def parse_rcnts(text: str) -> dict:
    counts = {}
    for m in RCNT_RE.finditer(text):
        n = int(m.group(1))
        v = int(m.group(2))
        counts[n] = counts.get(n, 0) + v
    return counts

def compute_avg(counts: dict, include_zero: bool = False) -> Tuple[float, int, int]:
    num = 0
    den = 0
    for k, v in counts.items():
        if k == 0 and not include_zero:
            continue
        num += k * v
        den += v
    if den == 0:
        return float('nan'), num, den
    return num / den, num, den

def find_rcnt_block(text: str) -> str:
    # try to find the line that starts with "sdocc_retry_cnts" and the next part
    # but more robustly, return whole file (we only search rcntN=... patterns)
    return text

def process_file(path: Path, include_zero: bool) -> Tuple[str, dict]:
    try:
        txt = path.read_text()
    except Exception as e:
        raise RuntimeError(f"Can't read {path}: {e}")
    block = find_rcnt_block(txt)
    counts = parse_rcnts(block)
    return path.name, counts

def main():
    p = argparse.ArgumentParser(description="Average retry count calculator")
    p.add_argument('files', nargs='*', help='Files to scan')
    p.add_argument('--results-dir', '-d', help='Directory containing result files to scan (will match by prefix)')
    p.add_argument('--prefix', '-p', default='0_SDOCC', help='Filename prefix to match when using --results-dir (default: 0_SDOCC)')
    p.add_argument('--include-zero', action='store_true', help='Include rcnt0 in average')
    p.add_argument('--precision', type=int, default=6, help='Decimal places in output')
    p.add_argument('--aria', action='store_true', help='Parse ARIA [summary] and compute abort-based retry averages')
    args = p.parse_args()

    combined_counts = {}
    any_found = False

    # gather file list either from explicit files or from a results directory
    files_to_process = []
    if args.results_dir:
        d = Path(args.results_dir)
        if not d.is_dir():
            print(f"[ERROR] --results-dir is not a directory: {args.results_dir}", file=sys.stderr)
            sys.exit(2)
        # recursive listing of files whose basename starts with the prefix
        for child in sorted(d.rglob('*')):
            if child.is_file() and child.name.startswith(args.prefix):
                files_to_process.append(child)
    # append any explicit files passed on the command line
    for fp in args.files:
        files_to_process.append(Path(fp))

    if not files_to_process:
        print("No files to process. Provide files or use --results-dir.", file=sys.stderr)
        sys.exit(2)

    # For each file, print a single line: "<filename> Combined: numerator=..., denominator=..., average=..."
    for path in files_to_process:
        if not path.exists():
            print(f"[WARN] file not found: {path}", file=sys.stderr)
            continue
        name, counts = process_file(path, args.include_zero)
        txt = None
        if args.aria:
            # read file text once for ARIA parsing
            try:
                txt = Path(path).read_text()
            except Exception:
                txt = None
        # sanitize filename to avoid accidental newlines in output
        safe_name = str(name).replace('\n', '').replace('\r', '')
        if args.aria:
            # parse [summary] line for total_txn_abort_cnt, unique_txn_abort_cnt, txn_cnt
            if not txt:
                print(f"{safe_name} ARIA: total_txn_abort_cnt=nan, unique_txn_abort_cnt=nan, txn_cnt=nan, only_retriers=nan, all_txns=nan")
                continue
            # find [summary] line
            m = re.search(r'\[summary\]\s*(.*)', txt)
            if not m:
                print(f"{safe_name} ARIA: no [summary] found")
                continue
            kvs = m.group(1)
            # parse comma-separated key=val pairs
            pairs = re.findall(r'([a-zA-Z0-9_]+)=([-+]?[0-9]*\.?[0-9]+)', kvs)
            summary = {k: float(v) for k, v in pairs}
            total_abort = summary.get('total_txn_abort_cnt', float('nan'))
            unique_abort = summary.get('unique_txn_abort_cnt', float('nan'))
            txn_cnt = summary.get('txn_cnt', float('nan'))
            total_commit = summary.get('total_txn_commit_cnt', float('nan'))
            only_retriers = float('nan')
            all_txns = float('nan')
            if unique_abort and not (unique_abort != unique_abort):
                # avoid division by zero
                try:
                    only_retriers = total_abort / unique_abort
                except Exception:
                    only_retriers = float('nan')
            # user requested denominator = total_txn_commit_cnt + unique_txn_abort_cnt
            denom_all = float('nan')
            if not (total_commit != total_commit) and not (unique_abort != unique_abort):
                denom_all = total_commit + unique_abort
            if denom_all and not (denom_all != denom_all):
                try:
                    all_txns = total_abort / denom_all
                except Exception:
                    all_txns = float('nan')
            print(f"{safe_name} ARIA: total_txn_abort_cnt={int(total_abort) if total_abort==int(total_abort) else total_abort}, unique_txn_abort_cnt={int(unique_abort) if unique_abort==int(unique_abort) else unique_abort}, txn_cnt={int(txn_cnt) if txn_cnt==int(txn_cnt) else txn_cnt}, only_retriers={only_retriers:.{args.precision}f}, all_txns={all_txns:.{args.precision}f}")
            continue

        if not counts:
            print(f"{safe_name} Combined: numerator=0, denominator=0, average=nan")
            continue
        any_found = True
        avg, num, den = compute_avg(counts, include_zero=args.include_zero)
        print(f"{safe_name} Combined: numerator={num}, denominator={den}, average={avg:.{args.precision}f}")

    # if in aria mode we already printed ARIA per-file results; skip rcnt-only summary
    if args.aria:
        return

    if not any_found:
        print("No rcnt entries found in any provided files.", file=sys.stderr)
        sys.exit(2)

    avg_all, num_all, den_all = compute_avg(combined_counts, include_zero=args.include_zero)
    print("-" * 50)
    print(f"Combined: numerator={num_all}, denominator={den_all}, average={avg_all:.{args.precision}f}")

if __name__ == '__main__':
    main()
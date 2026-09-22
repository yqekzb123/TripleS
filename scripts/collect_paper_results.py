#!/usr/bin/env python3
"""Collect TripleS paper results into CSV, XLSX, and multi-panel SVG files.

The collector reads immutable .cfg snapshots and raw node .out files.  Every
run remains a separate row.  Missing values are emitted as the literal
``MISSING``; zero is reserved for a measured or protocol-defined zero.
"""

import argparse
import csv
import json
import math
import re
import statistics
import zipfile
from collections import defaultdict
from pathlib import Path
from xml.sax.saxutils import escape

MISSING = "MISSING"
MIN_P99_SAMPLES = 100

GROUP_PATTERNS = [
    ("T1", ("paper_t1_", "ycsb_skew")),
    ("T2", ("paper_t2_", "ycsb_write")),
    ("T3", ("paper_t3_", "ycsb_dist")),
    ("T4", ("paper_t4_", "tpcc_wh")),
    ("H0", ("paper_h0_", "fig1_")),
    ("H1", ("paper_h1_",)),
    ("H2", ("paper_h2_", "ycsb_sdmvcc_long_size", "ycsb_caracal_long_size")),
    ("H3", ("paper_h3_",)),
    ("H4", ("paper_h4_",)),
    ("A1", ("paper_a1_", "scheduler_sweep", "ycsb_sch_cnt")),
    ("A2", ("paper_a2_", "lazy_intent_ablation")),
    ("A3", ("paper_a3_", "gc_ablation")),
    ("A4", ("paper_a4_", "watermark_mode")),
    ("S1", ("paper_s1_", "scaling")),
]

CORE_COLUMNS = [
    "group", "experiment", "round", "repository", "run_id", "protocol",
    "workload", "node_count", "worker_count", "scheduler_count",
    "batch_size", "x_name", "x_value", "variant", "measurement_s",
    "throughput_txn_s", "p50_s", "p99_s", "rollback_count",
    "short_throughput_txn_s", "short_p50_s", "short_p99_s",
    "short_samples", "long_throughput_txn_s", "long_p50_s", "long_p99_s",
    "long_samples", "short_p99_quality", "long_p99_quality",
    "watermark_wait_avg_ns", "watermark_wait_count",
    "sync_stall_avg_s", "sync_stall_ratio", "sync_stall_source",
    "version_chain_avg", "version_chain_peak", "read_intents_active",
    "read_intents_peak", "gc_calls", "versions_reclaimed",
    "p99_quality", "missing_node_count", "source_cfg", "config_json",
    "raw_metrics_json",
]

# Keep full provenance in CSV.  The workbook stays reviewable and avoids
# Excel's 32,767-character cell limit for raw per-node JSON.
XLSX_COLUMNS = [column for column in CORE_COLUMNS
                if column not in ("config_json", "raw_metrics_json")]

METRIC_DICTIONARY = [
    ("throughput_txn_s", "txn/s", "sum of server-node tput", "All committed transactions"),
    ("p50_s", "s", "mean of node P50", "Node fscl50; raw per-node distributions are unavailable"),
    ("p99_s", "s", "mean of node P99", "Node fscl99; raw per-node distributions are unavailable"),
    ("rollback_count", "count/window", "sum across nodes", "SDMVCC is protocol-defined zero"),
    ("short_throughput_txn_s", "txn/s", "completed short transactions / measured seconds", "Never inferred from configured ratio"),
    ("short_p50_s", "s", "YCSB class P50 or BoMB merged short distribution", "BoMB requires the new bomb_short_p50_ns field"),
    ("short_p99_s", "s", "YCSB class P99 or BoMB merged short distribution", "Flagged when samples < 100"),
    ("long_throughput_txn_s", "txn/s", "completed long transactions / measured seconds", "Never inferred from configured ratio"),
    ("long_p50_s", "s", "mean of node long-transaction P50", "Long transaction itself"),
    ("long_p99_s", "s", "mean of node long-transaction P99", "Flagged when samples < 100"),
    ("short_p99_quality", "label", "sample-count check", "INSUFFICIENT below 100 short samples"),
    ("long_p99_quality", "label", "sample-count check", "INSUFFICIENT below 100 long samples"),
    ("watermark_wait_avg_ns", "ns/wait", "count-weighted mean across nodes", "sdpcc_watermark_avg_wait_ns"),
    ("sync_stall_avg_s", "s/event", "phase-idle/count or watermark wait", "Source named in sync_stall_source"),
    ("sync_stall_ratio", "ratio", "idle seconds / thread capacity seconds", "Aria/Caracal barrier or SDMVCC scheduler idle"),
    ("version_chain_avg", "versions/row", "mean across nodes", "sdmvcc_avg_version_chain at run end"),
    ("version_chain_peak", "versions", "max across nodes", "sdmvcc_peak_version_chain"),
    ("read_intents_active", "entries", "sum across nodes", "Active at run end, not a time average"),
    ("read_intents_peak", "entries", "sum of node peaks", "Peak is sampled by implementation"),
]


def scalar(value):
    if value is None:
        return None
    text = str(value).strip().strip('"')
    if text.lower() in ("true", "false"):
        return text.lower()
    try:
        return float(text) if any(c in text for c in ".eE") else int(text)
    except ValueError:
        return text


def parse_cfg(path):
    result = {}
    for line in path.read_text(errors="replace").splitlines():
        match = re.match(r"\s*#define\s+([A-Za-z_]\w*)\s+(.+?)\s*$", line)
        if not match:
            continue
        value = match.group(2).split("//", 1)[0].strip()
        result[match.group(1)] = scalar(value)
    return result


def parse_kv(text):
    result = {}
    for key, value in re.findall(r"\b([A-Za-z_]\w*)\s*=\s*([^,\s]+)", text):
        parsed = scalar(value)
        if isinstance(parsed, (int, float)):
            result[key] = float(parsed)
    return result


def parse_output(path):
    summary = None
    timeseries = []
    for line in path.read_text(errors="replace").splitlines():
        if "[summary]" in line:
            summary = parse_kv(line.split("[summary]", 1)[1])
        elif "[timeseries]" in line:
            timeseries.append(parse_kv(line.split("[timeseries]", 1)[1]))
    return summary or {}, timeseries


def classify(name):
    lower = name.lower()
    for group, patterns in GROUP_PATTERNS:
        if any(pattern in lower for pattern in patterns):
            return group
    return "UNCLASSIFIED"


def mean_metric(nodes, key):
    values = [n[key] for n in nodes if key in n]
    return statistics.fmean(values) if values else None


def sum_metric(nodes, key):
    values = [n[key] for n in nodes if key in n]
    return sum(values) if values else None


def max_metric(nodes, key):
    values = [n[key] for n in nodes if key in n]
    return max(values) if values else None


def weighted_metric(nodes, value_key, count_key):
    pairs = [(n[value_key], n[count_key]) for n in nodes
             if value_key in n and count_key in n and n[count_key] > 0]
    total = sum(count for _, count in pairs)
    return sum(value * count for value, count in pairs) / total if total else None


def bool_label(value, true_label, false_label):
    return true_label if str(value).lower() == "true" else false_label


def x_axis(group, cfg, workload):
    if group == "T1": return "Skew factor", cfg.get("ZIPF_THETA")
    if group == "T2": return "Write ratio", cfg.get("TUP_WRITE_PERC")
    if group == "T3": return "Distributed transaction ratio", cfg.get("MPR")
    if group == "T4":
        nodes = cfg.get("NODE_CNT") or 1
        wh = cfg.get("NUM_WH")
        return "Warehouses per node", wh / nodes if isinstance(wh, (int, float)) else None
    if group == "H0": return "Workload case", "Pure TP" if workload == "YCSB" else "HTAP"
    if group == "H1": return "Long transaction ratio", cfg.get("LONG_QUERY_PERC")
    if group == "H2": return "Long transaction requests", cfg.get("REQ_PER_QUERY")
    if group == "H3": return "Long transaction request percent", cfg.get("BOMB_L1_RANDOM_PCT")
    if group in ("H4", "A3"): return "BoMB target products", cfg.get("BOMB_TARGET_PRODUCTS")
    if group == "A1": return "Scheduler count", cfg.get("SCHEDULER_CNT")
    if group == "A2": return "Read intent mode", bool_label(cfg.get("SDMVCC_LAZY_READ_INTENT"), "Lazy", "Eager")
    if group == "A4": return "Distributed watermark", bool_label(cfg.get("OPEN_DISTRIBUTED_WATERMARK"), "On", "Off")
    if group == "S1": return "Node count", cfg.get("NODE_CNT")
    return "Parameter", None


def variant(group, cfg):
    if group == "A2":
        return bool_label(cfg.get("SDMVCC_LAZY_READ_INTENT"), "Lazy read intent", "Eager read intent")
    if group == "A3":
        return bool_label(cfg.get("SDMVCC_INTENT_GC"), "Read-intent GC", "GC disabled")
    if group == "A4":
        return bool_label(cfg.get("OPEN_DISTRIBUTED_WATERMARK"), "Distributed watermark (unoptimized)", "Coalesced watermark")
    return str(cfg.get("CC_ALG", "UNKNOWN"))


def merged_short_latency(nodes, percentile):
    direct = mean_metric(nodes, "bomb_short_%s_ns" % percentile)
    if direct is not None:
        return direct / 1e9
    # Historical fallback is explicit and only used when merged fields did not
    # exist. It is weighted by per-type committed count, not a true percentile.
    weighted = []
    for node in nodes:
        for kind in ("s1", "s2", "s3", "s4", "s5"):
            value = node.get("bomb_%s_%s_ns" % (kind, percentile))
            count = node.get("bomb_%s_committed" % kind, 0)
            if value is not None and count > 0:
                weighted.append((value, count))
    count = sum(c for _, c in weighted)
    return sum(v * c for v, c in weighted) / count / 1e9 if count else None


def aggregate_run(cfg_path, repository):
    cfg = parse_cfg(cfg_path)
    experiment = cfg_path.parent.name
    group = classify(experiment)
    node_count = int(cfg.get("NODE_CNT", 0) or 0)
    nodes, node_rows, series_rows = [], [], []
    for node_id in range(node_count):
        path = cfg_path.parent / (str(node_id) + "_" + cfg_path.stem + ".out")
        if not path.exists():
            continue
        metrics, timeseries = parse_output(path)
        nodes.append(metrics)
        node_rows.append({"run_id": cfg_path.stem, "node": node_id,
                          "source": str(path), "metrics_json": json.dumps(metrics, sort_keys=True)})
        for point in timeseries:
            point.update({"run_id": cfg_path.stem, "node": node_id,
                          "group": group, "protocol": cfg.get("CC_ALG", repository.upper())})
            series_rows.append(point)

    protocol = str(cfg.get("CC_ALG", "CARACAL" if repository == "Caracal" else "UNKNOWN"))
    workload = str(cfg.get("WORKLOAD", "UNKNOWN"))
    runtime = mean_metric(nodes, "total_runtime")
    if runtime is None:
        timer = str(cfg.get("DONE_TIMER", ""))
        match = re.search(r"([0-9.]+)\s*\*\s*BILLION", timer)
        runtime = float(match.group(1)) if match else None
    x_name, x_value = x_axis(group, cfg, workload)

    throughput = sum_metric(nodes, "tput")
    p50 = mean_metric(nodes, "fscl50")
    p99 = mean_metric(nodes, "fscl99")
    rollback = sum_metric(nodes, "total_txn_abort_cnt")
    if rollback is None:
        rollback = sum_metric(nodes, "unique_txn_abort_cnt")
    if rollback is None and workload == "YCSB":
        values = [sum(n.get("ycsb_short_aborted", 0) + n.get("ycsb_long_aborted", 0)
                      for n in nodes)] if nodes else []
        rollback = values[0] if values else None
    if rollback is None and workload == "BOMB":
        rollback = sum(sum(n.get("bomb_%s_aborted" % k, 0)
                           for k in ("l1", "s1", "s2", "s3", "s4", "s5")) for n in nodes) if nodes else None
    if rollback is None and protocol in ("SDMVCC", "CALVIN", "CARACAL"):
        rollback = 0

    short_tput = short_p50 = short_p99 = short_samples = None
    long_tput = long_p50 = long_p99 = long_samples = None
    if workload == "YCSB":
        short_count = sum_metric(nodes, "ycsb_short_committed")
        long_count = sum_metric(nodes, "ycsb_long_committed")
        short_samples = sum_metric(nodes, "ycsb_short_samples")
        long_samples = sum_metric(nodes, "ycsb_long_samples")
        short_tput = short_count / runtime if short_count is not None and runtime else (throughput if cfg.get("LONG_QUERY_PERC", 0) in (0, 0.0) else None)
        long_tput = long_count / runtime if long_count is not None and runtime else None
        short_p50, short_p99 = mean_metric(nodes, "ycsb_short_p50"), mean_metric(nodes, "ycsb_short_p99")
        long_p50, long_p99 = mean_metric(nodes, "ycsb_long_p50"), mean_metric(nodes, "ycsb_long_p99")
        if short_p50 is None and cfg.get("LONG_QUERY_PERC", 0) in (0, 0.0): short_p50, short_p99 = p50, p99
    elif workload == "BOMB":
        short_count = sum_metric(nodes, "bomb_short_committed")
        if short_count is None:
            short_count = sum(sum(n.get("bomb_%s_committed" % k, 0) for k in ("s1", "s2", "s3", "s4", "s5")) for n in nodes) if nodes else None
        long_count = sum_metric(nodes, "bomb_l1_committed")
        short_samples = sum_metric(nodes, "bomb_short_samples") or short_count
        long_samples = long_count
        short_tput = short_count / runtime if short_count is not None and runtime else sum_metric(nodes, "bomb_short_tput")
        long_tput = long_count / runtime if long_count is not None and runtime else sum_metric(nodes, "bomb_long_tput")
        short_p50, short_p99 = merged_short_latency(nodes, "p50"), merged_short_latency(nodes, "p99")
        long_p50 = mean_metric(nodes, "bomb_l1_p50_ns")
        long_p99 = mean_metric(nodes, "bomb_l1_p99_ns")
        long_p50 = long_p50 / 1e9 if long_p50 is not None else None
        long_p99 = long_p99 / 1e9 if long_p99 is not None else None

    wait_count = sum_metric(nodes, "sdpcc_watermark_wait_count")
    wait_ns = weighted_metric(nodes, "sdpcc_watermark_avg_wait_ns", "sdpcc_watermark_wait_count")
    sync_avg = sync_ratio = sync_source = None
    if protocol == "ARIA":
        times = ("aria_read_phase_idle_time", "aria_reservation_phase_idle_time",
                 "aria_check_phase_idle_time", "aria_commit_phase_idle_time")
        counts = tuple(k.replace("_time", "_cnt") for k in times)
        total_time, total_count = sum((sum_metric(nodes, k) or 0) for k in times), sum((sum_metric(nodes, k) or 0) for k in counts)
        sync_avg = total_time / total_count if total_count else None
        capacity = (runtime or 0) * (cfg.get("THREAD_CNT", 0) or 0) * node_count
        sync_ratio = total_time / capacity if capacity else None
        sync_source = "Aria phase barriers"
    elif protocol == "CARACAL":
        times = ("caracal_init_phase_idle_time", "caracal_append_phase_idle_time", "caracal_execution_phase_idle_time")
        counts = tuple(k.replace("_time", "_cnt") for k in times)
        total_time, total_count = sum((sum_metric(nodes, k) or 0) for k in times), sum((sum_metric(nodes, k) or 0) for k in counts)
        sync_avg = total_time / total_count if total_count else None
        capacity = (runtime or 0) * (cfg.get("THREAD_CNT", 0) or 0) * node_count
        sync_ratio = total_time / capacity if capacity else None
        sync_source = "Caracal phase barriers"
    elif protocol in ("SDMVCC", "SDPCC"):
        sync_avg = wait_ns / 1e9 if wait_ns is not None else None
        idle = sum_metric(nodes, "sched_idle_time")
        capacity = (runtime or 0) * (cfg.get("SCHEDULER_CNT", 0) or 0) * node_count
        sync_ratio = idle / capacity if idle is not None and capacity else None
        sync_source = "Watermark wait; ratio uses scheduler idle"
    elif protocol == "CALVIN":
        sync_avg = mean_metric(nodes, "sched_idle_avg_time")
        idle = sum_metric(nodes, "sched_idle_time")
        capacity = (runtime or 0) * (cfg.get("SCHEDULER_CNT", 0) or 0) * node_count
        sync_ratio = idle / capacity if idle is not None and capacity else None
        sync_source = "Calvin scheduler idle"

    def sample_quality(sample_count):
        if sample_count is None:
            return MISSING
        return "INSUFFICIENT" if sample_count < MIN_P99_SAMPLES else "OK"
    samples = long_samples if long_tput not in (None, 0) else short_samples
    quality = sample_quality(samples)
    row = {
        "group": group, "experiment": experiment,
        "round": next((part for part in cfg_path.parts if re.fullmatch(r"20\d{6}-\d{6}", part)), "UNKNOWN"),
        "repository": repository, "run_id": cfg_path.stem, "protocol": protocol,
        "workload": workload, "node_count": node_count,
        # For Calvin-family protocols, THD_CNT + 1 is the fixed total of
        # scheduler and executor threads.  Expose the actual executor count.
        "worker_count": ((cfg.get("THREAD_CNT", 0) + 1 - cfg.get("SCHEDULER_CNT", 0))
                         if protocol in ("CALVIN", "SDMVCC", "SDPCC")
                         else cfg.get("THREAD_CNT")),
        "scheduler_count": cfg.get("SCHEDULER_CNT"),
        "batch_size": cfg.get("ARIA_BATCH_SIZE"), "x_name": x_name, "x_value": x_value,
        "variant": variant(group, cfg), "measurement_s": runtime,
        "throughput_txn_s": throughput, "p50_s": p50, "p99_s": p99,
        "rollback_count": rollback,
        "short_throughput_txn_s": short_tput, "short_p50_s": short_p50,
        "short_p99_s": short_p99, "short_samples": short_samples,
        "long_throughput_txn_s": long_tput, "long_p50_s": long_p50,
        "long_p99_s": long_p99, "long_samples": long_samples,
        "short_p99_quality": sample_quality(short_samples),
        "long_p99_quality": sample_quality(long_samples),
        "watermark_wait_avg_ns": wait_ns, "watermark_wait_count": wait_count,
        "sync_stall_avg_s": sync_avg, "sync_stall_ratio": sync_ratio,
        "sync_stall_source": sync_source,
        "version_chain_avg": mean_metric(nodes, "sdmvcc_avg_version_chain"),
        "version_chain_peak": max_metric(nodes, "sdmvcc_peak_version_chain"),
        "read_intents_active": sum_metric(nodes, "sdmvcc_active_intents"),
        "read_intents_peak": sum_metric(nodes, "sdmvcc_peak_active_intents"),
        "gc_calls": sum_metric(nodes, "sdmvcc_gc_calls"),
        "versions_reclaimed": sum_metric(nodes, "sdmvcc_versions_reclaimed"),
        "p99_quality": quality, "missing_node_count": node_count - len(nodes),
        "source_cfg": str(cfg_path), "config_json": json.dumps(cfg, sort_keys=True),
        "raw_metrics_json": json.dumps(nodes, sort_keys=True),
    }
    return row, node_rows, series_rows


def discover(roots):
    rows, nodes, series = [], [], []
    for repository, root in roots:
        if not root.exists():
            continue
        for cfg in sorted(root.rglob("*.cfg")):
            row, node_rows, series_rows = aggregate_run(cfg, repository)
            rows.append(row); nodes.extend(node_rows); series.extend(series_rows)
    return rows, nodes, series


def display(value):
    return MISSING if value is None else value


def write_csv(path, rows, columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: display(row.get(key)) for key in columns})


def excel_col(index):
    value = ""
    while index:
        index, rem = divmod(index - 1, 26); value = chr(65 + rem) + value
    return value


def sheet_xml(rows):
    widths = []
    for col in range(max((len(r) for r in rows), default=0)):
        widths.append(min(45, max(10, max((len(str(r[col])) if col < len(r) and r[col] is not None else 0 for r in rows), default=0) + 2)))
    data = []
    for rindex, row in enumerate(rows, 1):
        cells = []
        for cindex, value in enumerate(row, 1):
            ref = "%s%d" % (excel_col(cindex), rindex)
            style = 1 if rindex == 1 else (2 if value == MISSING else 0)
            if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
                cells.append('<c r="%s" s="%d"><v>%s</v></c>' % (ref, style, value))
            else:
                cells.append('<c r="%s" s="%d" t="inlineStr"><is><t>%s</t></is></c>' % (ref, style, escape("" if value is None else str(value))))
        data.append('<row r="%d">%s</row>' % (rindex, "".join(cells)))
    cols = "".join('<col min="%d" max="%d" width="%.1f" customWidth="1"/>' % (i+1, i+1, w) for i, w in enumerate(widths))
    last = "%s%d" % (excel_col(len(widths) or 1), len(rows) or 1)
    return ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>'
            '<cols>%s</cols><sheetData>%s</sheetData><autoFilter ref="A1:%s"/></worksheet>' % (cols, "".join(data), last))


def write_xlsx(path, sheets):
    path.parent.mkdir(parents=True, exist_ok=True)
    names = []
    for name, _ in sheets:
        clean = re.sub(r"[\\/*?:\[\]]", "_", name)[:31] or "Sheet"
        base, count = clean, 2
        while clean in names:
            clean = (base[:27] + "_%d" % count)[:31]; count += 1
        names.append(clean)
    content_types = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
        '<Default Extension="xml" ContentType="application/xml"/>',
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>']
    for i in range(len(sheets)):
        content_types.append('<Override PartName="/xl/worksheets/sheet%d.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' % (i+1))
    content_types.append('</Types>')
    workbook = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>%s</sheets></workbook>' %
        "".join('<sheet name="%s" sheetId="%d" r:id="rId%d"/>' % (escape(name), i+1, i+1) for i, name in enumerate(names)))
    rels = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>', '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">']
    for i in range(len(sheets)):
        rels.append('<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet%d.xml"/>' % (i+1, i+1))
    rels.append('<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>' % (len(sheets)+1)); rels.append('</Relationships>')
    styles = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="3"><font><sz val="10"/><name val="Arial"/></font><font><b/><color rgb="FFFFFFFF"/><sz val="10"/><name val="Arial"/></font><font><color rgb="FF9C0006"/><sz val="10"/><name val="Arial"/></font></fonts>'
        '<fills count="4"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FF1F4E78"/><bgColor indexed="64"/></patternFill></fill><fill><patternFill patternType="solid"><fgColor rgb="FFFFC7CE"/><bgColor indexed="64"/></patternFill></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="3"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf><xf numFmtId="0" fontId="2" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1"/></cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>')
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as book:
        book.writestr("[Content_Types].xml", "".join(content_types))
        book.writestr("_rels/.rels", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
        book.writestr("xl/workbook.xml", workbook); book.writestr("xl/_rels/workbook.xml.rels", "".join(rels)); book.writestr("xl/styles.xml", styles)
        for i, (_, rows) in enumerate(sheets, 1): book.writestr("xl/worksheets/sheet%d.xml" % i, sheet_xml(rows))


def expected_points():
    protocols = ("CALVIN", "ARIA", "SDMVCC", "CARACAL")
    result = []
    def add(group, workload, xs, protos=protocols, variants=None):
        for proto in protos:
            for x in xs:
                if variants:
                    for var in variants: result.append((group, workload, proto, x, var))
                else: result.append((group, workload, proto, x, proto))
    add("T1", "YCSB", (0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5)); add("T2", "YCSB", (0.0,0.2,0.4,0.6,0.8,1.0)); add("T3", "YCSB", (0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)); add("T4", "TPCC", (8,16,32,64,128))
    for proto in protocols:
        for case in ("Pure TP", "HTAP"): result.append(("H0", "YCSB" if case == "Pure TP" else "BOMB", proto, case, proto))
    add("H1", "YCSB", (0.01,0.05,0.10,0.20)); add("H2", "YCSB", (100,500,1000,5000)); add("H3", "BOMB", (0.1,0.5,1.0,5.0,10.0)); add("H4", "BOMB", (10,25,50,100,200))
    for workload in ("YCSB", "BOMB"):
        for proto in ("CALVIN", "SDMVCC"):
            for x in range(1,16): result.append(("A1", workload, proto, x, proto))
        for var in ("Eager read intent", "Lazy read intent"): result.append(("A2", workload, "SDMVCC", "Lazy" if var.startswith("Lazy") else "Eager", var))
        for var in ("Coalesced watermark", "Distributed watermark (unoptimized)"): result.append(("A4", workload, "SDMVCC", "Off" if var.startswith("Coalesced") else "On", var))
    for x in (10,25,50,100,200):
        for var in ("GC disabled", "Read-intent GC"): result.append(("A3", "BOMB", "SDMVCC", x, var))
    for workload in ("YCSB", "BOMB"):
        for x in (2,4,6,8): result.append(("S1", workload, "SDMVCC", x, "SDMVCC"))
    return result


def missing_rows(rows):
    def normalized(value):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return "%.12g" % value
        return str(value)
    available = {(r["group"], r["workload"], r["protocol"],
                  normalized(r["x_value"]), r["variant"]) for r in rows}
    result = []
    for group, workload, protocol, x, var in expected_points():
        key = (group, workload, protocol, normalized(x), var)
        if key not in available:
            result.append({"kind":"CONFIG", "group":group, "workload":workload, "protocol":protocol, "x_value":x, "variant":var, "metric":MISSING, "run_id":MISSING})
    required = {
        "T1": ("throughput_txn_s","p50_s","p99_s","rollback_count"), "T2": ("throughput_txn_s","p50_s","p99_s","rollback_count"),
        "T3": ("throughput_txn_s","p50_s","p99_s","rollback_count"), "T4": ("throughput_txn_s","p50_s","p99_s","rollback_count"),
        "H0": ("short_throughput_txn_s","short_p99_s","sync_stall_avg_s","sync_stall_ratio"),
        "H1": ("short_throughput_txn_s","short_p99_s","long_throughput_txn_s","long_p99_s"), "H2": ("short_throughput_txn_s","short_p99_s","long_throughput_txn_s","long_p99_s"),
        "H3": ("short_throughput_txn_s","short_p99_s","long_throughput_txn_s","long_p99_s"), "H4": ("short_throughput_txn_s","short_p99_s","long_throughput_txn_s","long_p99_s"),
        "A3": ("short_throughput_txn_s","short_p99_s","long_throughput_txn_s","long_p99_s","version_chain_avg","read_intents_peak"),
    }
    for row in rows:
        metrics = required.get(row["group"], ())
        if row["group"] in ("A1", "A2"):
            metrics = (("throughput_txn_s", "p99_s")
                       if row["workload"] == "YCSB" else
                       ("short_throughput_txn_s", "short_p99_s"))
        elif row["group"] == "A4":
            metrics = (("throughput_txn_s", "watermark_wait_avg_ns")
                       if row["workload"] == "YCSB" else
                       ("short_throughput_txn_s", "watermark_wait_avg_ns"))
        elif row["group"] == "S1":
            metrics = (("throughput_txn_s", "p99_s")
                       if row["workload"] == "YCSB" else
                       ("short_throughput_txn_s", "short_p99_s",
                        "long_throughput_txn_s", "long_p99_s"))
        for metric in metrics:
            if row.get(metric) is None:
                result.append({"kind":"METRIC", "group":row["group"], "workload":row["workload"], "protocol":row["protocol"], "x_value":row["x_value"], "variant":row["variant"], "metric":metric, "run_id":row["run_id"]})
    return result


PLOT_SPECS = {
 "T1":[("Throughput","throughput_txn_s"),("P50","p50_s"),("P99","p99_s"),("Rollbacks","rollback_count")],
 "T2":[("Throughput","throughput_txn_s"),("P50","p50_s"),("P99","p99_s"),("Rollbacks","rollback_count")],
 "T3":[("Throughput","throughput_txn_s"),("P50","p50_s"),("P99","p99_s"),("Rollbacks","rollback_count")],
 "T4":[("Throughput","throughput_txn_s"),("P50","p50_s"),("P99","p99_s"),("Rollbacks","rollback_count")],
 "H0":[("Short TP throughput","short_throughput_txn_s"),("Short TP P99","short_p99_s"),("Synchronization stall","sync_stall_avg_s"),("Synchronization stall ratio","sync_stall_ratio")],
 "H1":[("Short TP throughput","short_throughput_txn_s"),("Short TP P99","short_p99_s"),("Long throughput","long_throughput_txn_s"),("Long P99","long_p99_s")],
 "H2":[("Short TP throughput","short_throughput_txn_s"),("Short TP P99","short_p99_s"),("Long throughput","long_throughput_txn_s"),("Long P99","long_p99_s")],
 "H3":[("Short TP throughput","short_throughput_txn_s"),("Short TP P99","short_p99_s"),("Long throughput","long_throughput_txn_s"),("Long P99","long_p99_s")],
 "H4":[("Short TP throughput","short_throughput_txn_s"),("Short TP P99","short_p99_s"),("Long throughput","long_throughput_txn_s"),("Long P99","long_p99_s")],
 "A1":[("YCSB throughput","throughput_txn_s","YCSB"),("YCSB P99","p99_s","YCSB"),("BoMB short throughput","short_throughput_txn_s","BOMB"),("BoMB short P99","short_p99_s","BOMB")],
 "A2":[("YCSB throughput","throughput_txn_s","YCSB"),("YCSB P99","p99_s","YCSB"),("BoMB short throughput","short_throughput_txn_s","BOMB"),("BoMB short P99","short_p99_s","BOMB")],
 "A3":[("Short TP throughput","short_throughput_txn_s"),("Short TP P99","short_p99_s"),("Long throughput","long_throughput_txn_s"),("Long P99","long_p99_s"),("Version chain","version_chain_avg"),("Read intents","read_intents_peak")],
 "A4":[("YCSB throughput","throughput_txn_s","YCSB"),("YCSB watermark wait","watermark_wait_avg_ns","YCSB"),("BoMB short throughput","short_throughput_txn_s","BOMB"),("BoMB watermark wait","watermark_wait_avg_ns","BOMB")],
 "S1":[("YCSB throughput","throughput_txn_s","YCSB"),("YCSB P99","p99_s","YCSB"),("BoMB short throughput","short_throughput_txn_s","BOMB"),("BoMB short P99","short_p99_s","BOMB"),("BoMB long throughput","long_throughput_txn_s","BOMB"),("BoMB long P99","long_p99_s","BOMB")],
}


def svg_figure(path, group, rows):
    specs=PLOT_SPECS[group]; cols=2; panel_w=650; panel_h=360; margin=70
    total_w=panel_w*cols; total_h=panel_h*math.ceil(len(specs)/cols)+50
    colors=["#1f77b4","#d95f02","#2ca02c","#9467bd","#17becf","#8c564b"]
    out=['<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">'%(total_w,total_h,total_w,total_h),'<rect width="100%" height="100%" fill="white"/>','<style>text{font-family:Arial,sans-serif;fill:#111}.title{font-size:22px;font-weight:bold}.axis{font-size:15px}.legend{font-size:14px}.missing{font-size:28px;fill:#b91c1c}</style>']
    for idx,spec in enumerate(specs):
        title,metric=spec[:2]; workload=spec[2] if len(spec)>2 else None
        x0=(idx%cols)*panel_w+margin; y0=(idx//cols)*panel_h+45; pw=panel_w-2*margin; ph=panel_h-2*margin
        selected=[r for r in rows if r["group"]==group and (workload is None or r["workload"]==workload) and r.get(metric) is not None]
        out.append('<text x="%d" y="%d" class="title">%s</text>'%(x0,y0-14,escape(title)))
        out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#333"/><line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#333"/>'%(x0,y0+ph,x0+pw,y0+ph,x0,y0,x0,y0+ph))
        if not selected:
            out.append('<text x="%d" y="%d" class="missing">MISSING</text>'%(x0+pw//3,y0+ph//2)); continue
        series=defaultdict(lambda:defaultdict(list)); xvalues=[]
        for r in selected:
            key=r["variant"] if group in ("A2","A3","A4") else (r["workload"] if group=="H0" else r["protocol"])
            xv=r["x_value"]; series[key][str(xv)].append(float(r[metric])); xvalues.append(xv)
        numeric=all(isinstance(x,(int,float)) for x in xvalues)
        xs=sorted(set(xvalues),key=float) if numeric else sorted(set(str(x) for x in xvalues))
        lookup={str(x):i for i,x in enumerate(xs)}; ymax=max(statistics.fmean(v) for s in series.values() for v in s.values()) or 1
        for tick in range(5):
            val=ymax*tick/4; yy=y0+ph-(ph*tick/4); out.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#ddd"/><text x="%d" y="%.1f" text-anchor="end" class="axis">%.3g</text>'%(x0,yy,x0+pw,yy,x0-8,yy+5,val))
        for i,x in enumerate(xs):
            xx=x0+(pw*(i/(len(xs)-1) if len(xs)>1 else .5)); out.append('<text x="%.1f" y="%d" text-anchor="middle" class="axis">%s</text>'%(xx,y0+ph+24,escape(str(x))))
        for si,(name,values) in enumerate(sorted(series.items())):
            points=[]
            for x in xs:
                vals=values.get(str(x));
                if not vals: continue
                i=lookup[str(x)]; xx=x0+(pw*(i/(len(xs)-1) if len(xs)>1 else .5)); yy=y0+ph-ph*(statistics.fmean(vals)/ymax); points.append((xx,yy))
            color=colors[si%len(colors)]
            if len(points)>1: out.append('<polyline fill="none" stroke="%s" stroke-width="3" points="%s"/>'%(color," ".join('%.1f,%.1f'%p for p in points)))
            for xx,yy in points: out.append('<circle cx="%.1f" cy="%.1f" r="5" fill="%s"/>'%(xx,yy,color))
            out.append('<rect x="%d" y="%d" width="18" height="4" fill="%s"/><text x="%d" y="%d" class="legend">%s</text>'%(x0+pw-150,y0+15+si*20,color,x0+pw-125,y0+20+si*20,escape(name)))
    out.append('</svg>'); path.write_text("".join(out),encoding="utf-8")


def svg_a3_timeseries(path, series, rows):
    run_info = {row["run_id"]: row for row in rows if row["group"] == "A3"}
    points = [point for point in series
              if point.get("group") == "A3" and point.get("run_id") in run_info]
    metrics = (("Version chain over time", "sdmvcc_avg_version_chain"),
               ("Read intents over time", "sdmvcc_active_intents"))
    if not points or not any(metric in point for _, metric in metrics for point in points):
        return False
    width, height, panel_w = 1300, 430, 650
    out = ['<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d">' % (width, height),
           '<rect width="100%" height="100%" fill="white"/>',
           '<style>text{font-family:Arial,sans-serif;fill:#111}.title{font-size:22px;font-weight:bold}.axis{font-size:14px}.legend{font-size:12px}</style>']
    colors = ["#1f77b4", "#d95f02", "#2ca02c", "#9467bd", "#17becf",
              "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#393b79"]
    for panel, (title, metric) in enumerate(metrics):
        x0, y0, pw, ph = panel * panel_w + 70, 55, 500, 285
        grouped = defaultdict(lambda: defaultdict(list))
        for point in points:
            if metric not in point or "elapsed_ns" not in point:
                continue
            info = run_info[point["run_id"]]
            name = "%s / %s" % (info["variant"], info["x_value"])
            second = round(point["elapsed_ns"] / 1e9)
            grouped[name][second].append(float(point[metric]))
        out.append('<text x="%d" y="30" class="title">%s</text>' % (x0, escape(title)))
        out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#333"/><line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#333"/>' % (x0, y0+ph, x0+pw, y0+ph, x0, y0, x0, y0+ph))
        values = [statistics.fmean(v) for by_time in grouped.values() for v in by_time.values()]
        times = [t for by_time in grouped.values() for t in by_time]
        ymax, xmax = (max(values) if values else 1), (max(times) if times else 1)
        for index, (name, by_time) in enumerate(sorted(grouped.items())):
            coords = []
            for second, vals in sorted(by_time.items()):
                xx = x0 + pw * second / (xmax or 1)
                yy = y0 + ph - ph * statistics.fmean(vals) / (ymax or 1)
                coords.append((xx, yy))
            color = colors[index % len(colors)]
            if len(coords) > 1:
                out.append('<polyline fill="none" stroke="%s" stroke-width="2" points="%s"/>' % (color, " ".join('%.1f,%.1f' % p for p in coords)))
            out.append('<text x="%d" y="%d" class="legend" fill="%s">%s</text>' % (x0+pw+8, y0+12+index*14, color, escape(name)))
        out.append('<text x="%d" y="%d" class="axis">Time (s)</text>' % (x0+pw//2-25, y0+ph+35))
    out.append('</svg>')
    path.write_text("".join(out), encoding="utf-8")
    return True


def table_rows(rows, columns):
    return [columns] + [[display(row.get(col)) for col in columns] for row in rows]


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--results", action="append", default=[], help="SSS results root; repeatable")
    parser.add_argument("--caracal-results", action="append", default=[], help="Caracal results root; repeatable")
    parser.add_argument("--output", default="paper_results")
    args=parser.parse_args()
    sss=[Path(p) for p in args.results] or [Path("results")]
    caracal=[Path(p) for p in args.caracal_results]
    sibling=Path("../Caracal/results")
    if not caracal and sibling.exists(): caracal=[sibling]
    roots=[("SSS",p) for p in sss]+[("Caracal",p) for p in caracal]
    rows,nodes,series=discover(roots); rows.sort(key=lambda r:(r["group"],r["experiment"],r["round"],r["protocol"],str(r["x_value"])))
    output=Path(args.output); output.mkdir(parents=True,exist_ok=True); (output/"figures").mkdir(exist_ok=True); (output/"tables").mkdir(exist_ok=True)
    write_csv(output/"all_runs.csv",rows,CORE_COLUMNS)
    write_csv(output/"all_nodes.csv",nodes,["run_id","node","source","metrics_json"])
    series_cols=sorted({k for r in series for k in r}) if series else ["run_id","node","group","protocol","elapsed_ns"]
    write_csv(output/"time_series.csv",series,series_cols)
    missing=missing_rows(rows); missing_cols=["kind","group","workload","protocol","x_value","variant","metric","run_id"]
    write_csv(output/"missing.csv",missing,missing_cols)
    for group in PLOT_SPECS:
        group_rows=[r for r in rows if r["group"]==group]
        write_csv(output/"tables"/(group+".csv"),group_rows,CORE_COLUMNS)
        svg_figure(output/"figures"/(group+".svg"),group,group_rows)
    svg_a3_timeseries(output/"figures"/"A3_timeseries.svg", series, rows)
    summary=[]
    for group in PLOT_SPECS:
        summary.append({"group":group,"run_rows":sum(r["group"]==group for r in rows),"missing_configs":sum(m["group"]==group and m["kind"]=="CONFIG" for m in missing),"missing_metrics":sum(m["group"]==group and m["kind"]=="METRIC" for m in missing)})
    sheets=[("Summary",table_rows(summary,["group","run_rows","missing_configs","missing_metrics"])),("All runs",table_rows(rows,XLSX_COLUMNS))]
    for name,groups in (("TP",("T1","T2","T3","T4")),("HTAP",("H0","H1","H2","H3","H4")),("Ablation",("A1","A2","A3","A4")),("Scaling",("S1",))): sheets.append((name,table_rows([r for r in rows if r["group"] in groups],XLSX_COLUMNS)))
    sheets.extend([("Missing",table_rows(missing,missing_cols)),("Metric dictionary",[["metric","unit","aggregation","notes"]]+[list(r) for r in METRIC_DICTIONARY])])
    if series: sheets.append(("Time series",table_rows(series,series_cols)))
    write_xlsx(output/"paper_results.xlsx",sheets)
    manifest={"roots":[[repo,str(path)] for repo,path in roots],"run_rows":len(rows),"node_rows":len(nodes),"missing_rows":len(missing),"p99_min_samples":MIN_P99_SAMPLES}
    (output/"manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
    print(json.dumps(manifest,indent=2))


if __name__ == "__main__": main()

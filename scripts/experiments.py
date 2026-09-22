"""Paper experiment configurations.

Every public entry corresponds to one experiment group in PAPER_EXPERIMENTS.md.
Temporary smoke tests and implementation probes deliberately live outside this
registry so paper runs cannot accidentally select them.
"""

import itertools


PAPER_ALGOS = ("CALVIN", "ARIA", "SDMVCC")
WARMUP = "30*BILLION"
MEASURE = "30*BILLION"
BASE_NODES = 2
BASE_TABLE_PER_NODE = 8 * 1024 * 1024
BASE_THD_CNT = 15  # THD_CNT + 1 is the fixed scheduler + executor budget (16)


def _workers(algo):
    return 16 if algo == "ARIA" else 15


def _rows(fmt, records):
    return fmt, [[record[key] for key in fmt] for record in records]


def _ycsb(algo="SDMVCC", nodes=BASE_NODES):
    return {
        "WORKLOAD": "YCSB", "CC_ALG": algo,
        "NODE_CNT": nodes, "CLIENT_NODE_CNT": nodes,
        "THREAD_CNT": _workers(algo), "CLIENT_THREAD_CNT": 4,
        "SCHEDULER_CNT": 3, "MAX_TXN_IN_FLIGHT": 10000,
        "SYNTH_TABLE_SIZE": BASE_TABLE_PER_NODE,
        #  * nodes,
        "REQ_PER_QUERY": 10, "REQ_PER_SHORT_QUERY": 10,
        "MAX_ROW_PER_TXN": 2048,
        "ZIPF_THETA": 0.7, "TUP_WRITE_PERC": 0.2,
        "TXN_WRITE_PERC": 1.0, "MPR": 0.2,
        "ARIA_BATCH_SIZE": 3000, "LONG_TXN_WORKLOAD": "false",
        "LONG_QUERY_PERC": 0.0, "MSG_SIZE_MAX": 4096,
        "OPEN_DISTRIBUTED_WATERMARK": "false",
        "SDMVCC_LAZY_READ_INTENT": "false",
        "SDMVCC_INTENT_GC": "true",
        "SDMVCC_LONG_READ_GUARD": "false",
        "WARMUP_TIMER": WARMUP, "DONE_TIMER": MEASURE,
    }


def _tpcc(algo="SDMVCC", nodes=BASE_NODES):
    return {
        "WORKLOAD": "TPCC", "CC_ALG": algo,
        "NODE_CNT": nodes, "CLIENT_NODE_CNT": nodes,
        "THREAD_CNT": _workers(algo), "CLIENT_THREAD_CNT": 4,
        "SCHEDULER_CNT": 3, "MAX_TXN_IN_FLIGHT": 10000,
        "NUM_WH": 32,
        #  * nodes, 
        "PERC_PAYMENT": 0.489,
        "PRORATE_RATIO": 0, "MPR": 0.15, "MPR_NEWORDER": 0.10,
        "ARIA_BATCH_SIZE": 3000,
        "OPEN_DISTRIBUTED_WATERMARK": "false",
        "SDMVCC_LAZY_READ_INTENT": "false",
        "SDMVCC_INTENT_GC": "true",
        "WARMUP_TIMER": WARMUP, "DONE_TIMER": MEASURE,
    }


def _bomb(algo="SDMVCC", nodes=BASE_NODES):
    return {
        "WORKLOAD": "BOMB", "CC_ALG": algo,
        "NODE_CNT": nodes, "CLIENT_NODE_CNT": nodes,
        "THREAD_CNT": _workers(algo), "CLIENT_THREAD_CNT": 4,
        "SCHEDULER_CNT": 3, "MAX_TXN_IN_FLIGHT": 10000,
        "ARIA_BATCH_SIZE": 3000, "BOMB_DYNAMIC_MODE": "false",
        "BOMB_L1_PERIODIC_MIX": "true", "BOMB_L1_MIX_PERIOD": 2048,
        "BOMB_L1_RANDOM_MIX": "false", "BOMB_L1_RANDOM_PCT": 0.1,
        "BOMB_LONG_TX_MODE": "BOMB_LONG_TX_PER_CLIENT",
        "BOMB_LONG_TX_SOURCES": 1, "BOMB_SHORT_WORKERS": 3,
        "BOMB_FACTORY_COUNT": 8, "BOMB_PRODUCT_TYPES": 72000,
        "BOMB_MATERIAL_TYPES": 198000, "BOMB_RAW_MATERIAL_TYPES": 75000,
        "BOMB_TREES_PER_PRODUCT": 5, "BOMB_TREE_SIZE": 10,
        "BOMB_RAW_MATERIALS_PER_LEAF": 3,
        "BOMB_TARGET_PRODUCTS": 100, "BOMB_TARGET_MATERIALS": 1,
        "BOMB_QUERY_CACHE_SIZE": 2048, "BOMB_FORCE_SHORT_TYPE": -1,
        "BOMB_INJECT_STALE_PRESET": "false", "MSG_SIZE_MAX": 4194304,
        "OPEN_DISTRIBUTED_WATERMARK": "false",
        "SDPCC_LONG_HOLE_MODE": "SDPCC_LONG_HOLE_DISABLED",
        "SDMVCC_LAZY_READ_INTENT": "false", "SDMVCC_INTENT_GC": "true",
        "SDMVCC_LONG_READ_GUARD": "false",
        "SDMVCC_UNSAFE_L1_NO_INTENT": "false",
        "BOMB_L1_ACQUIRE_ONLY_WRITES": "false",
        "WARMUP_TIMER": WARMUP, "DONE_TIMER": MEASURE,
    }


YCSB_FMT = list(_ycsb().keys())
TPCC_FMT = list(_tpcc().keys())
BOMB_FMT = list(_bomb().keys())


def paper_t1_ycsb_skew():
    records = []
    for algo, skew in itertools.product(PAPER_ALGOS,
                                         (0.1, 0.3, 0.5, 0.7, 0.9, 1.1, 1.3, 1.5)):
        row = _ycsb(algo); row["ZIPF_THETA"] = skew; records.append(row)
    return _rows(YCSB_FMT, records)


def paper_t2_ycsb_write():
    records = []
    for algo, ratio in itertools.product(PAPER_ALGOS,
                                          (0.0, 0.2, 0.4, 0.6, 0.8, 1.0)):
        row = _ycsb(algo); row["TUP_WRITE_PERC"] = ratio; records.append(row)
    return _rows(YCSB_FMT, records)


def paper_t3_ycsb_dist():
    records = []
    for algo, ratio in itertools.product(PAPER_ALGOS,
                                          (0.1, 0.2, 0.3, 0.4, 0.5,
                                           0.6, 0.7, 0.8, 0.9, 1.0)):
        row = _ycsb(algo); row["MPR"] = ratio; records.append(row)
    return _rows(YCSB_FMT, records)


def paper_t4_tpcc_warehouses():
    records = []
    for algo, per_node in itertools.product(PAPER_ALGOS, (8, 16, 32, 64, 128)):
        row = _tpcc(algo); row["NUM_WH"] = per_node * row["NODE_CNT"]
        records.append(row)
    return _rows(TPCC_FMT, records)


def paper_h0_motivation():
    # Pure short YCSB and the fixed mixed BoMB point used by H3/H4.
    fmt = list(dict.fromkeys(YCSB_FMT + BOMB_FMT))
    records = []
    for algo in PAPER_ALGOS:
        ycsb = _ycsb(algo)
        ycsb["ZIPF_THETA"] = 0.0
        records.extend((ycsb, _bomb(algo)))
    # Fill keys irrelevant to one workload from repository defaults.
    defaults = _ycsb(); defaults.update(_bomb())
    return fmt, [[record.get(key, defaults.get(key, configs.get(key))) for key in fmt]
                 for record in records]


def paper_h1_ycsb_long_ratio():
    records = []
    for algo, ratio in itertools.product(PAPER_ALGOS, (0.01, 0.05, 0.10, 0.20)):
        row = _ycsb(algo); row.update({
            "LONG_TXN_WORKLOAD": "true", "LONG_QUERY_PERC": ratio,
            "REQ_PER_QUERY": 1000, "REQ_PER_SHORT_QUERY": 10,
            "MAX_ROW_PER_TXN": 2048, "MSG_SIZE_MAX": 1048576})
        records.append(row)
    return _rows(YCSB_FMT, records)


def paper_h2_ycsb_long_size():
    records = []
    for algo, size in itertools.product(PAPER_ALGOS, (100, 500, 1000, 5000)):
        row = _ycsb(algo); row.update({
            "LONG_TXN_WORKLOAD": "true", "LONG_QUERY_PERC": 0.05,
            "REQ_PER_QUERY": size, "REQ_PER_SHORT_QUERY": 10,
            "MAX_ROW_PER_TXN": 8192, "MSG_SIZE_MAX": 4194304})
        records.append(row)
    return _rows(YCSB_FMT, records)

def paper_h3_bomb_long_ratio():
    records = []
    for algo, dynamic, pct in itertools.product(PAPER_ALGOS, ("false", "true"), (0.1, 0.5, 1.0, 5.0, 10.0)):
        row = _bomb(algo)
        row.update({
            "BOMB_DYNAMIC_MODE": dynamic,
            "BOMB_L1_PERIODIC_MIX": "false",
            "BOMB_L1_RANDOM_MIX": "true",
            "BOMB_L1_RANDOM_PCT": pct
        })
        records.append(row)
    return _rows(BOMB_FMT, records)


def paper_h4_bomb_long_size():
    records = []
    for algo, dynamic, products in itertools.product(PAPER_ALGOS, ("false", "true"), (50, 75, 100, 150, 200)):
        row = _bomb(algo)
        row.update({
            "BOMB_DYNAMIC_MODE": dynamic,
            "BOMB_L1_PERIODIC_MIX": "false",
            "BOMB_L1_RANDOM_MIX": "false",
            "BOMB_L1_RANDOM_PCT": 0.5,
            "BOMB_TARGET_PRODUCTS": products
        })
        records.append(row)

    return _rows(BOMB_FMT, records)


# def paper_h4_bomb_long_size():
#     records = []
#     for algo, products in itertools.product(PAPER_ALGOS, (10, 25, 50, 100, 200)):
#         row = _bomb(algo); row.update({
#             "BOMB_L1_PERIODIC_MIX": "false", "BOMB_L1_RANDOM_MIX": "false",
#             "BOMB_L1_RANDOM_PCT": 0.5, "BOMB_TARGET_PRODUCTS": products})
#         records.append(row)
#     return _rows(BOMB_FMT, records)


def paper_a1_scheduler_ycsb():
    records = []
    for schedulers in range(1, 16):
        row = _ycsb("SDMVCC")
        row["SCHEDULER_CNT"] = schedulers
        row["THREAD_CNT"] = BASE_THD_CNT
        records.append(row)
    return _rows(YCSB_FMT, records)


def paper_a1_scheduler_bomb():
    records = []
    for schedulers in range(1, 16):
        row = _bomb("SDMVCC")
        row["SCHEDULER_CNT"] = schedulers
        row["THREAD_CNT"] = BASE_THD_CNT
        records.append(row)
    return _rows(BOMB_FMT, records)


def paper_a2_read_intent_ycsb():
    records = []
    for lazy in ("false", "true"):
        row = _ycsb(); row["SDMVCC_LAZY_READ_INTENT"] = lazy; records.append(row)
    return _rows(YCSB_FMT, records)


def paper_a2_read_intent_bomb():
    records = []
    for lazy in ("false", "true"):
        row = _bomb(); row["SDMVCC_LAZY_READ_INTENT"] = lazy; records.append(row)
    return _rows(BOMB_FMT, records)


def paper_a3_gc():
    # false is an explicit no-reclamation baseline, not a conventional GC.
    records = []
    for gc, products in itertools.product(("false", "true"),
                                           (50, 75, 100, 150, 200)):
        row = _bomb(); row.update({
            "BOMB_L1_PERIODIC_MIX": "false", "BOMB_L1_RANDOM_MIX": "true",
            "BOMB_L1_RANDOM_PCT": 0.5, "BOMB_TARGET_PRODUCTS": products,
            "SDMVCC_INTENT_GC": gc})
        records.append(row)
    return _rows(BOMB_FMT, records)


def paper_a4_coalescing_ycsb():
    records = []
    for distributed in ("false", "true"):
        row = _ycsb(); row["OPEN_DISTRIBUTED_WATERMARK"] = distributed
        records.append(row)
    return _rows(YCSB_FMT, records)


def paper_a4_coalescing_bomb():
    records = []
    for distributed in ("false", "true"):
        row = _bomb(); row["OPEN_DISTRIBUTED_WATERMARK"] = distributed
        records.append(row)
    return _rows(BOMB_FMT, records)


def paper_s1_scaling_ycsb():
    records = []
    for nodes in (2, 4, 6, 8): records.append(_ycsb(nodes=nodes))
    return _rows(YCSB_FMT, records)


def paper_s1_scaling_bomb():
    records = []
    for nodes in (2, 4, 6, 8): records.append(_bomb(nodes=nodes))
    return _rows(BOMB_FMT, records)


experiment_map = {
    "paper_t1_ycsb_skew": paper_t1_ycsb_skew,
    "paper_t2_ycsb_write": paper_t2_ycsb_write,
    "paper_t3_ycsb_dist": paper_t3_ycsb_dist,
    "paper_t4_tpcc_warehouses": paper_t4_tpcc_warehouses,
    "paper_h0_motivation": paper_h0_motivation,
    "paper_h1_ycsb_long_ratio": paper_h1_ycsb_long_ratio,
    "paper_h2_ycsb_long_size": paper_h2_ycsb_long_size,
    "paper_h3_bomb_long_ratio": paper_h3_bomb_long_ratio,
    "paper_h4_bomb_long_size": paper_h4_bomb_long_size,
    "paper_a1_scheduler_ycsb": paper_a1_scheduler_ycsb,
    "paper_a1_scheduler_bomb": paper_a1_scheduler_bomb,
    "paper_a2_read_intent_ycsb": paper_a2_read_intent_ycsb,
    "paper_a2_read_intent_bomb": paper_a2_read_intent_bomb,
    "paper_a3_gc": paper_a3_gc,
    "paper_a4_coalescing_ycsb": paper_a4_coalescing_ycsb,
    "paper_a4_coalescing_bomb": paper_a4_coalescing_bomb,
    "paper_s1_scaling_ycsb": paper_s1_scaling_ycsb,
    "paper_s1_scaling_bomb": paper_s1_scaling_bomb,
}


SHORTNAMES = {'ABORT_PENALTY': 'PENALTY',
 'ACCESS_PERC': 'A',
 'BOMB_L1_MIX_PERIOD': 'BLP',
 'BOMB_L1_RANDOM_MIX': 'BLR',
 'BOMB_L1_RANDOM_PCT': 'BLRP',
 'BOMB_LONG_TX_MODE': 'BLM',
 'BOMB_LONG_TX_SOURCES': 'BLS',
 'BOMB_SHORT_WORKERS': 'BSW',
 'BOMB_TARGET_PRODUCTS': 'BTP',
 'CC_ALG': '',
 'CH_OLAP_PERC': 'OLAP',
 'CH_QUERY_MAX': 'QMAX',
 'CH_QUERY_MIN': 'QMIN',
 'CH_QUERY_WAREHOUSE_PCT': 'QWH',
 'CH_SUPPLIER_COUNT': 'SUPP',
 'CLIENT_NODE_CNT': 'CN',
 'CLIENT_REM_THREAD_CNT': 'CRT',
 'CLIENT_SEND_THREAD_CNT': 'CST',
 'CLIENT_THREAD_CNT': 'CT',
 'CUST_PER_DIST_NORM': 'CUST',
 'DATA_PERC': 'D',
 'ISOLATION_LEVEL': 'LVL',
 'LONG_QUERY_PERC': 'LP',
 'MAX_ITEMS_NORM': 'ITEMS',
 'MAX_ROW_PER_TXN': 'MRT',
 'MAX_TXN_IN_FLIGHT': 'TIF',
 'MAX_TXN_PER_PART': 'TXNS',
 'MODE': '',
 'MPR': 'MPR',
 'MSG_SIZE_MAX': 'BS',
 'MSG_TIME_LIMIT': 'BT',
 'NETWORK_DELAY': 'NDLY',
 'NETWORK_DELAY_TEST': 'NDT',
 'NODE_CNT': 'N',
 'NUM_WH': 'WH',
 'OPEN_DISTRIBUTED_WATERMARK': 'DW',
 'PART_PER_TXN': 'PPT',
 'PERC_PAYMENT': 'PP',
 'PRIORITY': '',
 'PRORATE_RATIO': 'PRORATE',
 'REM_THREAD_CNT': 'RT',
 'REPLICA_CNT': 'RN',
 'REQ_PER_QUERY': 'RPQ',
 'REQ_PER_SHORT_QUERY': 'SRPQ',
 'RWSET_KNOWN_RATIO': 'RWSET',
 'SCHEDULER_CNT': 'SC',
 'SDMVCC_EARLY_VERSION_PUBLISH': 'EVP',
 'SDMVCC_INTENT_GC': 'IGC',
 'SDMVCC_LAZY_READ_INTENT': 'LRI',
 'SDMVCC_LONG_READ_GUARD': 'LRG',
 'SDPCC_LONG_HOLE_MODE': 'HOLE',
 'SEND_THREAD_CNT': 'ST',
 'STRICT_PPT': 'SPPT',
 'SYNTH_TABLE_SIZE': 'TBL',
 'THREAD_CNT': 'T',
 'TUP_READ_PERC': 'TRD',
 'TUP_WRITE_PERC': 'TWR',
 'TXN_READ_PERC': 'RD',
 'TXN_WRITE_PERC': 'WR',
 'WORKLOAD': '',
 'YCSB_ABORT_MODE': 'ABRTMODE',
 'ZIPF_THETA': 'SKEW'}

configs = {'ABORT_PENALTY': '10 * 1000000UL   // in ns.',
 'ABORT_PENALTY_MAX': '5 * 100 * 1000000UL   // in ns.',
 'ACCESS_PERC': 0.03,
 'ARIA_BATCH_SIZE': 3000,
 'BATCH_TIMER': '0',
 'BOMB_DYNAMIC_MODE': 'false',
 'BOMB_FACTORY_COUNT': 8,
 'BOMB_L1_PERIODIC_MIX': 'false',
 'BOMB_L1_RANDOM_MIX': 'false',
 'BOMB_L1_RANDOM_PCT': 10,
 'BOMB_LONG_TX_MODE': 'BOMB_LONG_TX_PER_CLIENT',
 'BOMB_LONG_TX_SOURCES': 1,
 'BOMB_MATERIAL_TYPES': 198000,
 'BOMB_PRODUCT_TYPES': 72000,
 'BOMB_RAW_MATERIALS_PER_LEAF': 3,
 'BOMB_RAW_MATERIAL_TYPES': 75000,
 'BOMB_SHORT_WORKERS': 4,
 'BOMB_TARGET_MATERIALS': 1,
 'BOMB_TARGET_PRODUCTS': 100,
 'BOMB_TREES_PER_PRODUCT': 5,
 'BOMB_TREE_SIZE': 10,
 'CC_ALG': 'CNULL',
 'CH_OLAP_PERC': 0.1,
 'CH_QUERY_MAX': 22,
 'CH_QUERY_MIN': 1,
 'CH_QUERY_WAREHOUSE_PCT': 100,
 'CH_SUPPLIER_COUNT': 10000,
 'CLIENT_NODE_CNT': 'NODE_CNT',
 'CLIENT_REM_THREAD_CNT': 2,
 'CLIENT_SEND_THREAD_CNT': 2,
 'CLIENT_THREAD_CNT': 4,
 'CUST_PER_DIST_NORM': 3000,
 'DATA_PERC': 100,
 'DONE_TIMER': '1 * 20 * BILLION // ~1 minutes',
 'ENVIRONMENT_EC2': 'false',
 'INIT_PARALLELISM': 8,
 'ISOLATION_LEVEL': 'SERIALIZABLE',
 'LOAD_METHOD': 'LOAD_MAX',
 'LOGGING': 'false',
 'LONG_QUERY_PERC': 0.0,
 'LONG_TXN_WORKLOAD': 'false',
 'MAX_ITEMS_NORM': 100000,
 'MAX_TXN_IN_FLIGHT': 10000,
 'MAX_TXN_PER_PART': 500000,
 'MODE': 'NORMAL_MODE',
 'MPR': 0.2,
 'MPR_NEWORDER': 'MPR',
 'MSG_SIZE_MAX': 4096,
 'MSG_TIME_LIMIT': '0',
 'NETWORK_DELAY': '0UL',
 'NETWORK_DELAY_TEST': 'false',
 'NETWORK_TEST': 'false',
 'NODE_CNT': 2,
 'NUM_WH': 32,
 'OPEN_DISTRIBUTED_WATERMARK': 'false',
 'OPEN_RANDOM_WAIT': 'false',
 'PART_CNT': 'NODE_CNT',
 'PART_PER_TXN': 2,
 'PERC_PAYMENT': 0.489,
 'PRIORITY': 'PRIORITY_ACTIVE',
 'PROG_TIMER': '10 * BILLION // in s',
 'PRORATE_RATIO': 0,
 'REM_THREAD_CNT': 2,
 'REPLICA_CNT': 0,
 'REPLICA_TYPE': 'AP',
 'REQ_PER_QUERY': 10,
 'REQ_PER_SHORT_QUERY': 10,
 'RWSET_KNOWN': 'false',
 'RWSET_KNOWN_RATIO': 1.0,
 'RWSET_VARIABLE_RATIO': 0.0,
 'SCHEDULER_CNT': 5,
 'SDMVCC_BLIND_WRITE': 'false',
 'SDMVCC_EARLY_VERSION_PUBLISH': 'false',
 'SDMVCC_LONG_READ_GUARD': 'false',
 'SDMVCC_UNSAFE_L1_NO_INTENT': 'false',
 'SDPCC_LONG_HOLE_MODE': 'SDPCC_LONG_HOLE_DISABLED',
 'SEND_THREAD_CNT': 2,
 'SEQ_BATCH_TIMER': '5 * 1 * MILLION // ~5ms -- same as CALVIN paper',
 'SERVER_GENERATE_QUERIES': 'false',
 'SET_AFFINITY': 'true',
 'SHMEM_ENV': 'false',
 'SKEW_METHOD': 'ZIPF',
 'STRICT_PPT': 0,
 'SYNTH_TABLE_SIZE': '1048576*8',
 'THREAD_CNT': 16,
 'TPORT_PORT': '18000',
 'TPORT_TYPE': 'TCP',
 'TUP_WRITE_PERC': 0.2,
 'TWOPL_LITE': 'false',
 'TXN_WRITE_PERC': 1.0,
 'WARMUP_TIMER': '1 * 20 * BILLION // ~1 minutes',
 'WORKLOAD': 'YCSB',
 'YCSB_ABORT_MODE': 'false',
 'ZIPF_THETA': 0.7}

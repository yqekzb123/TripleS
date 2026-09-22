"""Caracal configurations matching the paper comparison matrix in SSS."""

import itertools

WARMUP = "30*BILLION"
MEASURE = "30*BILLION"
BASE_NODES = 2
BASE_TABLE_PER_NODE = 8 * 1024 * 1024


def _rows(fmt, records):
    return fmt, [[record[key] for key in fmt] for record in records]


def _ycsb(nodes=BASE_NODES):
    return {"WORKLOAD": "YCSB", "CC_ALG": "CARACAL", "NODE_CNT": nodes,
            "CLIENT_NODE_CNT": nodes, "THREAD_CNT": 16, "CLIENT_THREAD_CNT": 4,
            "SCHEDULER_CNT": 3, "MAX_TXN_IN_FLIGHT": 10000,
            "SYNTH_TABLE_SIZE": BASE_TABLE_PER_NODE,
            #  * nodes,
            "REQ_PER_QUERY": 10, "REQ_PER_SHORT_QUERY": 10,
            "MAX_ROW_PER_TXN": 2048, "ZIPF_THETA": 0.7,
            "TUP_WRITE_PERC": 0.2, "TXN_WRITE_PERC": 1.0, "MPR": 0.2,
            "ARIA_BATCH_SIZE": 3000, "LONG_TXN_WORKLOAD": "false",
            "LONG_QUERY_PERC": 0.0, "MSG_SIZE_MAX": 4096,
            "WARMUP_TIMER": WARMUP, "DONE_TIMER": MEASURE}


def _tpcc(nodes=BASE_NODES):
    return {"WORKLOAD": "TPCC", "CC_ALG": "CARACAL", "NODE_CNT": nodes,
            "CLIENT_NODE_CNT": nodes, "THREAD_CNT": 16, "CLIENT_THREAD_CNT": 4,
            "SCHEDULER_CNT": 3, "MAX_TXN_IN_FLIGHT": 10000,
            "NUM_WH": 32,
            #  * nodes, 
            "PERC_PAYMENT": 0.489,
            "PRORATE_RATIO": 0, "MPR": 0.15, "MPR_NEWORDER": 0.10,
            "ARIA_BATCH_SIZE": 3000,
            "WARMUP_TIMER": WARMUP, "DONE_TIMER": MEASURE}


def _bomb(nodes=BASE_NODES):
    return {"WORKLOAD": "BOMB", "CC_ALG": "CARACAL", "NODE_CNT": nodes,
            "CLIENT_NODE_CNT": nodes, "THREAD_CNT": 16, "CLIENT_THREAD_CNT": 4,
            "SCHEDULER_CNT": 3, "MAX_TXN_IN_FLIGHT": 10000,
            "ARIA_BATCH_SIZE": 3000, "BOMB_DYNAMIC_MODE": "false",
            "BOMB_L1_PERIODIC_MIX": "true", "BOMB_L1_MIX_PERIOD": 2048,
            "BOMB_L1_RANDOM_MIX": "false", "BOMB_L1_RANDOM_PCT": 0.1,
            "BOMB_LONG_TX_MODE": "BOMB_LONG_TX_PER_CLIENT",
            "BOMB_LONG_TX_SOURCES": 1, "BOMB_SHORT_WORKERS": 3,
            "BOMB_FACTORY_COUNT": 8, "BOMB_PRODUCT_TYPES": 72000,
            "BOMB_MATERIAL_TYPES": 198000, "BOMB_RAW_MATERIAL_TYPES": 75000,
            "BOMB_TREES_PER_PRODUCT": 5, "BOMB_TREE_SIZE": 10,
            "BOMB_RAW_MATERIALS_PER_LEAF": 3, "BOMB_TARGET_PRODUCTS": 100,
            "BOMB_TARGET_MATERIALS": 1, "BOMB_QUERY_CACHE_SIZE": 2048,
            "BOMB_FORCE_SHORT_TYPE": -1, "BOMB_INJECT_STALE_PRESET": "false",
            "MSG_SIZE_MAX": 4194304,
            "WARMUP_TIMER": WARMUP, "DONE_TIMER": MEASURE}


YCSB_FMT = list(_ycsb().keys()); TPCC_FMT = list(_tpcc().keys()); BOMB_FMT = list(_bomb().keys())


def paper_t1_ycsb_skew():
    rs=[]
    for x in (0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5): r=_ycsb(); r["ZIPF_THETA"]=x; rs.append(r)
    return _rows(YCSB_FMT,rs)
def paper_t2_ycsb_write():
    rs=[]
    for x in (0.0,0.2,0.4,0.6,0.8,1.0): r=_ycsb(); r["TUP_WRITE_PERC"]=x; rs.append(r)
    return _rows(YCSB_FMT,rs)
def paper_t3_ycsb_dist():
    rs=[]
    for x in (0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0): r=_ycsb(); r["MPR"]=x; rs.append(r)
    return _rows(YCSB_FMT,rs)
def paper_t4_tpcc_warehouses():
    rs=[]
    for x in (8,16,32,64,128): r=_tpcc(); r["NUM_WH"]=x*r["NODE_CNT"]; rs.append(r)
    return _rows(TPCC_FMT,rs)
# def paper_h0_motivation():
#     fmt=list(dict.fromkeys(YCSB_FMT+BOMB_FMT)); rs=[_ycsb(),_bomb()]
#     defaults=_ycsb(); defaults.update(_bomb())
#     return fmt,[[r.get(k,defaults.get(k,configs.get(k))) for k in fmt] for r in rs]
def paper_h0_motivation():
    ycsb = _ycsb()
    ycsb["ZIPF_THETA"] = 0.0
    fmt = list(dict.fromkeys(YCSB_FMT + BOMB_FMT))
    rs = [ycsb, _bomb()]
    defaults = _ycsb()
    defaults.update(_bomb())
    return fmt, [[r.get(k, defaults.get(k, configs.get(k))) for k in fmt] for r in rs]
def paper_h1_ycsb_long_ratio():
    rs=[]
    for x in (0.01,0.05,0.10,0.20):
        r=_ycsb(); r.update({"LONG_TXN_WORKLOAD":"true","LONG_QUERY_PERC":x,"REQ_PER_QUERY":1000,"MAX_ROW_PER_TXN":2048,"MSG_SIZE_MAX":1048576}); rs.append(r)
    return _rows(YCSB_FMT,rs)
def paper_h2_ycsb_long_size():
    rs=[]
    for x in (100,500,1000,5000):
        r=_ycsb(); r.update({"LONG_TXN_WORKLOAD":"true","LONG_QUERY_PERC":0.05,"REQ_PER_QUERY":x,"MAX_ROW_PER_TXN":8192,"MSG_SIZE_MAX":4194304}); rs.append(r)
    return _rows(YCSB_FMT,rs)

def paper_h3_bomb_long_ratio():
    rs=[]
    for dynamic in ("false","true"):
        for x in (0.1,0.5,1.0,5.0,10.0):
            r=_bomb()
            r.update({
                "BOMB_DYNAMIC_MODE":dynamic,
                "BOMB_L1_PERIODIC_MIX":"false",
                "BOMB_L1_RANDOM_MIX":"true",
                "BOMB_L1_RANDOM_PCT":x
            })
            rs.append(r)
    return _rows(BOMB_FMT,rs)
def paper_h4_bomb_long_size():
    rs=[]
    for dynamic in ("false","true"):
        for x in (50, 75, 100, 150, 200):
            r=_bomb()
            r.update({
                "BOMB_DYNAMIC_MODE":dynamic,
                "BOMB_L1_PERIODIC_MIX":"false",
                "BOMB_L1_RANDOM_MIX":"false",
                "BOMB_L1_RANDOM_PCT":0.5,
                "BOMB_TARGET_PRODUCTS":x
            })
            rs.append(r)
    return _rows(BOMB_FMT,rs)
# def paper_h3_bomb_long_ratio():
#     rs=[]
#     for x in (0.1,0.5,1.0,5.0,10.0):
#         r=_bomb(); r.update({"BOMB_L1_PERIODIC_MIX":"false","BOMB_L1_RANDOM_MIX":"true","BOMB_L1_RANDOM_PCT":x}); rs.append(r)
#     return _rows(BOMB_FMT,rs)
# def paper_h4_bomb_long_size():
#     rs=[]
#     for x in (10,25,50,100,200):
#         r=_bomb(); r.update({"BOMB_L1_PERIODIC_MIX":"false","BOMB_L1_RANDOM_MIX":"true","BOMB_L1_RANDOM_PCT":0.5,"BOMB_TARGET_PRODUCTS":x}); rs.append(r)
#     return _rows(BOMB_FMT,rs)


experiment_map={
 "paper_t1_ycsb_skew":paper_t1_ycsb_skew,"paper_t2_ycsb_write":paper_t2_ycsb_write,
 "paper_t3_ycsb_dist":paper_t3_ycsb_dist,"paper_t4_tpcc_warehouses":paper_t4_tpcc_warehouses,
 "paper_h0_motivation":paper_h0_motivation,"paper_h1_ycsb_long_ratio":paper_h1_ycsb_long_ratio,
 "paper_h2_ycsb_long_size":paper_h2_ycsb_long_size,"paper_h3_bomb_long_ratio":paper_h3_bomb_long_ratio,
 "paper_h4_bomb_long_size":paper_h4_bomb_long_size}


SHORTNAMES = {'ABORT_PENALTY': 'PENALTY',
 'ACCESS_PERC': 'A',
 'BOMB_L1_MIX_PERIOD': 'BLP',
 'BOMB_L1_RANDOM_MIX': 'BLR',
 'BOMB_L1_RANDOM_PCT': 'BLRP',
 'BOMB_TARGET_PRODUCTS': 'BTP',
 'CC_ALG': '',
 'CLIENT_NODE_CNT': 'CN',
 'CLIENT_REM_THREAD_CNT': 'CRT',
 'CLIENT_SEND_THREAD_CNT': 'CST',
 'CLIENT_THREAD_CNT': 'CT',
 'DATA_PERC': 'D',
 'ISOLATION_LEVEL': 'LVL',
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
 'SDMVCC_INTENT_GC': 'IGC',
 'SDMVCC_LAZY_READ_INTENT': 'LRI',
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
 'ARIA_BATCH_SIZE': 9000,
 'BATCH_TIMER': '0',
 'BOMB_DYNAMIC_MODE': 'false',
 'BOMB_FACTORY_COUNT': 2,
 'BOMB_FORCE_SHORT_TYPE': -1,
 'BOMB_INJECT_STALE_PRESET': 'false',
 'BOMB_L1_RANDOM_MIX': 'false',
 'BOMB_L1_RANDOM_PCT': 10,
 'BOMB_LONG_TX_MODE': 'BOMB_LONG_TX_GLOBAL',
 'BOMB_LONG_TX_SOURCES': 1,
 'BOMB_MATERIAL_TYPES': 160,
 'BOMB_PRODUCT_TYPES': 64,
 'BOMB_QUERY_CACHE_SIZE': 2048,
 'BOMB_RAW_MATERIALS_PER_LEAF': 3,
 'BOMB_RAW_MATERIAL_TYPES': 64,
 'BOMB_SHORT_WORKERS': 3,
 'BOMB_TARGET_MATERIALS': 1,
 'BOMB_TARGET_PRODUCTS': 4,
 'BOMB_TREES_PER_PRODUCT': 5,
 'BOMB_TREE_SIZE': 10,
 'CC_ALG': 'CNULL',
 'CLIENT_NODE_CNT': 'NODE_CNT',
 'CLIENT_REM_THREAD_CNT': 2,
 'CLIENT_SEND_THREAD_CNT': 2,
 'CLIENT_THREAD_CNT': 4,
 'DATA_PERC': 100,
 'DONE_TIMER': '1 * 20 * BILLION // ~1 minutes',
 'ENVIRONMENT_EC2': 'false',
 'INIT_PARALLELISM': 8,
 'ISOLATION_LEVEL': 'SERIALIZABLE',
 'LOAD_METHOD': 'LOAD_MAX',
 'LOGGING': 'false',
 'LONG_QUERY_PERC': 0.0,
 'LONG_TXN_WORKLOAD': 'false',
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
 'RWSET_KNOWN': 'false',
 'RWSET_KNOWN_RATIO': 1.0,
 'RWSET_VARIABLE_RATIO': 0.0,
 'SCHEDULER_CNT': 3,
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

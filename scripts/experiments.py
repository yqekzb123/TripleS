import itertools
import os
# Experiments to run and analyze
# Go to end of file to fill in experiments
SHORTNAMES = {
    "CLIENT_NODE_CNT" : "CN",
    "CLIENT_THREAD_CNT" : "CT",
    "CLIENT_REM_THREAD_CNT" : "CRT",
    "CLIENT_SEND_THREAD_CNT" : "CST",
    "NODE_CNT" : "N",
    "THREAD_CNT" : "T",
    "SCHEDULER_CNT" : "SC",
    "REM_THREAD_CNT" : "RT",
    "SEND_THREAD_CNT" : "ST",
    "CC_ALG" : "",
    "WORKLOAD" : "",
    "MAX_TXN_PER_PART" : "TXNS",
    "MAX_TXN_IN_FLIGHT" : "TIF",
    "PART_PER_TXN" : "PPT",
    "TUP_READ_PERC" : "TRD",
    "TUP_WRITE_PERC" : "TWR",
    "TXN_READ_PERC" : "RD",
    "TXN_WRITE_PERC" : "WR",
    "ZIPF_THETA" : "SKEW",
    "MSG_TIME_LIMIT" : "BT",
    "MSG_SIZE_MAX" : "BS",
    "DATA_PERC":"D",
    "ACCESS_PERC":"A",
    "PERC_PAYMENT":"PP",
    "MPR":"MPR",
    "PRORATE_RATIO":"PRORATE",
    "REQ_PER_QUERY": "RPQ",
    "REQ_PER_SHORT_QUERY": "SRPQ",
    "LONG_QUERY_PERC": "LP",
    "SDPCC_LONG_HOLE_MODE": "HOLE",
    "MODE":"",
    "PRIORITY":"",
    "ABORT_PENALTY":"PENALTY",
    "STRICT_PPT":"SPPT",
    "NETWORK_DELAY":"NDLY",
    "NETWORK_DELAY_TEST":"NDT",
    "REPLICA_CNT":"RN",
    "SYNTH_TABLE_SIZE":"TBL",
    "RWSET_KNOWN_RATIO":"RWSET",
    "ISOLATION_LEVEL":"LVL",
    "YCSB_ABORT_MODE":"ABRTMODE",
    "NUM_WH":"WH",
    "MAX_ITEMS_NORM":"ITEMS",
    "CUST_PER_DIST_NORM":"CUST",
    "CH_OLAP_PERC":"OLAP",
    "CH_QUERY_MIN":"QMIN",
    "CH_QUERY_MAX":"QMAX",
    "CH_QUERY_WAREHOUSE_PCT":"QWH",
    "CH_SUPPLIER_COUNT":"SUPP",
    "BOMB_TARGET_PRODUCTS":"BTP",
    "BOMB_LONG_TX_MODE":"BLM",
    "BOMB_LONG_TX_SOURCES":"BLS",
    "BOMB_SHORT_WORKERS":"BSW",
    "SDMVCC_LAZY_READ_INTENT":"LRI",
    "SDMVCC_INTENT_GC":"IGC",
    "SDMVCC_LONG_READ_GUARD":"LRG",
}

fmt_title=["NODE_CNT","CC_ALG","ACCESS_PERC","TXN_WRITE_PERC","PERC_PAYMENT","MPR","MODE","MAX_TXN_IN_FLIGHT","SEND_THREAD_CNT","REM_THREAD_CNT","THREAD_CNT","SCHEDULER_CNT","TXN_WRITE_PERC","TUP_WRITE_PERC","ZIPF_THETA","LONG_QUERY_PERC","NUM_WH"]

##############################
# PLOTS
##############################
def ycsb_scaling_PCC():
    wl = 'YCSB'
    # nnodes = [2,4,6,8,10,12]
    nnodes = [2,4]
    # algos=['CALVIN','SDPCC']
    algos=['SDPCC']
    base_table_size=1048576*8
    txn_write_perc = [1]
    tup_write_perc = [0.2]
    load = [10000]
    tcnt = [15]
    ctcnt = [4]
    scnt = [2]
    rcnt = [2]
    mpr = [0.2]
    prorate = [0]
    skew = [0.7]
    fmt = ["WORKLOAD","CC_ALG","NODE_CNT","SYNTH_TABLE_SIZE","MPR","PRORATE_RATIO","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,algo,n,base_table_size*n,mpr,prorate_rate,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,mpr,prorate_rate,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,mpr,prorate,nnodes,algos)]
    return fmt,exp

def ycsb_scaling_OCC():
    wl = 'YCSB'
    nnodes = [2,4,6,8,10,12]
    algos=['SDOCC']
    base_table_size=1048576*8
    txn_write_perc = [1]
    tup_write_perc = [0.2]
    load = [10000]
    tcnt = [16]
    ctcnt = [4]
    scnt = [2]
    rcnt = [2]
    mpr = [0.2]
    prorate = [0]
    skew = [0.7]
    fmt = ["WORKLOAD","CC_ALG","NODE_CNT","SYNTH_TABLE_SIZE","MPR","PRORATE_RATIO","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,algo,n,base_table_size*n,mpr,prorate_rate,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,mpr,prorate_rate,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,mpr,prorate,nnodes,algos)]
    return fmt,exp

def ycsb_scaling_ARIA():
    wl = 'YCSB'
    nnodes = [2,4,6,8,10,12]
    algos=['ARIA']
    base_table_size=1048576*8
    txn_write_perc = [1]
    tup_write_perc = [0.2]
    load = [10000]
    tcnt = [16]
    ctcnt = [4]
    scnt = [2]
    rcnt = [2]
    mpr = [0.2]
    prorate = [0]
    skew = [0.7]
    fmt = ["WORKLOAD","CC_ALG","NODE_CNT","SYNTH_TABLE_SIZE","MPR","PRORATE_RATIO","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT","ARIA_BATCH_SIZE"]
    exp = [[wl,algo,n,base_table_size*n,mpr,prorate_rate,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr,100] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,mpr,prorate_rate,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,mpr,prorate,nnodes,algos)]
    return fmt,exp

# for test
def ycsb_skew():
    wl = 'YCSB'
    nnodes = [2]
    # algos=['CALVIN','ARIA','SDPCC','SDOCC']
    # algos=['ARIA','SDPCC','SDOCC']
    algos=['SDOCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    load = [10000]
    # total_cnt=[15]
    total_cnt=[16]
    # scnt = [1]
    scnt = [3]
    # skew = [0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5]
    skew = [0.7]
    # skew = [0.1]
    # skew = [1.5]
    fmt = ["WORKLOAD","CC_ALG","ZIPF_THETA","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,sk,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,t_cnt,s_cnt] for t_cnt,s_cnt,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(total_cnt,scnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_skew_PCC():
    wl = 'YCSB'
    nnodes = [2]
    # algos=['CALVIN','ARIA','SDPCC','SDOCC']
    # algos=['ARIA','SDPCC','SDOCC']
    algos=['SDMVCC']
    # algos=['CALVIN','SDPCC']
    base_table_size=1048576*8
    txn_write_perc = [1]
    tup_write_perc = [0.2]
    load = [10000]
    total_cnt=[15]
    # total_cnt=[16]
    # scnt = [1]
    scnt = [3]
    # skew = [0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5]
    skew = [0.7]
    # skew = [0.1]
    # skew = [1.5]
    fmt = ["WORKLOAD","CC_ALG","ZIPF_THETA","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,sk,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,t_cnt,s_cnt] for t_cnt,s_cnt,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(total_cnt,scnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_skew_OCC():
    wl = 'YCSB'
    nnodes = [2]
    # algos=['CALVIN','ARIA','SDPCC','SDOCC']
    # algos=['ARIA','SDPCC','SDOCC']
    algos=['SDOCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    load = [10000]
    total_cnt=[16]
    # scnt = [1]
    scnt = [3]
    skew = [0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5]
    # skew = [0.3]
    # skew = [0.1]
    # skew = [1.5]
    fmt = ["WORKLOAD","CC_ALG","ZIPF_THETA","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,sk,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,t_cnt,s_cnt] for t_cnt,s_cnt,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(total_cnt,scnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_writes_PCC():
    wl = 'YCSB'
    # nnodes = [4]
    nnodes = [2]
    algos=['CALVIN','SDPCC']
    # algos=['CALVIN']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    # tup_write_perc = [0.2]
    tup_write_perc = [0.0,0.2,0.4,0.6,0.8,1.0]
    load = [10000]
    total_cnt=[15]
    scnt = [3]
    skew = [0.7]
    fmt = ["WORKLOAD","CC_ALG","TUP_WRITE_PERC","NODE_CNT","SYNTH_TABLE_SIZE","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,tup_wr_perc,n,base_table_size*n,txn_wr_perc,ld,sk,thr,s_cnt] for thr,s_cnt,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(total_cnt,scnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_writes_OCC():
    wl = 'YCSB'
    # nnodes = [4]
    nnodes = [2]
    algos=['SDOCC']
    # algos=['ARIA','SDOCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    # tup_write_perc = [0.2]
    tup_write_perc = [0.0,0.2,0.4,0.6,0.8,1.0]
    load = [10000]
    total_cnt=[16]
    scnt = [3]
    skew = [0.7]
    fmt = ["WORKLOAD","CC_ALG","TUP_WRITE_PERC","NODE_CNT","SYNTH_TABLE_SIZE","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,tup_wr_perc,n,base_table_size*n,txn_wr_perc,ld,sk,thr,s_cnt] for thr,s_cnt,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(total_cnt,scnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_random_idle_PCC():
    wl = 'YCSB'
    nnodes = [2]
    # algos=['CALVIN']
    algos=['CALVIN','SDPCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    random_wait = 'true'
    # 0.0001ms, 0.001ms, 0.01ms, 0.1ms, 1ms
    # wait_time=['100000UL']
    wait_time=['0UL','100000UL']
    # wait_time=['100UL','1000UL','10000UL','100000UL','1000000UL']
    load = [10000]
    total_cnt=[15]
    skew = [0.0]
    # skew = [0.9]
    fmt = ["WORKLOAD","CC_ALG","RANDOM_WAIT_TIME","OPEN_RANDOM_WAIT","TUP_WRITE_PERC","NODE_CNT","SYNTH_TABLE_SIZE","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,algo,wait,random_wait,tup_wr_perc,n,base_table_size*n,txn_wr_perc,ld,sk,t_cnt] for t_cnt,txn_wr_perc,tup_wr_perc,wait,ld,n,sk,algo in itertools.product(total_cnt,txn_write_perc,tup_write_perc,wait_time,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_random_idle_OCC():
    wl = 'YCSB'
    nnodes = [2]
    algos=['SDOCC']
    # algos=['ARIA','SDOCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    random_wait = 'true'
    # 0.0001ms, 0.001ms, 0.01ms, 0.1ms, 1ms
    # wait_time=['100000UL']
    wait_time=['0UL','100000UL']
    # wait_time=['100UL','1000UL','10000UL','100000UL','1000000UL']
    load = [10000]
    total_cnt=[16]
    skew = [0.0]
    # skew = [0.9]
    fmt = ["WORKLOAD","CC_ALG","RANDOM_WAIT_TIME","OPEN_RANDOM_WAIT","TUP_WRITE_PERC","NODE_CNT","SYNTH_TABLE_SIZE","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,algo,wait,random_wait,tup_wr_perc,n,base_table_size*n,txn_wr_perc,ld,sk,t_cnt] for t_cnt,txn_wr_perc,tup_wr_perc,wait,ld,n,sk,algo in itertools.product(total_cnt,txn_write_perc,tup_write_perc,wait_time,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_random_idle_ARIA():
    wl = 'YCSB'
    nnodes = [2]
    algos=['ARIA']
    # algos=['ARIA','SDOCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    random_wait = 'true'
    # 0.0001ms, 0.001ms, 0.01ms, 0.1ms, 1ms
    # wait_time=['100000UL']
    wait_time=['0UL','100000UL']
    # wait_time=['100UL','1000UL','10000UL','100000UL','1000000UL']
    load = [10000]
    total_cnt=[16]
    skew = [0.0]
    # skew = [0.9]
    fmt = ["WORKLOAD","CC_ALG","RANDOM_WAIT_TIME","OPEN_RANDOM_WAIT","TUP_WRITE_PERC","NODE_CNT","SYNTH_TABLE_SIZE","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","ARIA_BATCH_SIZE"]
    exp = [[wl,algo,wait,random_wait,tup_wr_perc,n,base_table_size*n,txn_wr_perc,ld,sk,t_cnt,3000] for t_cnt,txn_wr_perc,tup_wr_perc,wait,ld,n,sk,algo in itertools.product(total_cnt,txn_write_perc,tup_write_perc,wait_time,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_dist_ratio_PCC():
    wl = 'YCSB'
    nnodes = [2]
    algos=['CALVIN','SDPCC']
    # algos=['CALVIN']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    load = [10000]
    # mpr=[0.0]
    mpr=[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1]
    total_cnt=[15]
    scnt = [3]
    fmt = ["WORKLOAD","CC_ALG","MPR","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,mpr,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,thr,s_cnt] for thr,s_cnt,ld,tup_wr_perc,txn_wr_perc,n,mpr,algo in itertools.product(total_cnt,scnt,load,tup_write_perc,txn_write_perc,nnodes,mpr,algos)]
    return fmt,exp

def ycsb_dist_ratio_OCC():
    wl = 'YCSB'
    nnodes = [2]
    # algos=['ARIA','SDOCC']
    algos=['SDOCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    load = [10000]
    # mpr=[0.0]
    mpr=[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1]
    total_cnt=[16]
    scnt = [3]
    fmt = ["WORKLOAD","CC_ALG","MPR","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,mpr,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,thr,s_cnt] for thr,s_cnt,ld,tup_wr_perc,txn_wr_perc,n,mpr,algo in itertools.product(total_cnt,scnt,load,tup_write_perc,txn_write_perc,nnodes,mpr,algos)]
    return fmt,exp

def ycsb_rwset_ratio():
    wl = 'YCSB'
    nnodes = [2]
    algos=['SDOCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    load = [10000]
    # rwset = [1.0]
    rwset = [0.0,0.2,0.4,0.6,0.8,1.0]
    # rwset = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    total_cnt=[16]
    scnt = [3]
    fmt = ["WORKLOAD","CC_ALG","RWSET_KNOWN_RATIO","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,rwset,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,thr,s_cnt] for thr,s_cnt,ld,tup_wr_perc,txn_wr_perc,n,rwset,algo in itertools.product(total_cnt,scnt,load,tup_write_perc,txn_write_perc,nnodes,rwset,algos)]
    return fmt,exp

def ycsb_rwset_variable_ratio():
    wl = 'YCSB'
    nnodes = [2]
    algos=['SDOCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    load = [10000]
    # rwset = [1.0]
    rwset = [0.0,0.2,0.4,0.6,0.8,1.0]
    total_cnt=[16]
    scnt = [3]
    skew = [0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5]
    # skew = [0.9,1.1]
    fmt = ["WORKLOAD","CC_ALG","RWSET_VARIABLE_RATIO","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,rwset,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,s_cnt] for thr,s_cnt,ld,tup_wr_perc,txn_wr_perc,n,rwset,algo,sk in itertools.product(total_cnt,scnt,load,tup_write_perc,txn_write_perc,nnodes,rwset,algos,skew)]
    return fmt,exp

# for sdpcc
def ycsb_sch_cnt():
    wl = 'YCSB'
    nnodes = [2]
    algos=['SDPCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    load = [10000]
    # rwset = [1.0]
    # rwset = [0.0,0.2,0.4,0.6,0.8,1.0]
    # rwset = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    total_cnt=[15]
    # scnt = [15,16]
    # scnt = [9,10,11,12,13,14]
    scnt = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]
    fmt = ["WORKLOAD","CC_ALG","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,thr,s_cnt] for thr,s_cnt,ld,tup_wr_perc,txn_wr_perc,n,algo in itertools.product(total_cnt,scnt,load,tup_write_perc,txn_write_perc,nnodes,algos)]
    return fmt,exp

def ycsb_batch_size_PCC():
    wl = 'YCSB'
    algos=['CALVIN','SDPCC']
    # algos=['CALVIN']
    aria_batch_size=[50,100,500,1000,2000,3000,5000,9000]
    # aria_batch_size=[9999]
    # skew = [0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5]
    total_cnt=[15]
    skew = [0.3,0.7]
    fmt = ["WORKLOAD","ARIA_BATCH_SIZE","ZIPF_THETA","THREAD_CNT","CC_ALG"]
    exp = [[wl,bs,sk,thr,algo] for algo,sk,bs,thr in itertools.product(algos,skew,aria_batch_size,total_cnt)]
    return fmt,exp

def ycsb_batch_size_OCC():
    wl = 'YCSB'
    algos=['SDOCC']
    # algos=['ARIA']
    aria_batch_size=[50,100,500,1000,2000,3000,5000,9000]
    # aria_batch_size=[9999]
    # skew = [0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5]
    total_cnt=[16]
    skew = [0.3,0.7]
    fmt = ["WORKLOAD","ARIA_BATCH_SIZE","ZIPF_THETA","THREAD_CNT","CC_ALG"]
    exp = [[wl,bs,sk,thr,algo] for algo,sk,bs,thr in itertools.product(algos,skew,aria_batch_size,total_cnt)]
    return fmt,exp

# for SKEW
def ycsb_aria_batch():
    wl = 'YCSB'
    # algos=['CALVIN','ARIA','SDPCC','SDOCC']
    algos=['ARIA']
    # aria_batch_size=[50,100,500,1000,2000,3000,5000,9000]
    aria_batch_size=[50,100,500,1000,2000,3000]
    skew = [0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5]
    # skew = [0.3]
    fmt = ["WORKLOAD","ARIA_BATCH_SIZE","ZIPF_THETA","CC_ALG"]
    exp = [[wl,bs,sk,algo] for algo,sk,bs in itertools.product(algos,skew,aria_batch_size)]
    return fmt,exp

# for dist
def ycsb_aria_batch2():
    wl = 'YCSB'
    algos=['ARIA']
    aria_batch_size=[50,100,500,1000,2000]
    mpr=[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1]
    skew = 0.7
    fmt = ["WORKLOAD","ARIA_BATCH_SIZE","MPR","CC_ALG","ZIPF_THETA"]
    exp = [[wl,bs,m,algo,skew] for algo,m,bs in itertools.product(algos,mpr,aria_batch_size)]
    return fmt,exp

# for write
def ycsb_aria_batch3():
    wl = 'YCSB'
    algos=['ARIA']
    aria_batch_size=[50,100,500]
    tup_write_perc = [0.0,0.2,0.4,0.6,0.8,1.0]
    skew = 0.7
    fmt = ["WORKLOAD","ARIA_BATCH_SIZE","TXN_WRITE_PERC","CC_ALG","ZIPF_THETA"]
    exp = [[wl,bs,tup_w,algo,skew] for algo,tup_w,bs in itertools.product(algos,tup_write_perc,aria_batch_size)]
    return fmt,exp

def tpcc_scaling_PCC():
    wl = 'TPCC'
    nnodes = [2,4,6,8,10,12]
    # algos=['ARIA']
    # algos=['SDOCC']
    algos=['CALVIN','SDPCC']
    npercpay=[0.489]
    num_wh=[32]
    load = [10000]
    tcnt = [15]
    ctcnt = [4]
    prorate = [0]
    mpr = [0.15]
    mpr_neworder = [0.1]
    fmt = ["WORKLOAD","CC_ALG","NODE_CNT","PERC_PAYMENT","PRORATE_RATIO","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","MPR","MPR_NEWORDER"]
    exp = [[wl,algo,n,pp,prorate_rate,wh*n,tif,thr,cthr,m,mn] for thr,cthr,tif,pp,prorate_rate,n,m,mn,wh,algo in itertools.product(tcnt,ctcnt,load,npercpay,prorate,nnodes,mpr,mpr_neworder,num_wh,algos)]
    return fmt,exp

def tpcc_scaling_OCC():
    wl = 'TPCC'
    nnodes = [2,4,6,8,10,12]
    # algos=['ARIA']
    algos=['SDOCC']
    # algos=['CALVIN','ARIA','SDPCC','SDOCC']
    npercpay=[0.489]
    num_wh=[32]
    load = [10000]
    tcnt = [16]
    ctcnt = [4]
    prorate = [0]
    mpr = [0.15]
    mpr_neworder = [0.1]
    fmt = ["WORKLOAD","CC_ALG","NODE_CNT","PERC_PAYMENT","PRORATE_RATIO","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","MPR","MPR_NEWORDER"]
    exp = [[wl,algo,n,pp,prorate_rate,wh*n,tif,thr,cthr,m,mn] for thr,cthr,tif,pp,prorate_rate,n,m,mn,wh,algo in itertools.product(tcnt,ctcnt,load,npercpay,prorate,nnodes,mpr,mpr_neworder,num_wh,algos)]
    return fmt,exp

def tpcc_scaling_ARIA():
    wl = 'TPCC'
    nnodes = [2,4,6,8,10,12]
    algos=['ARIA']
    # npercpay=[0.0]
    npercpay=[0.489]
    num_wh=[32]
    load = [10000]
    tcnt = [16]
    ctcnt = [2]
    prorate = [0]
    mpr = [0.15]
    mpr_neworder = [0.1]
    fmt = ["WORKLOAD","CC_ALG","NODE_CNT","PERC_PAYMENT","PRORATE_RATIO","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","MPR","MPR_NEWORDER","ARIA_BATCH_SIZE"]
    exp = [[wl,algo,n,pp,prorate_rate,wh*n,tif,thr,cthr,m,mn,500] for thr,cthr,tif,pp,prorate_rate,n,m,mn,wh,algo in itertools.product(tcnt,ctcnt,load,npercpay,prorate,nnodes,mpr,mpr_neworder,num_wh,algos)]
    return fmt,exp

def tpcc_wh_PCC():
    wl = 'TPCC'
    nnodes = [2]
    algos=['CALVIN','SDPCC']
    # algos=['CALVIN']
    # npercpay=[0.0]
    npercpay=[0.489]
    # num_wh=[32]
    num_wh=[128,64,32,16,8]
    # num_wh=[64,32,16,8]
    # num_wh=[256,128,64,32,16,8]
    load = [10000]
    total_cnt=[15]
    scnt = [3]
    ctcnt = [4]
    prorate = [0]
    # mpr = [1.0]
    # mpr_neworder = [1.0]
    mpr = [0.15]
    mpr_neworder = [0.1]
    fmt = ["WORKLOAD","CC_ALG","NUM_WH","NODE_CNT","PERC_PAYMENT","PRORATE_RATIO","MAX_TXN_IN_FLIGHT","THREAD_CNT","SCHEDULER_CNT","CLIENT_THREAD_CNT","MPR","MPR_NEWORDER"]
    exp = [[wl,algo,wh*n,n,pp,prorate_rate,tif,thr,s_cnt,cthr,m,mn] for thr,s_cnt,cthr,tif,pp,prorate_rate,n,m,mn,wh,algo in itertools.product(total_cnt,scnt,ctcnt,load,npercpay,prorate,nnodes,mpr,mpr_neworder,num_wh,algos)]
    return fmt,exp

def tpcc_wh_OCC():
    wl = 'TPCC'
    nnodes = [2]
    algos=['SDOCC']
    # algos=['CALVIN']
    # algos=['CALVIN','ARIA','SDOCC']
    # algos=['CALVIN','ARIA','SDPCC','SDOCC']
    # npercpay=[0.0]
    npercpay=[0.489]
    # num_wh=[32]
    num_wh=[128,64,32,16,8]
    # num_wh=[64,32,16,8]
    # num_wh=[256,128,64,32,16,8]
    load = [10000]
    total_cnt=[16]
    scnt = [3]
    ctcnt = [4]
    prorate = [0]
    # mpr = [1.0]
    # mpr_neworder = [1.0]
    mpr = [0.15]
    mpr_neworder = [0.1]
    fmt = ["WORKLOAD","CC_ALG","NUM_WH","NODE_CNT","PERC_PAYMENT","PRORATE_RATIO","MAX_TXN_IN_FLIGHT","THREAD_CNT","SCHEDULER_CNT","CLIENT_THREAD_CNT","MPR","MPR_NEWORDER"]
    exp = [[wl,algo,wh*n,n,pp,prorate_rate,tif,thr,s_cnt,cthr,m,mn] for thr,s_cnt,cthr,tif,pp,prorate_rate,n,m,mn,wh,algo in itertools.product(total_cnt,scnt,ctcnt,load,npercpay,prorate,nnodes,mpr,mpr_neworder,num_wh,algos)]
    return fmt,exp

def tpcc_dist_ratio():
    wl = 'TPCC'
    algos=['CALVIN','ARIA','SDPCC','SDOCC']
    # algos=['CNULL']
    mpr=[0,0.2,0.4,0.6,0.8,1]
    nnodes = [2]
    npercpay=[0.489]
    wh = 32
    load = [10000]
    fmt = ["WORKLOAD","CC_ALG","MPR","NODE_CNT","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,algo,mpr,n,pp,wh*n,tif] for tif,pp,n,mpr,algo in itertools.product(load,npercpay,nnodes,mpr,algos)]
    return fmt,exp

# for wh
def tpcc_aria_batch():
    wl = 'TPCC'
    nnodes = 2
    algos=['ARIA']
    aria_batch_size=[50,100,500,1000]
    num_wh=[128,64,32,16,8]
    fmt = ["WORKLOAD","ARIA_BATCH_SIZE","NUM_WH","CC_ALG"]
    exp = [[wl,bs,wh*nnodes,algo] for algo,wh,bs in itertools.product(algos,num_wh,aria_batch_size)]
    return fmt,exp

# for tpcc dist
def tpcc_aria_batch2():
    wl = 'TPCC'
    algos=['ARIA']
    aria_batch_size=[16,100,500,2000]
    mpr=[0,0.2,0.4,0.6,0.8,1]
    fmt=["WORKLOAD","ARIA_BATCH_SIZE","MPR","CC_ALG"]
    exp = [[wl,bs,m,algo] for algo,m,bs in itertools.product(algos,mpr,aria_batch_size)]
    return fmt,exp

def ycsb_sdpcc_long_hole():
    """Compare pure watermark, exact sets, and Bloom filters on YCSB long txns."""
    modes = ["SDPCC_LONG_HOLE_DISABLED", "SDPCC_LONG_HOLE_BLOOM"]
    # modes = ["SDPCC_LONG_HOLE_DISABLED", "SDPCC_LONG_HOLE_EXACT", "SDPCC_LONG_HOLE_BLOOM"]
    long_percs = [0.01, 0.05, 0.10]
    fmt = ["WORKLOAD", "SDPCC_LONG_HOLE_MODE", "LONG_QUERY_PERC", "CC_ALG",
           "NODE_CNT", "LONG_TXN_WORKLOAD", "REQ_PER_QUERY", "REQ_PER_SHORT_QUERY",
           "OPEN_DISTRIBUTED_WATERMARK", "THREAD_CNT", "SCHEDULER_CNT"]
    exp = [["YCSB", mode, perc, "SDPCC", 2, "true", 50, 10, "false", 16, 3]
           for perc, mode in itertools.product(long_percs, modes)]
    return fmt, exp

def ycsb_sdpcc_long_hole_adaptive():
    """Legacy entry point: compare pure watermark and fixed Bloom only."""
    variants = [
        ("SDPCC_LONG_HOLE_DISABLED", "false"),
        ("SDPCC_LONG_HOLE_BLOOM", "false"),
    ]
    rotation = int(os.environ.get("SDPCC_ADAPTIVE_ORDER", "0")) % len(variants)
    variants = variants[rotation:] + variants[:rotation]
    long_percs = [0.01, 0.05, 0.10]
    fmt = ["WORKLOAD", "SDPCC_LONG_HOLE_MODE", "SDPCC_LONG_HOLE_ADAPTIVE",
           "LONG_QUERY_PERC", "CC_ALG", "NODE_CNT", "LONG_TXN_WORKLOAD",
           "REQ_PER_QUERY", "REQ_PER_SHORT_QUERY", "OPEN_DISTRIBUTED_WATERMARK",
           "THREAD_CNT", "SCHEDULER_CNT"]
    exp = [["YCSB", mode, adaptive, perc, "SDPCC", 2, "true", 50, 10,
            "false", 16, 3]
           for perc, (mode, adaptive) in itertools.product(long_percs, variants)]
    return fmt, exp

def ycsb_sdpcc_long_hole_size():
    """Compare pure watermark and fixed Bloom for 100/500-op long txns."""
    modes = ["SDPCC_LONG_HOLE_DISABLED", "SDPCC_LONG_HOLE_BLOOM"]
    long_percs = [0.05, 0.10]
    long_sizes = [(100, 128), (500, 512)]
    fmt = ["WORKLOAD", "SDPCC_LONG_HOLE_MODE", "LONG_QUERY_PERC", "CC_ALG",
           "NODE_CNT", "LONG_TXN_WORKLOAD", "REQ_PER_QUERY", "REQ_PER_SHORT_QUERY",
           "MAX_ROW_PER_TXN", "MSG_SIZE_MAX", "SDPCC_LONG_HOLE_ADAPTIVE",
           "SDPCC_LONG_BLOOM_BITS", "SDPCC_LONG_BLOOM_HASHES",
           "OPEN_DISTRIBUTED_WATERMARK", "THREAD_CNT", "SCHEDULER_CNT"]
    exp = [["YCSB", mode, perc, "SDPCC", 2, "true", long_size, 10,
            max_rows, 16384, "false", 8192, 4, "false", 16, 3]
           for long_size, max_rows in long_sizes
           for perc, mode in itertools.product(long_percs, modes)]
    return fmt, exp

def ycsb_sdmvcc_long():
    """Compare original SDPCC and SDMVCC with 100-op YCSB long txns."""
    algos = ["SDPCC", "SDMVCC"]
    long_percs = [0.01, 0.05, 0.10]
    fmt = ["WORKLOAD", "CC_ALG", "LONG_QUERY_PERC", "NODE_CNT",
           "LONG_TXN_WORKLOAD", "REQ_PER_QUERY", "REQ_PER_SHORT_QUERY",
           "MAX_ROW_PER_TXN", "MSG_SIZE_MAX", "OPEN_DISTRIBUTED_WATERMARK",
           "SDPCC_LONG_HOLE_MODE", "THREAD_CNT", "SCHEDULER_CNT"]
    exp = [["YCSB", algo, perc, 2, "true", 100, 10, 128, 16384, "false",
            "SDPCC_LONG_HOLE_DISABLED", 16, 3]
           for perc, algo in itertools.product(long_percs, algos)]
    return fmt, exp

def chbenchmark_sdmvcc_test():
    """Small two-node CH-benCHmark correctness/performance smoke test.

    The analytical stream cycles deterministically through Q1..Q22.  Change
    olap_percs or warehouse_pcts below to sweep the HTAP mix or query range.
    """
    olap_percs = [0.10]
    warehouse_pcts = [100]
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "NUM_WH",
           "MAX_ITEMS_NORM", "CUST_PER_DIST_NORM", "CH_SUPPLIER_COUNT",
           "CH_OLAP_PERC", "CH_QUERY_MIN", "CH_QUERY_MAX",
           "CH_QUERY_WAREHOUSE_PCT", "MAX_TXN_IN_FLIGHT",
           "MAX_TXN_PER_PART", "THREAD_CNT", "SCHEDULER_CNT",
           "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["CHBENCHMARK", "SDMVCC", 2, 2, 1000, 1000, 1000,
            olap, 1, 22, warehouse_pct, 100, 10000, 16, 3,
            "0*BILLION", "30*BILLION"]
           for olap, warehouse_pct in itertools.product(
               olap_percs, warehouse_pcts)]
    return fmt, exp

##############################
# END PLOTS
##############################

def _bomb_formal(dynamic_mode, algos):
    """Full-size two-node BoMB configuration used for paper experiments."""
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "ARIA_BATCH_SIZE", "BOMB_DYNAMIC_MODE",
           "BOMB_L1_PERIODIC_MIX", "BOMB_L1_MIX_PERIOD",
           "BOMB_LONG_TX_MODE",
           "BOMB_LONG_TX_SOURCES", "BOMB_SHORT_WORKERS",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TREES_PER_PRODUCT", "BOMB_TREE_SIZE",
           "BOMB_RAW_MATERIALS_PER_LEAF", "BOMB_TARGET_PRODUCTS",
           "BOMB_TARGET_MATERIALS", "BOMB_QUERY_CACHE_SIZE",
           "BOMB_FORCE_SHORT_TYPE", "BOMB_INJECT_STALE_PRESET",
           "MSG_SIZE_MAX", "OPEN_DISTRIBUTED_WATERMARK",
           "SDPCC_LONG_HOLE_MODE", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "SDMVCC_UNSAFE_L1_NO_INTENT", "BOMB_L1_ACQUIRE_ONLY_WRITES",
           "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", algo, 2, 2,
            16, 4, 10000,
            3000, dynamic_mode,
            "true", 256, "BOMB_LONG_TX_PER_CLIENT",
            1, 3,
            8, 72000,
            198000, 75000,
            5, 10,
            3, 100,
            1, 2048,
            -1, "false",
            4194304, "false",
            "SDPCC_LONG_HOLE_DISABLED", "false",
            "true", "false",
            "false", "false",
            "30*BILLION", "30*BILLION"]
           for algo in algos]
    return fmt, exp


def bomb_static():
    """Formal static BoMB (L1/S1/S2), not a smoke test."""
    return _bomb_formal("false", ["CALVIN", "SDMVCC"])
    # return _bomb_formal("false", ["CALVIN", "ARIA", "SDMVCC"])


def bomb_dynamic():
    """Formal dynamic BoMB (L1/S1/S2/S3/S4/S5), not a smoke test."""
    return _bomb_formal("true", ["CALVIN", "ARIA", "SDMVCC"])


def ycsb_idle_default():
    """Two-node default YCSB used for the Aria/SDMVCC idle comparison."""
    algos = ["SDPCC", "SDMVCC"]
    # algos = ["ARIA", "SDMVCC","CALVIN"]
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "SCHEDULER_CNT",
           "MAX_TXN_IN_FLIGHT", "SYNTH_TABLE_SIZE", "REQ_PER_QUERY",
           "ZIPF_THETA", "TUP_WRITE_PERC", "TXN_WRITE_PERC", "MPR",
           "ARIA_BATCH_SIZE", "OPEN_DISTRIBUTED_WATERMARK",
           "SDMVCC_LAZY_READ_INTENT", "SDMVCC_INTENT_GC",
           "SDMVCC_LONG_READ_GUARD", "SDMVCC_UNSAFE_L1_NO_INTENT",
           "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["YCSB", algo, 2, 2,
            16, 4, 3,
            10000, 8 * 1024 * 1024, 10,
            0.7, 0.2, 1.0, 0.2,
            3000, "false",
            "false", "true",
            "false", "false",
            "30*BILLION", "30*BILLION"]
           for algo in algos]
    return fmt, exp

def bomb_calvin_smoke():
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES", "BOMB_MATERIAL_TYPES",
           "BOMB_RAW_MATERIAL_TYPES", "BOMB_TARGET_PRODUCTS",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES", "BOMB_SHORT_WORKERS",
           "MSG_SIZE_MAX", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "CALVIN", 2, 2, 8, 3, 10000,
            2, 64, 160, 64, 4,
            "BOMB_LONG_TX_GLOBAL", 1, 2,
            1048576, "20*BILLION", "30*BILLION"]]
    return fmt, exp


def bomb_sdmvcc_smoke():
    """Two-node SDMVCC + BoMB static-mode smoke test.  SDMVCC shares the
    SDPCC scheduler plumbing with CALVIN, so the plan mirrors
    bomb_calvin_smoke with CC_ALG switched to SDMVCC."""
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES", "BOMB_MATERIAL_TYPES",
           "BOMB_RAW_MATERIAL_TYPES", "BOMB_TARGET_PRODUCTS",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES", "BOMB_SHORT_WORKERS",
           "MSG_SIZE_MAX", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", 2, 2, 8, 4, 10000,
            2, 64, 160, 64, 4,
            "BOMB_LONG_TX_GLOBAL", 1, 3,
            1048576, "20*BILLION", "20*BILLION"]]
    return fmt, exp

def bomb_sdmvcc_lazy_intent_ablation():
    """Paired 2-node eager-vs-execution-time read-intent comparison."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_LONG_TX_MODE",
           "BOMB_LONG_TX_SOURCES", "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", lazy, "false", 2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "BOMB_LONG_TX_GLOBAL", 1, 3, 4194304,
            "30*BILLION", "30*BILLION"]
        #    for lazy in ["false"]]
           for lazy in ["false", "true"]]
    return fmt, exp

def bomb_sdmvcc_long_read_guard_ablation():
    """Scheme A: only BoMB L1 uses a transaction-level LongReadGuard.
    All short transactions keep eager per-key intents and intent-driven GC."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", guard,
            2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_GLOBAL", 1, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]
           for guard in ["false", "true"]]
    return fmt, exp

def bomb_sdmvcc_bypass_smoke():
    """Two-node static BoMB comparison for SDMVCC long-transaction bypass."""
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES", "BOMB_MATERIAL_TYPES",
           "BOMB_RAW_MATERIAL_TYPES", "BOMB_TARGET_PRODUCTS",
           "BOMB_DYNAMIC_MODE", "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX", "SDPCC_LONG_HOLE_MODE",
           "SDPCC_LONG_HOLE_ADAPTIVE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", 2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_GLOBAL", 1, 3, 4194304, mode, "false",
            "20*BILLION", "20*BILLION"]
           for mode in ["SDPCC_LONG_HOLE_DISABLED",
                        # "SDPCC_LONG_HOLE_EXACT",
                        "SDPCC_LONG_HOLE_BLOOM"]]
    return fmt, exp

def bomb_sdmvcc_dynamic_smoke():
    """Two-node SDMVCC + BoMB dynamic-mode (adds S3/S4/S5 with topology-version
    guards and replan-retry).  Mirrors bomb_aria_dynamic_smoke with CC_ALG
    switched to SDMVCC and no ARIA_BATCH_SIZE knob."""
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES", "BOMB_MATERIAL_TYPES",
           "BOMB_RAW_MATERIAL_TYPES", "BOMB_TARGET_PRODUCTS",
           "BOMB_DYNAMIC_MODE", "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", 2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "true",
            "BOMB_LONG_TX_GLOBAL", 1, 3, 1048576,
            "20*BILLION", "20*BILLION"]]
    return fmt, exp

def bomb_sdmvcc_long_read_guard_dynamic_ablation():
    """Dynamic BoMB counterpart of the LongReadGuard ablation.
    S3/S4/S5 remain eager-intent transactions; only L1 uses the guard."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", guard,
            2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "true",
            "BOMB_LONG_TX_GLOBAL", 1, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]
           for guard in ["false", "true"]]
    return fmt, exp

def bomb_sdmvcc_long_tx_interference_ceiling():
    """Measure the maximum TP throughput recoverable by a long-txn
    optimization: identical eager-intent + GC runs with zero versus one L1
    source. Keep the short-worker count fixed so offered TP load is equal."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "false",
            2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_GLOBAL", long_sources, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]
           for long_sources in [1, 0]]
    return fmt, exp

def bomb_sdmvcc_ltc_perclient4_fwd():
    """Long-tx interference ceiling, PER_CLIENT mode with BLS=4 (half the client
    threads are long sources), forward order (with-L1 first, then without-L1)."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "false",
            2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", long_sources, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]
           for long_sources in [4, 0]]
    return fmt, exp

def bomb_sdmvcc_ltc_perclient4_rev():
    """Long-tx interference ceiling, PER_CLIENT mode with BLS=4, reverse order
    (without-L1 first, then with-L1)."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "false",
            2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", long_sources, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]
           for long_sources in [0, 4]]
    return fmt, exp

def bomb_sdmvcc_ltc_global_fwd():
    """Long-tx interference ceiling, GLOBAL mode, forward order (with-L1 first, then without-L1). Identical short load; measures max TP recoverable by a perfect long-tx optimization."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "false",
            2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_GLOBAL", long_sources, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]
           for long_sources in [1, 0]]
    return fmt, exp

def bomb_sdmvcc_ltc_global_rev():
    """Long-tx interference ceiling, GLOBAL mode, reverse order (without-L1 first, then with-L1). Controls for order/temperature drift."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "false",
            2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_GLOBAL", long_sources, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]
           for long_sources in [0, 1]]
    return fmt, exp

def bomb_sdmvcc_ltc_perclient_fwd():
    """Long-tx interference ceiling, PER_CLIENT mode, forward order (with-L1 first, then without-L1)."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "false",
            2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", long_sources, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]
           for long_sources in [1, 0]]
    return fmt, exp

def bomb_sdmvcc_ltc_perclient_rev():
    """Long-tx interference ceiling, PER_CLIENT mode, reverse order (without-L1 first, then with-L1)."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "false",
            2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", long_sources, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]
           for long_sources in [0, 1]]
    return fmt, exp

def bomb_aria_dynamic_hif():
    """Control for bomb_sdmvcc_dynamic_smoke: same dynamic-mode config but
    ARIA, with MAX_TXN_IN_FLIGHT raised to 10000 to match the SDMVCC run."""
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "ARIA_BATCH_SIZE",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES", "BOMB_MATERIAL_TYPES",
           "BOMB_RAW_MATERIAL_TYPES", "BOMB_TARGET_PRODUCTS",
           "BOMB_DYNAMIC_MODE", "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "ARIA", 2, 2, 8, 4, 10000, 16,
            2, 64, 160, 64, 4, "true",
            "BOMB_LONG_TX_GLOBAL", 1, 3, 1048576,
            "2*BILLION", "5*BILLION"]]
    return fmt, exp

def bomb_calvin_baseline():
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_TARGET_PRODUCTS", "BOMB_LONG_TX_MODE",
           "BOMB_LONG_TX_SOURCES", "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX"]
    exp = [["BOMB", "CALVIN", 2, 2, 16, 5, 256, target,
            "BOMB_LONG_TX_GLOBAL", 1, 4, 4194304]
           for target in [10, 50, 100]]
    return fmt, exp


def bomb_calvin_dynamic_smoke():
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES", "BOMB_MATERIAL_TYPES",
           "BOMB_RAW_MATERIAL_TYPES", "BOMB_TARGET_PRODUCTS",
           "BOMB_DYNAMIC_MODE", "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "CALVIN", 2, 2, 8, 4, 32,
            2, 64, 160, 64, 4, "true",
            "BOMB_LONG_TX_GLOBAL", 1, 3, 1048576,
            "2*BILLION", "5*BILLION"]]
    return fmt, exp


def bomb_aria_smoke():
    """Two-node Aria + BoMB static-mode (L1/S1/S2) correctness/performance
    smoke test.  Every Aria server owns a sequencer that must fill a
    same-sized batch, so each client node provisions BOMB_SHORT_WORKERS local
    short sources plus the globally unique long source."""
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "ARIA_BATCH_SIZE",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES", "BOMB_MATERIAL_TYPES",
           "BOMB_RAW_MATERIAL_TYPES", "BOMB_TARGET_PRODUCTS",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES", "BOMB_SHORT_WORKERS",
           "MSG_SIZE_MAX", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "ARIA", 2, 2, 8, 4, 10000, 16,
            2, 64, 160, 64, 4,
            "BOMB_LONG_TX_GLOBAL", 1, 3, 1048576,
            "2*BILLION", "5*BILLION"]]
    return fmt, exp


def bomb_aria_dynamic_smoke():
    """Two-node Aria + BoMB dynamic-mode (adds S3/S4/S5 with topology-version
    guards and replan-retry)."""
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "ARIA_BATCH_SIZE",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES", "BOMB_MATERIAL_TYPES",
           "BOMB_RAW_MATERIAL_TYPES", "BOMB_TARGET_PRODUCTS",
           "BOMB_DYNAMIC_MODE", "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "ARIA", 2, 2, 8, 4, 64, 16,
            2, 64, 160, 64, 4, "true",
            "BOMB_LONG_TX_GLOBAL", 1, 3, 1048576,
            "2*BILLION", "5*BILLION"]]
    return fmt, exp


def bomb_sdmvcc_ltc_perclient4_guard_fwd():
    """PER_CLIENT BLS=4 with SDMVCC_LONG_READ_GUARD=true, forward (L1 first)."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "true",
            2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", long_sources, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]
           for long_sources in [4, 0]]
    return fmt, exp

def bomb_sdmvcc_ltc_perclient4_guard_rev():
    """PER_CLIENT BLS=4 with SDMVCC_LONG_READ_GUARD=true, reverse (no-L1 first)."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "true",
            2, 2, 16, 4, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", long_sources, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]
           for long_sources in [0, 4]]
    return fmt, exp


def bomb_sdmvcc_htap8_fwd():
    """HTAP: CLIENT_THREAD_CNT=8, BLS=8 (cluster-wide 8 long txns => 4 L1 + 4 short per
    client node), guard off first then on. Compares against TP-only (CT=4, BLS=0)
    with identical short-thread supply (4 per node)."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "false",
            2, 2, 16, 8, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", 8, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"],
           ["BOMB", "SDMVCC", "false", "true", "true",
            2, 2, 16, 8, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", 8, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]]
    return fmt, exp

def bomb_sdmvcc_htap8_rev():
    """HTAP same as htap8_fwd but guard on first then off (reverse order)."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "true",
            2, 2, 16, 8, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", 8, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"],
           ["BOMB", "SDMVCC", "false", "true", "false",
            2, 2, 16, 8, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", 8, 3, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]]
    return fmt, exp

def bomb_sdmvcc_unsafe_l1_upper_fwd():
    """One-off upper bound: correct eager L1 first, then UNSAFE no-intent L1."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "SDMVCC_UNSAFE_L1_NO_INTENT",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_SOURCES", "MSG_SIZE_MAX",
           "WARMUP_TIMER", "DONE_TIMER"]
    common = [2, 2, 16, 4, 10000, 8, 72000, 198000, 75000, 100,
              "false", 4, 4194304, "30*BILLION", "30*BILLION"]
    exp = [["BOMB", "SDMVCC", "false", "true", "false", unsafe] + common
           for unsafe in ["false", "true"]]
    return fmt, exp

def bomb_sdmvcc_unsafe_l1_upper_rev():
    """One-off upper bound in reverse order: UNSAFE no-intent, then correct."""
    fmt, exp = bomb_sdmvcc_unsafe_l1_upper_fwd()
    return fmt, list(reversed(exp))

def bomb_sdmvcc_htap8_unsafe_fwd():
    """HTAP CT=8 BLS=8 (4 L1 + 4 short per node), guard off, unsafe off first
    then on. Upper bound for removing L1 read intents under mixed workload."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "SDMVCC_UNSAFE_L1_NO_INTENT",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_SOURCES", "MSG_SIZE_MAX",
           "WARMUP_TIMER", "DONE_TIMER"]
    common = [2, 2, 16, 8, 10000, 8, 72000, 198000, 75000, 100,
              "false", 8, 4194304, "30*BILLION", "30*BILLION"]
    exp = [["BOMB", "SDMVCC", "false", "true", "false", unsafe] + common
           for unsafe in ["false", "true"]]
    return fmt, exp

def bomb_sdmvcc_htap8_unsafe_rev():
    """HTAP CT=8 BLS=8, guard off, unsafe on first then off (reverse order)."""
    fmt, exp = bomb_sdmvcc_htap8_unsafe_fwd()
    exp = [exp[1], exp[0]]
    return fmt, exp


def bomb_sdmvcc_htap8_mix_fwd():
    """HTAP CT=8/BLS=8: back-to-back L1 injection (legacy, one L1 in flight per
    source, self-clocked) vs periodic mix (BOMB_L1_PERIODIC_MIX=true, one L1
    every BOMB_L1_MIX_PERIOD=64 txns per long source, L1s may overlap).  Guard
    off in both."""
    fmt = ["WORKLOAD", "CC_ALG", "SDMVCC_LAZY_READ_INTENT",
           "SDMVCC_INTENT_GC", "SDMVCC_LONG_READ_GUARD",
           "NODE_CNT", "CLIENT_NODE_CNT", "THREAD_CNT",
           "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "BOMB_FACTORY_COUNT", "BOMB_PRODUCT_TYPES",
           "BOMB_MATERIAL_TYPES", "BOMB_RAW_MATERIAL_TYPES",
           "BOMB_TARGET_PRODUCTS", "BOMB_DYNAMIC_MODE",
           "BOMB_LONG_TX_MODE", "BOMB_LONG_TX_SOURCES",
           "BOMB_SHORT_WORKERS", "BOMB_L1_PERIODIC_MIX",
           "BOMB_L1_MIX_PERIOD", "MSG_SIZE_MAX",
           "SDPCC_LONG_HOLE_MODE", "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "SDMVCC", "false", "true", "false",
            2, 2, 16, 8, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", 8, 3, "false", 256, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"],
           ["BOMB", "SDMVCC", "false", "true", "false",
            2, 2, 16, 8, 10000,
            8, 72000, 198000, 75000, 100, "false",
            "BOMB_LONG_TX_PER_CLIENT", 8, 3, "true", 256, 4194304,
            "SDPCC_LONG_HOLE_DISABLED", "30*BILLION", "30*BILLION"]]
    return fmt, exp

def bomb_sdmvcc_htap8_mix_rev():
    """Same two configs as mix_fwd, reverse order (periodic mix first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix_fwd()
    exp = [exp[1], exp[0]]
    return fmt, exp


def bomb_sdmvcc_htap8_mix_guard_fwd():
    """HTAP CT=8/BLS=8 periodic mix (X=256): L1 read-intent bloom guard
    off vs on under the dense periodic-mix L1 load. Mix on in both."""
    fmt, exp = bomb_sdmvcc_htap8_mix_fwd()
    # keep config2 (mix=true, guard=false) and flip guard to true
    cfg_mix = exp[1][:]
    cfg_mix[4] = "true"   # SDMVCC_LONG_READ_GUARD
    return fmt, [exp[1], cfg_mix]


def bomb_sdmvcc_htap8_mix_guard_rev():
    """Same two configs as mix_guard_fwd, reverse order (guard on first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix_guard_fwd()
    return fmt, [exp[1], exp[0]]


def bomb_sdmvcc_htap8_mix_unsafe_fwd():
    """HTAP CT=8/BLS=8 periodic mix (X=256), guard off: unsafe (no L1 read
    intent) off vs on under the dense periodic-mix L1 load. Upper bound for
    zero-cost L1 read intents; intentionally incorrect snapshot semantics."""
    fmt, exp = bomb_sdmvcc_htap8_mix_fwd()
    # insert SDMVCC_UNSAFE_L1_NO_INTENT right after SDMVCC_LONG_READ_GUARD
    fmt.insert(5, "SDMVCC_UNSAFE_L1_NO_INTENT")
    for row in exp:
        row.insert(5, "false")
    # keep config2 (mix=true, unsafe=false) and flip unsafe to true
    cfg_mix = exp[1][:]
    cfg_mix[5] = "true"
    return fmt, [exp[1], cfg_mix]


def bomb_sdmvcc_htap8_mix_unsafe_rev():
    """Same two configs as mix_unsafe_fwd, reverse order (unsafe on first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix_unsafe_fwd()
    return fmt, [exp[1], exp[0]]


def bomb_sdmvcc_htap8_mix_unsafe_skiprd_fwd():
    """HTAP CT=8/BLS=8 periodic mix (X=256), guard off, unsafe ON in both:
    L1 acquire_locks registers all requests vs only the write set
    (BOMB_L1_ACQUIRE_ONLY_WRITES, ~100 WR rows out of ~19.4k).  Isolates the
    cost of the per-row registration machinery itself (loop + index_read +
    register_access) on top of the zero-intent unsafe baseline.  Intentionally
    incorrect read-set snapshot semantics."""
    fmt, exp = bomb_sdmvcc_htap8_mix_unsafe_fwd()
    # unsafe lives at index 5; append the scale ablation flag at the end
    fmt.append("BOMB_L1_ACQUIRE_ONLY_WRITES")
    for row in exp:
        row.append("false")
    # exp[1] = mix + unsafe on; flip only the ablation flag for the second cfg
    cfg_on = exp[1][:]
    cfg_on[-1] = "true"
    return fmt, [exp[1], cfg_on]


def bomb_sdmvcc_htap8_mix_unsafe_skiprd_rev():
    """Same two configs as mix_unsafe_skiprd_fwd, reverse order (skiprd on
    first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix_unsafe_skiprd_fwd()
    return fmt, [exp[1], exp[0]]


def bomb_sdmvcc_htap8_mix_vs_nol1_fwd():
    """HTAP CT=8/BLS=8 periodic mix, guard off, unsafe OFF, correct semantics
    in both arms.  cfg A: periodic mix, one L1 every BOMB_L1_MIX_PERIOD=256
    txns (BLS=8 long sources).  cfg B: identical client/short-txn stream but
    BOMB_LONG_TX_SOURCES=0 -> no L1 at all.  Measures the QPS cost of mixing
    one L1 per 256 txns into the short stream."""
    fmt, exp = bomb_sdmvcc_htap8_mix_unsafe_fwd()
    # exp[0] = periodic mix on + unsafe off (correct baseline WITH L1)
    cfg_a = exp[0][:]
    assert cfg_a[5] == "false", "unsafe must be off: %s" % cfg_a[5]  # unsafe
    assert cfg_a[20] == "true", "periodic mix must be on: %s" % cfg_a[20]
    cfg_b = cfg_a[:]
    cfg_b[18] = 0  # BOMB_LONG_TX_SOURCES: 8 -> 0 (no L1 in the whole cluster)
    return fmt, [cfg_a, cfg_b]


def bomb_sdmvcc_htap8_mix_vs_nol1_rev():
    """Same two configs as mix_vs_nol1_fwd, reverse order (no-L1 first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix_vs_nol1_fwd()
    return fmt, [exp[1], exp[0]]


def bomb_sdmvcc_htap8_mix64_vs_nol1_fwd():
    """HTAP CT=8/BLS=8 periodic mix, guard off, unsafe OFF, correct semantics
    in both arms.  cfg A: periodic mix, one L1 every BOMB_L1_MIX_PERIOD=64
    txns (4x denser than the 256 run).  cfg B: identical short-txn stream but
    BOMB_LONG_TX_SOURCES=0 -> no L1.  Measures the QPS/TPS cost of a denser
    L1 mix ratio."""
    fmt, exp = bomb_sdmvcc_htap8_mix_vs_nol1_fwd()
    cfg_a = exp[0][:]
    assert str(cfg_a[21]) == "256", "base must be X=256: %s" % cfg_a[21]
    cfg_a = cfg_a[:]
    cfg_a[21] = 64  # BOMB_L1_MIX_PERIOD: 256 -> 64
    cfg_b = cfg_a[:]
    cfg_b[18] = 0  # BOMB_LONG_TX_SOURCES: 8 -> 0 (no L1)
    return fmt, [cfg_a, cfg_b]


def bomb_sdmvcc_htap8_mix64_vs_nol1_rev():
    """Same two configs as mix64_vs_nol1_fwd, reverse order (no-L1 first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix64_vs_nol1_fwd()
    return fmt, [exp[1], exp[0]]


def bomb_sdmvcc_htap8_mix32_vs_nol1_fwd():
    """HTAP CT=8/BLS=8 periodic mix, guard off, unsafe OFF, correct semantics
    in both arms.  cfg A: periodic mix, one L1 every BOMB_L1_MIX_PERIOD=32
    txns (8x denser than the 256 run, 2x denser than 64).  cfg B: identical
    short-txn stream but BOMB_LONG_TX_SOURCES=0 -> no L1."""
    fmt, exp = bomb_sdmvcc_htap8_mix_vs_nol1_fwd()
    cfg_a = exp[0][:]
    assert str(cfg_a[21]) == "256", "base must be X=256: %s" % cfg_a[21]
    cfg_a = cfg_a[:]
    cfg_a[21] = 32  # BOMB_L1_MIX_PERIOD: 256 -> 32
    cfg_b = cfg_a[:]
    cfg_b[18] = 0  # BOMB_LONG_TX_SOURCES: 8 -> 0 (no L1)
    return fmt, [cfg_a, cfg_b]


def bomb_sdmvcc_htap8_mix32_vs_nol1_rev():
    """Same two configs as mix32_vs_nol1_fwd, reverse order (no-L1 first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix32_vs_nol1_fwd()
    return fmt, [exp[1], exp[0]]


def bomb_sdmvcc_htap8_mix16_vs_nol1_fwd():
    """HTAP CT=8/BLS=8 periodic mix, guard off, unsafe OFF.  cfg A: one L1
    every BOMB_L1_MIX_PERIOD=16 txns.  cfg B: no L1."""
    fmt, exp = bomb_sdmvcc_htap8_mix_vs_nol1_fwd()
    cfg_a = exp[0][:]
    assert str(cfg_a[21]) == "256", "base must be X=256: %s" % cfg_a[21]
    cfg_a = cfg_a[:]
    cfg_a[21] = 16  # BOMB_L1_MIX_PERIOD: 256 -> 16
    cfg_b = cfg_a[:]
    cfg_b[18] = 0  # BOMB_LONG_TX_SOURCES: 8 -> 0 (no L1)
    return fmt, [cfg_a, cfg_b]


def bomb_sdmvcc_htap8_mix16_vs_nol1_rev():
    """Same two configs as mix16_vs_nol1_fwd, reverse order (no-L1 first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix16_vs_nol1_fwd()
    return fmt, [exp[1], exp[0]]


def bomb_sdmvcc_htap8_mix8_vs_nol1_fwd():
    """HTAP CT=8/BLS=8 periodic mix, guard off, unsafe OFF.  cfg A: one L1
    every BOMB_L1_MIX_PERIOD=8 txns.  cfg B: no L1."""
    fmt, exp = bomb_sdmvcc_htap8_mix_vs_nol1_fwd()
    cfg_a = exp[0][:]
    assert str(cfg_a[21]) == "256", "base must be X=256: %s" % cfg_a[21]
    cfg_a = cfg_a[:]
    cfg_a[21] = 8  # BOMB_L1_MIX_PERIOD: 256 -> 8
    cfg_b = cfg_a[:]
    cfg_b[18] = 0  # BOMB_LONG_TX_SOURCES: 8 -> 0 (no L1)
    return fmt, [cfg_a, cfg_b]


def bomb_sdmvcc_htap8_mix8_vs_nol1_rev():
    """Same two configs as mix8_vs_nol1_fwd, reverse order (no-L1 first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix8_vs_nol1_fwd()
    return fmt, [exp[1], exp[0]]


def bomb_sdmvcc_htap8_mix64_unsafe_fwd():
    """X=64 periodic mix, guard off: L1 read-intent unsafe ON (no L1 read
    intents registered, incorrect snapshot semantics, upper bound) vs unsafe
    OFF (correct semantics).  Same L1 density in both arms -> isolates the
    cost of L1 read-intent registration itself."""
    fmt, exp = bomb_sdmvcc_htap8_mix_vs_nol1_fwd()
    cfg_a = exp[0][:]
    assert str(cfg_a[21]) == "256", "base must be X=256: %s" % cfg_a[21]
    cfg_a = cfg_a[:]
    cfg_a[21] = 64   # BOMB_L1_MIX_PERIOD
    cfg_a[5] = "true"    # SDMVCC_UNSAFE_L1_NO_INTENT on
    cfg_b = cfg_a[:]
    cfg_b[5] = "false"   # unsafe off, same density
    return fmt, [cfg_a, cfg_b]


def bomb_sdmvcc_htap8_mix64_unsafe_rev():
    """Same two configs as mix64_unsafe_fwd, reverse order (unsafe off first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix64_unsafe_fwd()
    return fmt, [exp[1], exp[0]]


def bomb_sdmvcc_htap8_mix32_unsafe_fwd():
    """X=32 periodic mix, guard off: unsafe ON vs OFF, same density both arms."""
    fmt, exp = bomb_sdmvcc_htap8_mix64_unsafe_fwd()
    cfg_a = exp[0][:]
    cfg_a[21] = 32
    cfg_b = cfg_a[:]
    cfg_b[5] = "false"
    return fmt, [cfg_a, cfg_b]


def bomb_sdmvcc_htap8_mix32_unsafe_rev():
    """Same two configs as mix32_unsafe_fwd, reverse order (unsafe off first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix32_unsafe_fwd()
    return fmt, [exp[1], exp[0]]


def bomb_sdmvcc_htap8_mix16_unsafe_fwd():
    """X=16 periodic mix, guard off: unsafe ON vs OFF, same density both arms."""
    fmt, exp = bomb_sdmvcc_htap8_mix64_unsafe_fwd()
    cfg_a = exp[0][:]
    cfg_a[21] = 16
    cfg_b = cfg_a[:]
    cfg_b[5] = "false"
    return fmt, [cfg_a, cfg_b]


def bomb_sdmvcc_htap8_mix16_unsafe_rev():
    """Same two configs as mix16_unsafe_fwd, reverse order (unsafe off first)."""
    fmt, exp = bomb_sdmvcc_htap8_mix16_unsafe_fwd()
    return fmt, [exp[1], exp[0]]


experiment_map = {
    'bomb_static': bomb_static,
    'bomb_dynamic': bomb_dynamic,
    'ycsb_idle_default': ycsb_idle_default,
    'bomb_sdmvcc_htap8_mix_fwd': bomb_sdmvcc_htap8_mix_fwd,
    'bomb_sdmvcc_htap8_mix_rev': bomb_sdmvcc_htap8_mix_rev,
    'bomb_sdmvcc_htap8_mix_guard_fwd': bomb_sdmvcc_htap8_mix_guard_fwd,
    'bomb_sdmvcc_htap8_mix_guard_rev': bomb_sdmvcc_htap8_mix_guard_rev,
    'bomb_sdmvcc_htap8_mix_unsafe_fwd': bomb_sdmvcc_htap8_mix_unsafe_fwd,
    'bomb_sdmvcc_htap8_mix_unsafe_rev': bomb_sdmvcc_htap8_mix_unsafe_rev,
    'bomb_sdmvcc_htap8_mix_unsafe_skiprd_fwd': bomb_sdmvcc_htap8_mix_unsafe_skiprd_fwd,
    'bomb_sdmvcc_htap8_mix_unsafe_skiprd_rev': bomb_sdmvcc_htap8_mix_unsafe_skiprd_rev,
    'bomb_sdmvcc_htap8_mix_vs_nol1_fwd': bomb_sdmvcc_htap8_mix_vs_nol1_fwd,
    'bomb_sdmvcc_htap8_mix_vs_nol1_rev': bomb_sdmvcc_htap8_mix_vs_nol1_rev,
    'bomb_sdmvcc_htap8_mix64_vs_nol1_fwd': bomb_sdmvcc_htap8_mix64_vs_nol1_fwd,
    'bomb_sdmvcc_htap8_mix64_vs_nol1_rev': bomb_sdmvcc_htap8_mix64_vs_nol1_rev,
    'bomb_sdmvcc_htap8_mix32_vs_nol1_fwd': bomb_sdmvcc_htap8_mix32_vs_nol1_fwd,
    'bomb_sdmvcc_htap8_mix32_vs_nol1_rev': bomb_sdmvcc_htap8_mix32_vs_nol1_rev,
    'bomb_sdmvcc_htap8_mix16_vs_nol1_fwd': bomb_sdmvcc_htap8_mix16_vs_nol1_fwd,
    'bomb_sdmvcc_htap8_mix16_vs_nol1_rev': bomb_sdmvcc_htap8_mix16_vs_nol1_rev,
    'bomb_sdmvcc_htap8_mix8_vs_nol1_fwd': bomb_sdmvcc_htap8_mix8_vs_nol1_fwd,
    'bomb_sdmvcc_htap8_mix8_vs_nol1_rev': bomb_sdmvcc_htap8_mix8_vs_nol1_rev,
    'bomb_sdmvcc_htap8_mix64_unsafe_fwd': bomb_sdmvcc_htap8_mix64_unsafe_fwd,
    'bomb_sdmvcc_htap8_mix64_unsafe_rev': bomb_sdmvcc_htap8_mix64_unsafe_rev,
    'bomb_sdmvcc_htap8_mix32_unsafe_fwd': bomb_sdmvcc_htap8_mix32_unsafe_fwd,
    'bomb_sdmvcc_htap8_mix32_unsafe_rev': bomb_sdmvcc_htap8_mix32_unsafe_rev,
    'bomb_sdmvcc_htap8_mix16_unsafe_fwd': bomb_sdmvcc_htap8_mix16_unsafe_fwd,
    'bomb_sdmvcc_htap8_mix16_unsafe_rev': bomb_sdmvcc_htap8_mix16_unsafe_rev,
    'bomb_sdmvcc_htap8_unsafe_fwd': bomb_sdmvcc_htap8_unsafe_fwd,
    'bomb_sdmvcc_htap8_unsafe_rev': bomb_sdmvcc_htap8_unsafe_rev,
    # for test
    'ycsb_skew' : ycsb_skew,
    # YCSB_WRITE
    'ycsb_writes_PCC': ycsb_writes_PCC, # calvin sdpcc
    'ycsb_writes_OCC': ycsb_writes_OCC, # sdocc
    'ycsb_aria_batch3': ycsb_aria_batch3, # Aria的write

    # YCSB_SKEW:
    'ycsb_skew_PCC': ycsb_skew_PCC, # calvin sdpcc
    'ycsb_skew_OCC': ycsb_skew_OCC, # sdocc
    'ycsb_aria_batch': ycsb_aria_batch, # Aria的skew

    # YCSB_DIST:
    'ycsb_dist_ratio_PCC': ycsb_dist_ratio_PCC, # calvin sdpcc
    'ycsb_dist_ratio_OCC': ycsb_dist_ratio_OCC, # sdocc
    'ycsb_aria_batch2': ycsb_aria_batch2,  # Aria的dist

    # 随机等待
    'ycsb_random_idle_PCC': ycsb_random_idle_PCC, # calvin sdpcc
    'ycsb_random_idle_OCC': ycsb_random_idle_OCC, # sdocc
    'ycsb_random_idle_ARIA':ycsb_random_idle_ARIA, # Aria

    # batch size
    'ycsb_batch_size_PCC' : ycsb_batch_size_PCC, # calvin sdpcc
    'ycsb_batch_size_OCC' : ycsb_batch_size_OCC, # sdocc Aria

    # TPCC_WH
    'tpcc_wh_PCC': tpcc_wh_PCC,
    'tpcc_wh_OCC': tpcc_wh_OCC,
    'tpcc_aria_batch': tpcc_aria_batch, # Aria的TPCC WH

    # 下面是优化测试
    # SDOCC的优化测试
    'ycsb_rwset_variable_ratio': ycsb_rwset_variable_ratio,
    # SDPCC的优化测试
    'ycsb_sch_cnt': ycsb_sch_cnt,
    'ycsb_sdpcc_long_hole': ycsb_sdpcc_long_hole,
    'ycsb_sdpcc_long_hole_adaptive': ycsb_sdpcc_long_hole_adaptive,
    'ycsb_sdpcc_long_hole_size': ycsb_sdpcc_long_hole_size,
    'ycsb_sdmvcc_long': ycsb_sdmvcc_long,
    'chbenchmark_sdmvcc_test': chbenchmark_sdmvcc_test,
    'bomb_calvin_smoke': bomb_calvin_smoke,
    'bomb_calvin_baseline': bomb_calvin_baseline,
    'bomb_calvin_dynamic_smoke': bomb_calvin_dynamic_smoke,
    'bomb_aria_smoke': bomb_aria_smoke,
    'bomb_aria_dynamic_smoke': bomb_aria_dynamic_smoke,
    'bomb_aria_dynamic_hif': bomb_aria_dynamic_hif,
    'bomb_sdmvcc_smoke': bomb_sdmvcc_smoke,
    'bomb_sdmvcc_lazy_intent_ablation': bomb_sdmvcc_lazy_intent_ablation,
    'bomb_sdmvcc_long_read_guard_ablation': bomb_sdmvcc_long_read_guard_ablation,
    'bomb_sdmvcc_long_read_guard_dynamic_ablation': bomb_sdmvcc_long_read_guard_dynamic_ablation,
    'bomb_sdmvcc_long_tx_interference_ceiling': bomb_sdmvcc_long_tx_interference_ceiling,
    'bomb_sdmvcc_ltc_perclient_rev': bomb_sdmvcc_ltc_perclient_rev,
    'bomb_sdmvcc_ltc_perclient_fwd': bomb_sdmvcc_ltc_perclient_fwd,
    'bomb_sdmvcc_ltc_global_rev': bomb_sdmvcc_ltc_global_rev,
    'bomb_sdmvcc_ltc_global_fwd': bomb_sdmvcc_ltc_global_fwd,
    'bomb_sdmvcc_ltc_perclient4_fwd': bomb_sdmvcc_ltc_perclient4_fwd,
    'bomb_sdmvcc_ltc_perclient4_rev': bomb_sdmvcc_ltc_perclient4_rev,
    'bomb_sdmvcc_ltc_perclient4_guard_fwd': bomb_sdmvcc_ltc_perclient4_guard_fwd,
    'bomb_sdmvcc_ltc_perclient4_guard_rev': bomb_sdmvcc_ltc_perclient4_guard_rev,
    'bomb_sdmvcc_ltc_perclient4_guard_rev': bomb_sdmvcc_ltc_perclient4_guard_rev,

    'bomb_sdmvcc_bypass_smoke': bomb_sdmvcc_bypass_smoke,
    'bomb_sdmvcc_dynamic_smoke': bomb_sdmvcc_dynamic_smoke,

    # Scaling
    'ycsb_scaling_PCC': ycsb_scaling_PCC, # calvin sdpcc
    'ycsb_scaling_OCC': ycsb_scaling_OCC, # sdocc
    'ycsb_scaling_ARIA': ycsb_scaling_ARIA, # Aria

    'tpcc_scaling_PCC': tpcc_scaling_PCC, # calvin sdpcc
    'tpcc_scaling_OCC': tpcc_scaling_OCC, # sdocc
    'tpcc_scaling_ARIA': tpcc_scaling_ARIA, # Aria

    # 下面是没跑的实验
    'ycsb_rwset_ratio': ycsb_rwset_ratio,
    'tpcc_dist_ratio': tpcc_dist_ratio,
    'bomb_sdmvcc_htap8_fwd': bomb_sdmvcc_htap8_fwd,
    'bomb_sdmvcc_htap8_rev': bomb_sdmvcc_htap8_rev,
    'bomb_sdmvcc_unsafe_l1_upper_fwd': bomb_sdmvcc_unsafe_l1_upper_fwd,
    'bomb_sdmvcc_unsafe_l1_upper_rev': bomb_sdmvcc_unsafe_l1_upper_rev,
    'tpcc_aria_batch2': tpcc_aria_batch2,
}


# Default values for variable configurations
configs = {
    "NODE_CNT" : 2,
    "THREAD_CNT": 16,
    "REPLICA_CNT": 0,
    "REPLICA_TYPE": "AP",
    "REM_THREAD_CNT": 2,
    "SEND_THREAD_CNT": 2,
    "CLIENT_NODE_CNT" : "NODE_CNT",
    "CLIENT_THREAD_CNT" : 4,
    "CLIENT_REM_THREAD_CNT" : 2,
    "CLIENT_SEND_THREAD_CNT" : 2,
    "MAX_TXN_PER_PART" : 500000,
    "WORKLOAD" : "YCSB",
    "CC_ALG" : "CNULL",
    "MPR" : 0.2,    #分布式事务比列
    "TPORT_TYPE":"TCP",
    "TPORT_PORT":"18000",
    "PART_CNT": "NODE_CNT",
    "PART_PER_TXN": 2,
    "MAX_TXN_IN_FLIGHT": 10000,
    "NETWORK_DELAY": '0UL',
    "NETWORK_DELAY_TEST": 'false',
    "DONE_TIMER": "1 * 20 * BILLION // ~1 minutes",
    "WARMUP_TIMER": "1 * 20 * BILLION // ~1 minutes",
    "SEQ_BATCH_TIMER": "5 * 1 * MILLION // ~5ms -- same as CALVIN paper",
    "BATCH_TIMER" : "0",
    "PROG_TIMER" : "10 * BILLION // in s",
    "NETWORK_TEST" : "false",
    "ABORT_PENALTY": "10 * 1000000UL   // in ns.",
    "ABORT_PENALTY_MAX": "5 * 100 * 1000000UL   // in ns.",
    "MSG_TIME_LIMIT": "0",
    "MSG_SIZE_MAX": 4096,
    "TXN_WRITE_PERC":1.0,
    "PRIORITY":"PRIORITY_ACTIVE",
    "TWOPL_LITE":"false",
    "LONG_TXN_WORKLOAD":'false',
    "LONG_QUERY_PERC":0.0,
    "OPEN_DISTRIBUTED_WATERMARK":'false',
    "SDPCC_LONG_HOLE_MODE":"SDPCC_LONG_HOLE_DISABLED",
    "SDMVCC_LONG_READ_GUARD":"false",
    "SDMVCC_UNSAFE_L1_NO_INTENT":"false",
    "OPEN_RANDOM_WAIT":'false',
#YCSB
    "INIT_PARALLELISM" : 8,
    "TUP_WRITE_PERC":0.2,
    "ZIPF_THETA":0.7,
    "ACCESS_PERC":0.03,
    "DATA_PERC": 100,
    "REQ_PER_QUERY": 10,
    "REQ_PER_SHORT_QUERY": 10,
    "SYNTH_TABLE_SIZE":"1048576*8",
    "RWSET_KNOWN_RATIO":1.0,
    "RWSET_KNOWN":"false",
    "RWSET_VARIABLE_RATIO":0.0,
#TPCC
    "NUM_WH":32,
    "PERC_PAYMENT":0.489,
    "MPR_NEWORDER":"MPR",
    "MAX_ITEMS_NORM":100000,
    "CUST_PER_DIST_NORM":3000,
#CH-benCHmark
    "CH_OLAP_PERC":0.10,
    "CH_QUERY_MIN":1,
    "CH_QUERY_MAX":22,
    "CH_QUERY_WAREHOUSE_PCT":100,
    "CH_SUPPLIER_COUNT":10000,
#BoMB
    "BOMB_DYNAMIC_MODE":"false",
    "BOMB_L1_PERIODIC_MIX":"false",
    "BOMB_LONG_TX_MODE":"BOMB_LONG_TX_GLOBAL",
    "BOMB_LONG_TX_SOURCES":1,
    "BOMB_SHORT_WORKERS":4,
    "BOMB_FACTORY_COUNT":8,
    "BOMB_PRODUCT_TYPES":72000,
    "BOMB_MATERIAL_TYPES":198000,
    "BOMB_RAW_MATERIAL_TYPES":75000,
    "BOMB_TREES_PER_PRODUCT":5,
    "BOMB_TREE_SIZE":10,
    "BOMB_RAW_MATERIALS_PER_LEAF":3,
    "BOMB_TARGET_PRODUCTS":100,
    "BOMB_TARGET_MATERIALS":1,
#TXN
    "PRORATE_RATIO":0,
    "ARIA_BATCH_SIZE":3000,
    "LOGGING":"false",
    "SCHEDULER_CNT": 3,
#OTHERS
    # "DEBUG_DISTR":"false",
    # "DEBUG_ALLOC":"false",
    # "DEBUG_RACE":"false",
    "MODE":"NORMAL_MODE",
    "SHMEM_ENV":"false",
    "STRICT_PPT":0,
    "SET_AFFINITY":"true",
    "SERVER_GENERATE_QUERIES":"false",
    "SKEW_METHOD":"ZIPF",
    "ENVIRONMENT_EC2":"false",
    "YCSB_ABORT_MODE":"false",
    "LOAD_METHOD": "LOAD_MAX",
    "ISOLATION_LEVEL":"SERIALIZABLE"
}

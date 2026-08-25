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

##############################
# END PLOTS
##############################

experiment_map = {
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

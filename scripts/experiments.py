import itertools
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

def ycsb_scaling():
    wl = 'YCSB'
    # nnodes = [12]
    nnodes = [2,4,6,8,10,12]
    # algos=['CALVIN','ARIA','SDPCC']
    algos=['CARACAL']
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

def ycsb_skew_pip():
    wl = 'YCSB'
    nnodes = [2]
    # algos=['CALVIN','ARIA','CARACAL','SDPCC','SDOCC']
    algos=['CARACAL']
    base_table_size=1048576*8
    txn_write_perc = [1]
    tup_write_perc = [0.2]
    load = [10000]
    total_cnt=[16]
    # scnt = [1]
    scnt = [3]
    skew = [0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5]
    # skew = [1.5]
    # skew = [0.1]
    # skew = [0.1,1.5]
    fmt = ["WORKLOAD","CC_ALG","ZIPF_THETA","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,sk,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,t_cnt,s_cnt] for t_cnt,s_cnt,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(total_cnt,scnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_writes():
    wl = 'YCSB'
    # nnodes = [4]
    nnodes = [2]
    algos=['CARACAL']
    # algos=['CALVIN','ARIA','SDPCC','SDOCC']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    # tup_write_perc = [0.0,0.2,0.4,0.6,0.8,1.0]
    load = [10000]
    total_cnt=[16]
    skew = [0.0]
    # skew = [0.9]
    fmt = ["WORKLOAD","CC_ALG","RANDOM_WAIT_TIME","OPEN_RANDOM_WAIT","TUP_WRITE_PERC","NODE_CNT","SYNTH_TABLE_SIZE","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,algo,wait,random_wait,tup_wr_perc,n,base_table_size*n,txn_wr_perc,ld,sk,t_cnt] for t_cnt,txn_wr_perc,tup_wr_perc,wait,ld,n,sk,algo in itertools.product(total_cnt,txn_write_perc,tup_write_perc,wait_time,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_random_idle():
    wl = 'YCSB'
    nnodes = [2]
    algos=['CARACAL']
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
    exp = [[wl,algo,wait,random_wait,tup_wr_perc,n,base_table_size*n,txn_wr_perc,ld,sk,t_cnt,9000] for t_cnt,txn_wr_perc,tup_wr_perc,wait,ld,n,sk,algo in itertools.product(total_cnt,txn_write_perc,tup_write_perc,wait_time,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_dist_ratio():
    wl = 'YCSB'
    nnodes = [2]
    # algos=['CALVIN','ARIA','SDPCC','SDOCC']
    algos=['CARACAL']
    # algos=['CALVIN', 'NO_WAIT']
    base_table_size=1048576*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.2]
    load = [10000]
    mpr=[0.0]
    # mpr=[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1]
    total_cnt=[16]
    scnt = [3]
    fmt = ["WORKLOAD","CC_ALG","MPR","NODE_CNT","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","THREAD_CNT","SCHEDULER_CNT"]
    exp = [[wl,algo,mpr,n,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,thr,s_cnt] for thr,s_cnt,ld,tup_wr_perc,txn_wr_perc,n,mpr,algo in itertools.product(total_cnt,scnt,load,tup_write_perc,txn_write_perc,nnodes,mpr,algos)]
    return fmt,exp

# for skew
def ycsb_aria_batch():
    wl = 'YCSB'
    algos=['CARACAL']
    # aria_batch_size=[9999]
    # aria_batch_size=[50, 100, 500, 1000, 2000, 3000, 5000, 9000, 9999]
    aria_batch_size=[5000, 9000]
    skew = [0.1,0.3,0.5,0.7,0.9,1.1,1.3,1.5]
    fmt = ["WORKLOAD","ARIA_BATCH_SIZE","ZIPF_THETA","CC_ALG"]
    exp = [[wl,bs,sk,algo] for algo,sk,bs in itertools.product(algos,skew,aria_batch_size)]
    return fmt,exp

# for dist
def ycsb_aria_batch2():
    wl = 'YCSB'
    algos=['CARACAL']
    # aria_batch_size=[50, 100, 500, 1000, 2000, 3000, 5000, 9000, 9999]
    aria_batch_size=[5000, 9000]
    mpr=[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1]
    skew = 0.7
    fmt = ["WORKLOAD","ARIA_BATCH_SIZE","MPR","CC_ALG","ZIPF_THETA"]
    exp = [[wl,bs,m,algo,skew] for algo,m,bs in itertools.product(algos,mpr,aria_batch_size)]
    return fmt,exp

# for write
def ycsb_aria_batch3():
    wl = 'YCSB'
    algos=['CARACAL']
    # aria_batch_size=[50, 100, 500, 1000, 2000, 3000, 5000, 9000, 9999]
    aria_batch_size=[5000, 9000]
    skew = 0.7
    tup_write_perc = [0.0,0.2,0.4,0.6,0.8,1.0]
    fmt = ["WORKLOAD","ARIA_BATCH_SIZE","TXN_WRITE_PERC","CC_ALG","ZIPF_THETA"]
    exp = [[wl,bs,tup_w,algo,skew] for algo,tup_w,bs in itertools.product(algos,tup_write_perc,aria_batch_size)]
    return fmt,exp

def ycsb_batch_size():
    wl = 'YCSB'
    algos=['CARACAL']
    # algos=['ARIA']
    aria_batch_size=[50,100,500,1000,2000,3000,5000,9000]
    # aria_batch_size=[9999]
    total_cnt=[16]
    skew = [0.3,0.7]
    fmt = ["WORKLOAD","ARIA_BATCH_SIZE","ZIPF_THETA","THREAD_CNT","CC_ALG"]
    exp = [[wl,bs,sk,thr,algo] for algo,sk,bs,thr in itertools.product(algos,skew,aria_batch_size,total_cnt)]
    return fmt,exp

def tpcc_scaling():
    wl = 'TPCC'
    nnodes = [2,4,6,8,10,12]
    # nnodes = [8]
    algos=['CARACAL']
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

def tpcc_wh():
    wl = 'TPCC'
    nnodes = [2]
    algos=['CARACAL']
    # algos=['CALVIN']
    # algos=['CALVIN','ARIA','SDOCC']
    # algos=['CALVIN','ARIA','SDPCC','SDOCC']
    # npercpay=[0.0]
    npercpay=[0.489]
    # num_wh=[128]
    # num_wh=[128,64,32,16,8]
    num_wh=[64,32,16,8]
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

# for wh
def tpcc_aria_batch():
    wl = 'TPCC'
    nnodes = 2
    algos=['CARACAL']
    # aria_batch_size=[50, 100, 500, 1000, 2000, 3000, 5000, 9000, 9999]
    aria_batch_size=[9000]
    num_wh=[64,32,16,8]
    # num_wh=[128,64,32,16,8]
    fmt = ["WORKLOAD","ARIA_BATCH_SIZE","NUM_WH","CC_ALG"]
    exp = [[wl,bs,wh*nnodes,algo] for algo,wh,bs in itertools.product(algos,num_wh,aria_batch_size)]
    return fmt,exp

def tpcc_aria_batch2():
    wl = 'TPCC'
    algos=['ARIA']
    aria_batch_size=[16,100,500,2000]
    mpr=[0,0.2,0.4,0.6,0.8,1]
    fmt=["WORKLOAD","ARIA_BATCH_SIZE","MPR","CC_ALG"]
    exp = [[wl,bs,m,algo] for algo,m,bs in itertools.product(algos,mpr,aria_batch_size)]
    return fmt,exp

def bomb_caracal_smoke():
    """Two-node Caracal + BoMB smoke runs for static and dynamic plans."""
    fmt = ["WORKLOAD", "CC_ALG", "NODE_CNT", "CLIENT_NODE_CNT",
           "THREAD_CNT", "CLIENT_THREAD_CNT", "MAX_TXN_IN_FLIGHT",
           "ARIA_BATCH_SIZE", "BOMB_DYNAMIC_MODE", "MSG_SIZE_MAX",
           "WARMUP_TIMER", "DONE_TIMER"]
    exp = [["BOMB", "CARACAL", 2, 2, 8, 4, 10000, 16, dynamic,
            1048576, "1*BILLION", "3*BILLION"]
           for dynamic in ["false", "true"]]
    return fmt, exp

##############################
# END PLOTS
##############################

experiment_map = {
    # YCSB_WRITE
    'ycsb_aria_batch3': ycsb_aria_batch3,

    # YCSB_SKEW
    'ycsb_aria_batch': ycsb_aria_batch,
    
    # YCSB_DIST
    'ycsb_aria_batch2': ycsb_aria_batch2,
    
    # 随机等待
    'ycsb_random_idle': ycsb_random_idle, # 需要配置batch size为3k

    # ycsb batch size
    'ycsb_batch_size' : ycsb_batch_size,

    # tpcc_wh
    # 'tpcc_aria_batch': tpcc_aria_batch,
    'tpcc_wh': tpcc_wh,

    # 可扩展性
    'ycsb_scaling': ycsb_scaling,
    'tpcc_scaling': tpcc_scaling,
    
    # 下面是没跑的实验
    'ycsb_writes': ycsb_writes,
    'ycsb_skew_pip': ycsb_skew_pip,
    'ycsb_dist_ratio': ycsb_dist_ratio,
    'tpcc_aria_batch2': tpcc_aria_batch2,
    'bomb_caracal_smoke': bomb_caracal_smoke,
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
    "OPEN_RANDOM_WAIT":'false',
#YCSB
    "INIT_PARALLELISM" : 8,
    "TUP_WRITE_PERC":0.2,
    "ZIPF_THETA":0.7,
    "ACCESS_PERC":0.03,
    "DATA_PERC": 100,
    "REQ_PER_QUERY": 10,
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
    "ARIA_BATCH_SIZE":9000,
    "LOGGING":"false",
    "SCHEDULER_CNT": 3,
#BoMB
    "BOMB_DYNAMIC_MODE":"false",
    "BOMB_LONG_TX_MODE":"BOMB_LONG_TX_GLOBAL",
    "BOMB_LONG_TX_SOURCES":1,
    "BOMB_SHORT_WORKERS":3,
    "BOMB_FACTORY_COUNT":2,
    "BOMB_PRODUCT_TYPES":64,
    "BOMB_MATERIAL_TYPES":160,
    "BOMB_RAW_MATERIAL_TYPES":64,
    "BOMB_TREES_PER_PRODUCT":5,
    "BOMB_TREE_SIZE":10,
    "BOMB_RAW_MATERIALS_PER_LEAF":3,
    "BOMB_TARGET_PRODUCTS":4,
    "BOMB_TARGET_MATERIALS":1,
    "BOMB_QUERY_CACHE_SIZE":2048,
    "BOMB_FORCE_SHORT_TYPE":-1,
    "BOMB_INJECT_STALE_PRESET":"false",
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


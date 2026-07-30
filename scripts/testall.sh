# python3 run_experiments.py ycsb_skew_pip ycsb_writes ycsb_dist_ratio tpcc_wh ycsb_rwset_variable_ratio
# python3 run_experiments.py ycsb_writes
# python3 run_experiments.py ycsb_dist_ratio
# python3 run_experiments.py tpcc_wh

# python3 run_experiments.py ycsb_rwset_ratio
# python3 run_experiments.py ycsb_sch_cnt

# python3 run_experiments.py ycsb_writes ycsb_dist_ratio tpcc_wh ycsb_rwset_ratio

python3 run_experiments.py  ycsb_writes_PCC ycsb_writes_OCC \
                            ycsb_skew_PCC ycsb_skew_OCC \
                            ycsb_dist_ratio_PCC ycsb_dist_ratio_OCC \
                            ycsb_random_idle_PCC ycsb_random_idle_OCC\
                            ycsb_batch_size_PCC ycsb_batch_size_OCC\
                            tpcc_wh_PCC tpcc_wh_OCC\
                            ycsb_sch_cnt
# 还差一个SDPCC的本地水印更新优化，得跨节点跑

python3 run_experiments.py  ycsb_writes_PCC ycsb_writes_OCC \
                            ycsb_skew_PCC ycsb_skew_OCC \
                            ycsb_dist_ratio_PCC ycsb_dist_ratio_OCC \
                            ycsb_random_idle_PCC ycsb_random_idle_OCC\
                            ycsb_batch_size_PCC ycsb_batch_size_OCC\
                            tpcc_wh_PCC tpcc_wh_OCC\
                            ycsb_sch_cnt
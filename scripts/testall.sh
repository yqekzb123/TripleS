# python3 run_experiments.py ycsb_skew_pip ycsb_writes ycsb_dist_ratio tpcc_wh ycsb_rwset_variable_ratio
# python3 run_experiments.py ycsb_writes
# python3 run_experiments.py ycsb_dist_ratio
# python3 run_experiments.py tpcc_wh

# python3 run_experiments.py ycsb_rwset_ratio
# python3 run_experiments.py ycsb_sch_cnt

# python3 run_experiments.py ycsb_writes ycsb_dist_ratio tpcc_wh ycsb_rwset_ratio

python3 run_experiments.py  ycsb_writes_PCC ycsb_writes_OCC ycsb_aria_batch3 \
                            ycsb_skew_PCC ycsb_skew_OCC ycsb_aria_batch \
                            ycsb_dist_ratio_PCC ycsb_dist_ratio_OCC ycsb_aria_batch2 \
                            ycsb_random_idle_PCC ycsb_random_idle_OCC \
                            ycsb_batch_size_PCC ycsb_batch_size_OCC \
                            tpcc_wh_PCC tpcc_wh_OCC tpcc_aria_batch \
                            ycsb_sch_cnt ycsb_rwset_variable_ratio 



# 一会儿Aria的random idle，写，SDOCC的random idle。还有Caracal
# 
# 还差一个SDPCC的本地水印更新优化，得跨节点跑
python3 run_experiments.py ycsb_aria_batch3 ycsb_random_idle_OCC ycsb_random_idle_ARIA


python3 run_experiments.py ycsb_scaling_PCC ycsb_scaling_OCC ycsb_scaling_ARIA tpcc_scaling_PCC tpcc_scaling_OCC tpcc_scaling_ARIA

# python3 run_experiments.py  ycsb_writes_PCC ycsb_writes_OCC \
#                             ycsb_skew_PCC ycsb_skew_OCC \
#                             ycsb_dist_ratio_PCC ycsb_dist_ratio_OCC \
#                             ycsb_random_idle_PCC ycsb_random_idle_OCC\
#                             ycsb_batch_size_PCC ycsb_batch_size_OCC\
#                             tpcc_wh_PCC tpcc_wh_OCC\
#                             ycsb_sch_cnt
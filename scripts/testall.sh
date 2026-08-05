
# python3 run_experiments.py  ycsb_writes_PCC ycsb_writes_OCC ycsb_aria_batch3 \
#                             ycsb_skew_PCC ycsb_skew_OCC ycsb_aria_batch \
#                             ycsb_dist_ratio_PCC ycsb_dist_ratio_OCC ycsb_aria_batch2 \
#                             ycsb_random_idle_PCC ycsb_random_idle_OCC \
#                             ycsb_batch_size_PCC ycsb_batch_size_OCC \
#                             tpcc_wh_PCC tpcc_wh_OCC tpcc_aria_batch \
#                             ycsb_sch_cnt ycsb_rwset_variable_ratio \
#                             ycsb_scaling_PCC ycsb_scaling_OCC \
#                             tpcc_scaling_PCC tpcc_scaling_OCC

python3 run_experiments.py  ycsb_aria_batch3 \
                            ycsb_aria_batch \
                            ycsb_aria_batch2 \
                            ycsb_random_idle \
                            ycsb_batch_size \
                            tpcc_aria_batch 
                            # ycsb_scaling_PCC ycsb_scaling_OCC \
                            # tpcc_scaling_PCC tpcc_scaling_OCC

python3 run_experiments.py ycsb_scaling tpcc_scaling
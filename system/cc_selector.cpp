#include "global.h"
#include "cc_selector.h"
#include "mem_alloc.h"
#include "msg_queue.h"
#include "ycsb_query.h"
#include "tpcc_helper.h"
#include "tpcc.h"
#include "tpcc_query.h"

void CCSelector::init(){
    pstats=new uint64_t[g_total_shard_num];
    is_high_conflict=new bool[g_total_shard_num];
    memset(pstats,0,g_total_shard_num*sizeof(uint64_t));
    memset(is_high_conflict,false,g_total_shard_num*sizeof(bool));
}
CCSelector::~CCSelector(){
    delete [] pstats;
    delete [] is_high_conflict;
}
#if CC_ALG == SNAPPER
int CCSelector::get_best_cc(Message *msg){
    if(msg->rtype == CL_QRY){
        double r = (double)(rand() % 10000) / 10000;
        if (r < g_prorate_ratio) {
            return WAIT_DIE;
        }
    } else {
        return WAIT_DIE;
    }
    return CALVIN;
}
#else
int CCSelector::get_best_cc(Message *msg){
// Prorate transactions to Silo as non-deterministic workload
    if(msg->rtype == CL_QRY){
        double r = (double)(rand() % 10000) / 10000;
        if (r < g_prorate_ratio) {
            return SILO;
        }
    }
#if WORKLOAD == YCSB
    auto req = ((YCSBClientQueryMessage*)msg)->requests;
    for(uint64_t i = 0; i < req.size(); i++){
        uint64_t shard = key_to_shard(req[i]->key);
        if((shard % g_node_cnt != g_node_id) || is_high_conflict[shard]){
        // if((shard % g_node_cnt != g_node_id)){
        // if (is_high_conflict[shard]) {
            // txn that accesses multi partition or high conflict shard, use CALVIN
            return CALVIN;
        }
    }
    return SILO;//txn that accesses single partition as well as low conflict shard, use SILO
#elif WORKLOAD == TPCC
    auto tpcc_msg = (TPCCClientQueryMessage*)msg;
    uint64_t w_id = tpcc_msg->w_id;
	uint64_t d_id = tpcc_msg->d_id;
	uint64_t c_id = tpcc_msg->c_id;
	uint64_t d_w_id = tpcc_msg->d_w_id;
	uint64_t c_w_id = tpcc_msg->c_w_id;
	uint64_t c_d_id = tpcc_msg->c_d_id;
	char * c_last = tpcc_msg->c_last;
	uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
    uint64_t key, shard;
	switch(tpcc_msg->txn_type) {
		case TPCC_PAYMENT:
            if(GET_NODE_ID(part_id_w) != g_node_id || GET_NODE_ID(part_id_c_w) != g_node_id){
                return CALVIN;
            }
            // WH
            key = w_id;
            key += TPCCTableKey::WAREHOUSE_OFFSET;
            shard = key_to_shard(key);
            if(is_high_conflict[shard]){
                return CALVIN;
            }
            // Dist
            key = distKey(d_id, d_w_id);
            key += TPCCTableKey::DISTRICT_OFFSET;
            shard = key_to_shard(key);
            if(is_high_conflict[shard]){
                return CALVIN;
            }
			// Cust
            if (tpcc_msg->by_last_name) {
                key = custNPKey(c_last, c_d_id, c_w_id);
                key += TPCCTableKey::CUST_BY_NAME_OFFSET;
                shard = key_to_shard(key);
                if(is_high_conflict[shard]){
                    return CALVIN;
                }
            } else {
                key = custKey(c_id, c_d_id, c_w_id);
                key += TPCCTableKey::CUST_BY_ID_OFFSET;
                shard = key_to_shard(key);
                if(is_high_conflict[shard]){
                    return CALVIN;
                }
            }
			break;
		case TPCC_NEW_ORDER:
            if(GET_NODE_ID(part_id_w) != g_node_id){
                return CALVIN;
            }
            // WH
            key = w_id;
            key += TPCCTableKey::WAREHOUSE_OFFSET;
            shard = key_to_shard(key);
            if(is_high_conflict[shard]){
                return CALVIN;
            }
            // Cust
            key = custKey(c_id, d_id, w_id);
            key += TPCCTableKey::CUST_BY_ID_OFFSET;
            shard = key_to_shard(key);
            if(is_high_conflict[shard]){
                return CALVIN;
            }
            // Dist
            key = distKey(d_id, w_id);
            key += TPCCTableKey::DISTRICT_OFFSET;
            shard = key_to_shard(key);
            if(is_high_conflict[shard]){
                return CALVIN;
            }
            // Items
            for(uint64_t i = 0; i < tpcc_msg->ol_cnt; i++) {
                if(GET_NODE_ID(wh_to_part(tpcc_msg->items[i]->ol_supply_w_id)) != g_node_id){
                    return CALVIN;
                }
                // item
                key = tpcc_msg->items[i]->ol_i_id;
                key += TPCCTableKey::ITEM_OFFSET;
                shard = key_to_shard(key);
                if(is_high_conflict[shard]){
                    return CALVIN;
                }
                // stock
                key = stockKey(tpcc_msg->items[i]->ol_i_id, tpcc_msg->items[i]->ol_supply_w_id);
                key += TPCCTableKey::STOCK_OFFSET;
                shard = key_to_shard(key);
                if(is_high_conflict[shard]){
                    return CALVIN;
                }
            }
			break;
        case TPCC_ORDER_STATUS:
        case TPCC_DELIVERY:
            return SILO;
        case TPCC_STOCK_LEVEL:
            // Dist
            key = distKey(d_id, w_id);
            key += TPCCTableKey::DISTRICT_OFFSET;
            shard = key_to_shard(key);
            if(is_high_conflict[shard]){
                return CALVIN;
            }
            break;
		default:
			assert(false);
	}
    return SILO;
#endif
}
#endif

#if WORKLOAD == YCSB
void CCSelector::update_conflict_stats(row_t * row){
    uint64_t shard = key_to_shard(row->get_primary_key());
    ATOM_ADD(pstats[shard], 1);
}
#elif WORKLOAD == TPCC
void CCSelector::update_conflict_stats(TPCCQuery *query, row_t * row){
    auto table_name = row->get_table_name();
    uint64_t key = row->get_primary_key();
    if(strcmp(table_name, "WAREHOUSE") == 0){
        key += TPCCTableKey::WAREHOUSE_OFFSET;
    }else if(strcmp(table_name, "DISTRICT") == 0){
        key += TPCCTableKey::DISTRICT_OFFSET;
    }else if(strcmp(table_name, "ITEM") == 0){
        key += TPCCTableKey::ITEM_OFFSET;
    }else if(strcmp(table_name, "STOCK") == 0){
        key += TPCCTableKey::STOCK_OFFSET;
    }else if(strcmp(table_name, "CUSTOMER") == 0){
        // only PAYMENT txns will index table customer by name
        // in NEW_ORDER txn, by_last_name is meaningless and maybe is not false
        if(query->txn_type == TPCC_PAYMENT && query->by_last_name){
            key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
            key += TPCCTableKey::CUST_BY_NAME_OFFSET;
        }else{
            key += TPCCTableKey::CUST_BY_ID_OFFSET;
        }
    }else{
        return;
    //     assert(false);
    }
    uint64_t shard = key_to_shard(key);
    ATOM_ADD(pstats[shard], 1);
}
#endif

void CCSelector::update_ccselector(){
#if WORKLOAD == TPCC
    // Scaling is needed for customer table
    uint64_t start_shard, end_shard;
    start_shard = key_to_shard(TPCCTableKey::CUST_BY_ID_START + TPCCTableKey::CUST_BY_ID_OFFSET);
    end_shard = key_to_shard(TPCCTableKey::CUST_BY_ID_END + TPCCTableKey::CUST_BY_ID_OFFSET);
    for(uint64_t i = start_shard; i < end_shard; i++){
        //  for table Customer, 60% cases we use index Customer_by_ID, so we need some scaling up to restore true conflict value
        pstats[i] = pstats[i] / 0.6;
    }
    start_shard = key_to_shard(TPCCTableKey::CUST_BY_NAME_START+TPCCTableKey::CUST_BY_NAME_OFFSET);
    end_shard = key_to_shard(TPCCTableKey::CUST_BY_NAME_END+TPCCTableKey::CUST_BY_NAME_OFFSET);
    for(uint64_t i = start_shard; i < end_shard; i++){
        //  for table Customer, 40% cases we use index Customer_by_NAME, so we need some scaling up to restore true conflict value
        pstats[i] = pstats[i] / 0.4;
    }
#endif
    //  i equals to shard_number_in_node, refer to key_to_shard for more information
    //  only update shards that physically stored in this node which is specified by g_node_id
    for(uint64_t i=0;;i++){
        uint64_t shard=i*g_node_cnt+g_node_id;
        if(shard>=g_total_shard_num){
            break;
        }
        if(pstats[shard]<g_lower_bound){
            is_high_conflict[shard]=false;
        }else if(pstats[shard]>g_upper_bound){
            is_high_conflict[shard]=true;
        }
    }
    memset(pstats,0,g_total_shard_num*sizeof(uint64_t));
}
Message* CCSelector::pack_msg(){
    Message *msg=Message::create_message(CONF_STAT);//initialization of conflict_statistics finishes inside this function
    for(uint64_t i=0;i<g_total_shard_num;i++){
        ((ConflictStaticsMessage*)msg)->conflict_statics.add(is_high_conflict[i]);
    }
    return msg;
}
void CCSelector::process_conflict_msg(ConflictStaticsMessage *msg){
    uint64_t node_num=msg->get_return_id();
    //  i equals to shard_number_in_node, refer to key_to_shard for more information
    //  only update shards that physically stored in other node which is specified by node_num in upper row
    for(uint64_t i=0;;i++){
        uint64_t shard=i*g_node_cnt+node_num;
        if(shard>=g_total_shard_num){
            break;
        }
        is_high_conflict[shard]=msg->conflict_statics[shard];
    }
    msg->release();//msg life ends here
}

uint64_t CCSelector::get_total_conflict() {
    uint64_t sum = 0;
    // all shard stats
    for (uint64_t i = 0; i < g_total_shard_num; i++) {
        sum += pstats[i];
    }
    return sum;
}

uint64_t CCSelector::get_highest_conflict() {
    return pstats[g_node_id];
}

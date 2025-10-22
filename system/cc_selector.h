#ifndef _CC_SELECTOR_H_
#define _CC_SELECTOR_H_

#include "global.h"
#include "message.h"
#include "helper.h"
#include "tpcc_helper.h"
#include "tpcc_query.h"

#if WORKLOAD == TPCC
namespace TPCCTableKey{
    //  key range
    const uint64_t  WAREHOUSE_START     =   1,
                    WAREHOUSE_END       =   g_num_wh,
                    DISTRICT_START      =   distKey(1,1),
                    DISTRICT_END        =   distKey(g_dist_per_wh,g_num_wh),
                    CUST_BY_ID_START    =   custKey(1,1,1),
                    CUST_BY_ID_END      =   custKey(g_cust_per_dist,g_dist_per_wh,g_num_wh),
                    CUST_BY_NAME_START  =   custNPKey((char*)"BARBARBAR",1,1),
                    CUST_BY_NAME_END    =   custNPKey((char*)"OUGHTOUGHTOUGHT",g_dist_per_wh,g_num_wh),
                    STOCK_START         =   stockKey(1,1),
                    STOCK_END           =   stockKey(g_max_items, g_num_wh),
                    ITEM_START          =   1,
                    ITEM_END            =   g_max_items;
    //  key offset
    //  our code involves 5 tables in TPCC, that is, Warehouse, District, Item, Stock, Customer
    //  but we have two methods for indexing table Customer and these tables' key range overlaps with each other, sometimes
    //  to make these tables a whole big table(virtually)
    //  we need to apply necessary key offset and make table Customer two virtual tables
    //  the order in which these 5 tables make up the big virtual table is, Warehouse, District, Item, Stock, Customer
    const uint64_t  WAREHOUSE_OFFSET    =   0,
                    DISTRICT_OFFSET     =   WAREHOUSE_END - WAREHOUSE_START + 1 + WAREHOUSE_OFFSET,
                    ITEM_OFFSET         =   DISTRICT_END - DISTRICT_START + 1 + DISTRICT_OFFSET,
                    STOCK_OFFSET        =   ITEM_END - ITEM_START + 1 + ITEM_OFFSET,
                    CUST_BY_ID_OFFSET   =   STOCK_END - STOCK_START + 1 + STOCK_OFFSET,
                    CUST_BY_NAME_OFFSET =   CUST_BY_ID_END - CUST_BY_ID_START + 1 + CUST_BY_ID_OFFSET;
}
#endif

class CCSelector {
public:
    ~CCSelector();
    void init();
    int get_best_cc(Message *msg);//for a txn, pick optimal concurrency control
#if WORKLOAD == YCSB
    void update_conflict_stats(row_t * row);
#elif WORKLOAD == TPCC
    void update_conflict_stats(TPCCQuery * query, row_t * row);
#endif
    void update_ccselector();
    Message* pack_msg();
    void process_conflict_msg(ConflictStaticsMessage *msg);
    uint64_t get_total_conflict();
    uint64_t get_highest_conflict();
    uint64_t *pstats;
private:
    bool *is_high_conflict;
};

#endif

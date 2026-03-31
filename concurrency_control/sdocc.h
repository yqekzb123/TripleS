/* 
   版权所有 (c) 2026 山东大学
   许可协议: Apache License, Version 2.0
   你可以在遵循许可协议的前提下使用和修改本文件。
*/

#ifndef SDOCC_H
#define SDOCC_H

#if CC_ALG == SDOCC
#include "txn.h"
#include "row.h"
extern std::string get_sdocc_phase_str(SDOCC_PHASE phase);
extern void update_local_watermark(uint64_t thd_id, TxnManager * txn_manager);
#endif
#endif // SDOCC_H
/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _ARIA_H_
#define _ARIA_H_

#include "txn.h"
#include "row.h"
#include "row_aria.h"

#if LONG_TXN_SCHEDULE
// Functions implemented in concurrency_control/aria.cpp when LONG_TXN_SCHEDULE is enabled.
// Declare with external linkage to avoid duplicate static definitions across translation units.
// extern bool update_aria_sid(uint64_t thd_id, uint64_t key, uint64_t*& sids, uint64_t& min_sid, ARIA_PHASE phase);
extern void txn_next_aria_phase(uint64_t thd_id, ARIA_PHASE& current_phase, TxnManager * txn_manager);
#endif
std::string get_aria_phase_str(ARIA_PHASE phase);
#endif

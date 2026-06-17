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

#include "work_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "message.h"
#include "client_query.h"
#include <functional>
#include "txn.h"
#include "water_mark.h"
#include <boost/lockfree/queue.hpp>
#include "circle_list.h"
#include "ordered_list.h"

void QWorkQueue::init() {

	last_sched_dq = NULL;
	sched_ptr = 0;
	seq_queue = new boost::lockfree::queue<work_queue_entry* > (0);
	work_queue = new boost::lockfree::queue<work_queue_entry* > (0);
	new_txn_queue = new boost::lockfree::queue<work_queue_entry* >(0);

#if CC_ALG == ARIA
	aria_read_queue = new boost::lockfree::queue<work_queue_entry* >(0);
	aria_reserve_queue = new boost::lockfree::queue<work_queue_entry* >(0);
	aria_check_queue = new boost::lockfree::queue<work_queue_entry* >(0);
	aria_commit_queue = new boost::lockfree::queue<work_queue_entry* >(0);
#endif
#if CC_ALG == SDOCC// || CC_ALG == SILO
	sdocc_queue = new boost::lockfree::queue<work_queue_entry* > (0);
#endif
	sched_queue = new boost::lockfree::queue<work_queue_entry* > * [g_node_cnt];
	for ( uint64_t i = 0; i < g_node_cnt; i++) {
		sched_queue[i] = new boost::lockfree::queue<work_queue_entry* > (0);
	}

	txn_queue_size = 0;
	work_queue_size = 0;

	work_enqueue_size = 0;
	work_dequeue_size = 0;
	txn_enqueue_size = 0;
	txn_dequeue_size = 0;

	#if CC_ALG == SDOCC
		sdocc_ready = true;
		sdocc_lockfree = new TxnMsgLockList("SdoccList");
	#endif
	#if CC_ALG == SDPCC
		sched_ready = true;
		sdpcc_scheduled_list_lockfree = new TxnMsgLockList("SdpccScheduledList");
		sdpcc_list = new CircleList(g_inflight_max,g_node_cnt);
	#endif
	#if CC_ALG == ARIA
		read_ready = true;
		aria_read_lockfree = new TxnMsgLockList("AriaReadList");
		reserve_ready = true;
		aria_reserve_lockfree = new TxnMsgLockList("AriaReserveList");
		check_ready = true;
		aria_check_lockfree = new TxnMsgLockList("AriaCheckList");
		commit_ready = true;
		aria_commit_lockfree = new TxnMsgLockList("AriaCommitList");
	#endif
	#if CC_ALG == CARACAL
		caracal_init_queue = new boost::lockfree::queue<work_queue_entry* >(0);
		caracal_execute_queues = new CaracalQueue[g_thread_cnt];
		caracal_execute_queues_mutex = new pthread_mutex_t;

		caracal_ack_queue = new boost::lockfree::queue<work_queue_entry* >(0);
	#endif


	sem_init(&_semaphore, 0, 1);
	top_element=NULL;
}

void QWorkQueue::sequencer_enqueue(uint64_t thd_id, Message * msg) {
	uint64_t starttime = get_sys_clock();
	assert(msg);
	DEBUG_M("SeqQueue::enqueue work_queue_entry alloc\n");
	work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
	entry->msg = msg;
	entry->rtype = msg->rtype;
	entry->txn_id = msg->txn_id;
	entry->batch_id = msg->batch_id;
	entry->starttime = get_sys_clock();
	assert(ISSERVER);

	DEBUG("Seq Enqueue (%ld,%ld)\n",entry->batch_id,entry->txn_id);
		while (!seq_queue->push(entry) && !simulation->is_done()) {
		}

	INC_STATS(thd_id,seq_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,seq_queue_enq_cnt,1);

}

Message * QWorkQueue::sequencer_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(ISSERVER);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	bool valid = seq_queue->pop(entry);

	if(valid) {
		msg = entry->msg;
		assert(msg);
		DEBUG("Seq Dequeue (%ld,%ld)\n",entry->batch_id,entry->txn_id);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,seq_queue_wait_time,queue_time);
		INC_STATS(thd_id,seq_queue_cnt,1);
		// DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d,
		// 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
		DEBUG_M("SeqQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,seq_queue_dequeue_time,get_sys_clock() - starttime);
	}

	return msg;

}

#if CC_ALG == ARIA 
Message* QWorkQueue::txn_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(CC_ALG == ARIA);
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	bool valid = false;

	valid = new_txn_queue->pop(entry);
	if(valid) {
		msg = entry->msg;
		assert(msg);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		statqueue(thd_id, entry);
		if(msg->rtype == CL_QRY) {
			sem_wait(&_semaphore);
			txn_queue_size --;
			txn_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			assert(false);
		}
		msg->wq_time = queue_time;
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->batch_id,entry->txn_id);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
	}
	return msg;
}
void QWorkQueue::work_enqueue(uint64_t thd_id, Message* msg, bool not_ready, ARIA_PHASE phase) {
	uint64_t starttime = get_sys_clock();
	assert(CC_ALG == ARIA);
	assert(msg);
	DEBUG_M("QWorkQueue::enqueue work_queue_entry alloc\n");
	work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
	entry->msg = msg;
	entry->rtype = msg->rtype;
	entry->txn_id = msg->txn_id;
	entry->batch_id = msg->batch_id;
	entry->starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	DEBUG("Work Enqueue (%ld,%ld) %s\n",entry->batch_id,entry->txn_id,entry->get_message_name().c_str());

	assert(msg->rtype == CL_QRY);

	if(not_ready) {
		INC_STATS(thd_id,work_queue_conflict_cnt,1);
	}
	switch (phase) {
	case ARIA_READ:
		// printf("thd_id: %ld add txn: %ld to read queue\n", thd_id, msg->txn_id);
		while (!aria_read_queue->push(entry) && !simulation->is_done()) {}
		break;
	case ARIA_RESERVATION:
		// printf("thd_id: %ld add txn: %ld to reserve queue\n", thd_id, msg->txn_id);
		while (!aria_reserve_queue->push(entry) && !simulation->is_done()) {}
		break;
	case ARIA_CHECK:
		// printf("thd_id: %ld add txn: %ld to check queue\n", thd_id, msg->txn_id);
		while (!aria_check_queue->push(entry) && !simulation->is_done()) {}
		break;
	case ARIA_COMMIT:
		// printf("thd_id: %ld add txn: %ld to commit queue\n", thd_id, msg->txn_id);
		while (!aria_commit_queue->push(entry) && !simulation->is_done()) {}
		break;
	default:
		assert(false);
		break;
	}
	sem_wait(&_semaphore);
	work_queue_size ++;
	work_enqueue_size ++;
	sem_post(&_semaphore);

	INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,work_queue_enq_cnt,1);
	INC_STATS(thd_id,trans_work_queue_item_total,txn_queue_size+work_queue_size);
}

Message* QWorkQueue::work_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(CC_ALG == ARIA);
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	bool valid = false;
	// 加一个随机数，50%概率去取work_queue，25%概率去取read_queue，25%概率去取reserve_queue，check_queue和commit_queue暂时不考虑，之后可以根据实际情况调整。
	int rand_num = rand() % 100;
	if (rand_num < 50) {
		valid = work_queue->pop(entry);
	}
	if (!valid) {
		switch (simulation->aria_phase)
		{
		case ARIA_READ:
			valid = aria_read_queue->pop(entry);
			if (valid) {
				// printf("thd_id: %ld pop txn: %ld from read queue\n", thd_id, entry->msg->txn_id);
			}
			break;
		case ARIA_RESERVATION:
			valid = aria_reserve_queue->pop(entry);
			if (valid) {
				// printf("thd_id: %ld pop txn: %ld from reserve queue\n", thd_id, entry->msg->txn_id);
			}
			break;
		case ARIA_CHECK:
			valid = aria_check_queue->pop(entry);
			if (valid) {
				// printf("thd_id: %ld pop txn: %ld from check queue\n", thd_id, entry->msg->txn_id);
			}
			break;
		case ARIA_COMMIT:
			valid = aria_commit_queue->pop(entry);
			if (valid) {
				// printf("thd_id: %ld pop txn: %ld from commit queue\n", thd_id, entry->msg->txn_id);
			}
			break;
		default:
			break;
		}
	}

	if(valid) {
		msg = entry->msg;
		assert(msg);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		statqueue(thd_id, entry);
		if(msg->rtype == CL_QRY) {
			sem_wait(&_semaphore);
			work_queue_size ++;
			work_enqueue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			// printf("recieve msg type: %d\n", msg->rtype);
			sem_wait(&_semaphore);
			work_queue_size ++;
			work_enqueue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_old_cnt,1);
		}
		msg->wq_time = queue_time;
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->batch_id,entry->txn_id);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
	}
	return msg;
}
#endif // CC_ALG == ARIA

void QWorkQueue::sched_enqueue(uint64_t thd_id, Message * msg) {
	assert(CC_ALG == CALVIN || CC_ALG == SDPCC);
	assert(msg);
	assert(ISSERVERN(msg->return_node_id));
	uint64_t starttime = get_sys_clock();

	DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry alloc\n");
	work_queue_entry * entry = (work_queue_entry*)mem_allocator.alloc(sizeof(work_queue_entry));
	entry->msg = msg;
	entry->rtype = msg->rtype;
	entry->txn_id = msg->txn_id;
	entry->batch_id = msg->batch_id;
	entry->starttime = get_sys_clock();

	DEBUG("Sched Enqueue (%ld,%ld)\n",entry->batch_id,entry->txn_id);
	uint64_t mtx_time_start = get_sys_clock();
	while (!sched_queue[msg->get_return_id()]->push(entry) && !simulation->is_done()) {
	}
	INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_time_start);

	INC_STATS(thd_id,sched_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,sched_queue_enq_cnt,1);
}

Message * QWorkQueue::sched_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();

	assert(CC_ALG == CALVIN || CC_ALG == SDPCC);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;

	// 暂时先通过封锁来做吧，之后考虑把sched_queue合成一个，在IOThread中处理顺序问题。
	bool valid = sched_queue[sched_ptr]->pop(entry);

	if(valid) {

		msg = entry->msg;
		DEBUG("Sched Dequeue (%ld,%ld)\n",entry->batch_id,entry->txn_id);

		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,sched_queue_wait_time,queue_time);
		INC_STATS(thd_id,sched_queue_cnt,1);

		DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));

		if(msg->rtype == RDONE) {
			// Advance to next queue or next epoch
			DEBUG("Sched RDONE %ld %ld\n",sched_ptr,simulation->get_worker_epoch());
			assert(msg->get_batch_id() == simulation->get_worker_epoch());
			if(sched_ptr == g_node_cnt - 1) {
				INC_STATS(thd_id,sched_epoch_cnt,1);
				INC_STATS(thd_id,sched_epoch_diff,get_sys_clock()-simulation->last_worker_epoch_time);
				simulation->next_worker_epoch();
			}
			sched_ptr = (sched_ptr + 1) % g_node_cnt;
			msg->release();
			msg = NULL;

		} else {
			simulation->inc_epoch_txn_cnt();
			DEBUG("Sched msg dequeue %ld (%ld,%ld) %ld\n", sched_ptr, msg->batch_id, msg->txn_id, 
						simulation->get_worker_epoch());
			assert(msg->batch_id == simulation->get_worker_epoch());
		}

		INC_STATS(thd_id,sched_queue_dequeue_time,get_sys_clock() - starttime);
	}

	return msg;
}

void QWorkQueue::enqueue(uint64_t thd_id, Message * msg, bool busy) {
	uint64_t starttime = get_sys_clock();
	assert(msg);
	DEBUG_M("QWorkQueue::enqueue work_queue_entry alloc\n");
	work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
	entry->msg = msg;
	entry->rtype = msg->rtype;
	entry->txn_id = msg->txn_id;
	entry->batch_id = msg->batch_id;
	entry->starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	assert(msg->rtype != WATERMARK);
	DEBUG("Work Enqueue (%ld,%ld) %s\n",entry->batch_id,entry->txn_id, entry->get_message_name().c_str());

	uint64_t mtx_wait_starttime = get_sys_clock();
	if(msg->rtype == CL_QRY) {
		while (!new_txn_queue->push(entry) && !simulation->is_done()) {
		}
		sem_wait(&_semaphore);
		txn_queue_size ++;
		txn_enqueue_size ++;
		sem_post(&_semaphore);
	} else {
		while (!work_queue->push(entry) && !simulation->is_done()) {
		}
		sem_wait(&_semaphore);
		work_queue_size ++;
		work_enqueue_size ++;
		sem_post(&_semaphore);
	}
	INC_STATS(thd_id,mtx[13],get_sys_clock() - mtx_wait_starttime);

	if(busy) {
		INC_STATS(thd_id,work_queue_conflict_cnt,1);
	}
	INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,work_queue_enq_cnt,1);
	INC_STATS(thd_id,trans_work_queue_item_total,txn_queue_size+work_queue_size);
}

void QWorkQueue::statqueue(uint64_t thd_id, work_queue_entry * entry) {
	Message *msg = entry->msg;
	if (msg->rtype == RTXN_CONT ||
		msg->rtype == RQRY_RSP || msg->rtype == RACK_PREP  ||
		msg->rtype == RACK_FIN || msg->rtype == RTXN  ||
		msg->rtype == CL_RSP) {
		uint64_t queue_time = get_sys_clock() - entry->starttime;
			INC_STATS(thd_id,trans_work_local_wait,queue_time);
	} else if (msg->rtype == RQRY || msg->rtype == RQRY_CONT ||
				msg->rtype == RFIN || msg->rtype == RPREPARE ||
				msg->rtype == RFWD){
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,trans_work_remote_wait,queue_time);
	}else if (msg->rtype == CL_QRY) {
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,trans_get_client_wait,queue_time);
	}
}

Message * QWorkQueue::dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	uint64_t mtx_wait_starttime = get_sys_clock();
	bool valid = false;

#ifdef THD_ID_QUEUE
	if (thd_id < THREAD_CNT / 2)
		valid = work_queue->pop(entry);
	else
		valid = new_txn_queue->pop(entry);
#else
	double x = (double)(rand() % 10000) / 10000;
	if (x > TXN_QUEUE_PERCENT)
		valid = work_queue->pop(entry);
	else
		valid = new_txn_queue->pop(entry);
	if(!valid) {
#if SERVER_GENERATE_QUERIES
		if(ISSERVER) {
			BaseQuery * m_query = client_query_queue.get_next_query(thd_id,thd_id);
			if(m_query) {
				assert(m_query);
				msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
			}
		}
#else
		if (x > TXN_QUEUE_PERCENT)
			valid = new_txn_queue->pop(entry);
		else
			valid = work_queue->pop(entry);
		// if ((thd_id % THREAD_CNT) % 2 == 0)
			// valid = new_txn_queue->pop(entry);
		// else
		// 	valid = work_queue->pop(entry);
#endif
	}
#endif
	INC_STATS(thd_id,mtx[14],get_sys_clock() - mtx_wait_starttime);

	if(valid) {
		msg = entry->msg;
		assert(msg);
		//printf("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
    	statqueue(thd_id, entry);
		if(msg->rtype == CL_QRY) {
			sem_wait(&_semaphore);
			txn_queue_size --;
			txn_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			sem_wait(&_semaphore);
			txn_queue_size --;
			txn_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_old_cnt,1);
		}
		msg->wq_time = queue_time;
		// DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d,
		// 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->batch_id,entry->txn_id);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
	}

#if SERVER_GENERATE_QUERIES
	if(msg && msg->rtype == CL_QRY) {
		INC_STATS(thd_id,work_queue_new_wait_time,get_sys_clock() - starttime);
		INC_STATS(thd_id,work_queue_new_cnt,1);
	}
#endif
	return msg;
}

// 下面是 PIPLINE相关的代码
// 最基本的lock-free list插入函数，需要指定线程号，插入的链表，还有插入的消息
void QWorkQueue::insert_list_lockfree(uint64_t thd_id, 
									  TxnMsgLockList * list, 
									  Message * msg, TxnManager * txn) {
	uint64_t key;
	if (msg) {
		key = get_batch_key(msg->get_batch_id(), msg->get_return_id(), msg->get_txn_id());
	} else {
		key = get_batch_key(txn->get_batch_id(), txn->return_id, txn->get_txn_id());
	}
	list_node_entry * entry = (list_node_entry*)mem_allocator.align_alloc(sizeof(list_node_entry));
	entry->key = key;
	if (txn) {
		entry->txn = txn;
		entry->entry_type = ENTRY_TYPE::TYPE_TXN;
		txn->scheduled_entry = entry;
	} else {
		entry->entry_type = ENTRY_TYPE::TYPE_MSG;
		entry->txn = NULL;
	}
	entry->msg = msg;

	list->insert(entry, thd_id);
}

#if CC_ALG == SDOCC// || CC_ALG == SILO
Message * QWorkQueue::sdocc_sequencer_dequeue(uint64_t thd_id) {
	Message * msg = sequencer_dequeue(thd_id);
	if(msg) {
		return msg;
	} else {
		return txn_dequeue(thd_id);
	}
}

Message* QWorkQueue::txn_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(CC_ALG == SDOCC || CC_ALG == SILO);
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	bool valid = false;

	valid = new_txn_queue->pop(entry);
	if(valid) {
		msg = entry->msg;
		assert(msg);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		statqueue(thd_id, entry);
		if(msg->rtype == CL_QRY) {
			sem_wait(&_semaphore);
			txn_queue_size --;
			txn_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			assert(false);
		}
		msg->wq_time = queue_time;
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->batch_id,entry->txn_id);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
	}
	return msg;
}

void QWorkQueue::sdocc_enqueue(uint64_t thd_id, Message* msg, bool not_ready) {
	uint64_t starttime = get_sys_clock();
	assert(msg);
	DEBUG_M("QWorkQueue::enqueue work_queue_entry alloc\n");
	work_queue_entry * entry;

	entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
	entry->msg = msg;
	entry->rtype = msg->rtype;
	entry->txn_id = msg->txn_id;
	entry->batch_id = msg->batch_id;
	entry->starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	// DEBUG("Work Enqueue (%ld,%ld) %d\n",entry->batch_id,entry->txn_id,entry->rtype);

	assert(msg->rtype == CL_QRY);

	if(not_ready) {
		INC_STATS(thd_id,work_queue_conflict_cnt,1);
	}
	while (!sdocc_queue->push(entry) && !simulation->is_done()) {}
	
	sem_wait(&_semaphore);
	work_queue_size ++;
	work_enqueue_size ++;
	sem_post(&_semaphore);

	INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,work_queue_enq_cnt,1);
	INC_STATS(thd_id,trans_work_queue_item_total,txn_queue_size+work_queue_size);
}

Message* QWorkQueue::sdocc_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	// assert(CC_ALG == SDOCC);
	assert(ISSERVER || ISREPLICA);
	work_queue_entry * entry = NULL;
	bool valid = false;
	Message * msg = NULL;
	// uint64_t dequeue_starttime = 0;
	enum SDOCC_QUEUE_TYPE {WORK_QUEUE, SDOCC_QUEUE, SDOCC_LIST};
	SDOCC_QUEUE_TYPE queue_type = WORK_QUEUE;
	
	// 先从work_queue里pop，如果有的话就直接返回，没有的话再从sdocc_queue里pop
	valid = work_queue->pop(entry);
	if (!valid) {
		valid = sdocc_queue->pop(entry);
		queue_type = SDOCC_QUEUE;
	}
	if (valid) {
		msg = entry->msg;
		assert(msg);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		statqueue(thd_id, entry);
		if(msg->rtype == CL_QRY) {
			sem_wait(&_semaphore);
			work_queue_size --;
			work_enqueue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			sem_wait(&_semaphore);
			work_queue_size --;
			work_enqueue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_old_cnt,1);
		}
		// assert(msg->txn_id != (uint64_t)-1);
		msg->wq_time = queue_time;
		DEBUG("Work Dequeue (%ld,%ld) from %d\n",entry->batch_id,entry->txn_id,queue_type);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
		return msg;
	} else {
		queue_type = SDOCC_LIST;
		// 最后从sdocc_list_lockfree里取，如果有的话就返回
		// TxnManager * txn = get_from_sdocc_list_lockfree(thd_id);
		// // INC_STATS(thd_id,small_lock_queue_dequeue_time,get_sys_clock() - dequeue_starttime);
		// if (txn) {
		// 	msg = txn->last_msg;
		// 	assert(msg);
		// 	DEBUG_WRK("[SDOCC] thd %ld dequeue txn %p with msg %p-%ld,%ld from sdocc_list_lockfree\n", thd_id, txn, msg, msg->batch_id, msg->txn_id);
		// 	return msg;
		// }
	}
	return msg;
}

void QWorkQueue::insert_sdocc_list_lockfree(uint64_t thd_id, TxnManager * txn) {
	insert_list_lockfree(thd_id, sdocc_lockfree, nullptr, txn);
	return;
}
TxnManager * QWorkQueue::get_from_sdocc_list_lockfree(uint64_t thd_id) {
	// 第一个函数
	std::function<bool(list_node_entry*)> func = [](list_node_entry * arg) -> bool {
		list_node_entry * entry = arg;
		// uint64_t minSid = UINT64_MAX;
		uint64_t minSid = check_water_mark->get_global_watermark();
		if (!entry) return false;
		if (entry->key <= minSid) {
			return true;
		}
		DEBUG_LOCKFREE("[LockFreeList] get_from_sdocc_list_lockfree cond1 skip txn %p key=%lu minSid=%lu\n",entry->txn, entry->key, minSid);
		return false;
	};

	list_node_entry * entry = NULL;

	bool succ = sdocc_lockfree->try_take(func, func, entry, thd_id);

	if (succ) {
		DEBUG("[LockFreeList] thd %ld get_from_sdocc_list_lockfree key=%lu txn=%p\n", thd_id, entry->key, entry->txn);
		TxnManager * txn = entry->txn;
		return txn;
	} else {
		return NULL;
	}
}
#endif

#if CC_ALG == SDPCC
Message * QWorkQueue::sdpcc_sched_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();

	assert(CC_ALG == SDPCC);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;

	while (!ATOM_CAS(sched_ready, true, false)) {
	}
	bool valid = sched_queue[sched_ptr]->pop(entry);

	if(valid) {
		msg = entry->msg;
		DEBUG("Sched Dequeue (%ld,%ld)\n",entry->batch_id,entry->txn_id);

		// uint64_t queue_time = get_sys_clock() - entry->starttime;
		// INC_STATS(thd_id,sched_queue_wait_time,queue_time);
		// INC_STATS(thd_id,sched_queue_cnt,1);

		if(msg->rtype == RDONE) {
			// Advance to next queue or next epoch
			DEBUG("Sched RDONE %ld %ld\n",sched_ptr,simulation->get_worker_epoch());
			assert(msg->get_batch_id() == simulation->get_worker_epoch());
			if(sched_ptr == g_node_cnt - 1) {
				INC_STATS(thd_id,sched_epoch_cnt,1);
				INC_STATS(thd_id,sched_epoch_diff,get_sys_clock()-simulation->last_worker_epoch_time);
				simulation->next_worker_epoch();
			}
			sched_ptr = (sched_ptr + 1) % g_node_cnt;
			ATOM_CAS(sched_ready, false, true);
			msg->release();
			msg = NULL;
		} else {
			assert(msg->batch_id == simulation->get_worker_epoch());
			ATOM_CAS(sched_ready, false, true);
		}

		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,sched_queue_wait_time,queue_time);
		INC_STATS(thd_id,sched_queue_cnt,1);

		DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,sched_queue_dequeue_time,get_sys_clock() - starttime);
	} else {
		ATOM_CAS(sched_ready, false, true);
	}
	return msg;
}

void QWorkQueue::insert_sdpcc_list_lockfree(uint64_t thd_id, TxnManager * txn) {
	// insert_list_lockfree(thd_id, sdpcc_scheduled_list_lockfree, nullptr, txn);
	uint64_t key = get_batch_key(txn->get_batch_id(), txn->return_id, txn->get_txn_id());
	sdpcc_list->insert(thd_id, key, txn);
	return;
}

TxnManager * QWorkQueue::get_from_sdpcc_list_lockfree(uint64_t thd_id, uint64_t &key) {
	uint64_t starttime = get_sys_clock();
	std::function<bool(circle_node_entry&)> judge_lock_watermark = [](circle_node_entry & arg) -> bool {
		assert(arg.key <= minSid && arg.txn->lock_ready_cnt <= 0);
		if (arg.key <= minSid && arg.txn->lock_ready_cnt <= 0) {
			if (arg.txn->lock_ready_cnt < 0) {
				DEBUG_LOCKFREE("[LockFreeList] get_from_sdpcc_list_lockfree txn %p lock_ready_cnt=%d\n", arg.txn, arg.txn->lock_ready_cnt);
			}
			return true;
		}
		DEBUG_LOCKFREE("[LockFreeList] get_from_sdpcc_list_lockfree cond2 skip txn %p key=%lu lock_ready_cnt=%d \n", arg.txn, arg.key, arg.txn->lock_ready_cnt);
		return false;
	};
	TxnManager* txn = nullptr;
	bool succ = sdpcc_list->try_take(judge_lock_watermark, txn, thd_id);

	if (succ) {
		// DEBUG("[LockFreeList] thd %ld get_from_sdpcc_list_lockfree key=%lu txn=%p\n", thd_id, entry->key, entry->txn);
		INC_STATS(thd_id,small_lock_get_cnt,1);
		INC_STATS(thd_id,small_lock_queue_wait_time,get_sys_clock() - starttime);
		return txn;
	} else {
		INC_STATS(thd_id,small_lock_no_get_cnt,1);
		INC_STATS(thd_id,small_lock_queue_wait_time,get_sys_clock() - starttime);
		return NULL;
	}
}
#endif



#if CC_ALG == CARACAL 
Message* QWorkQueue::txn_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(CC_ALG == CARACAL);
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	bool valid = false;

	valid = new_txn_queue->pop(entry);
	if(valid) {
		msg = entry->msg;
		assert(msg);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		statqueue(thd_id, entry);
		if(msg->rtype == CL_QRY) {
			sem_wait(&_semaphore);
			txn_queue_size --;
			txn_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			assert(false);
		}
		msg->wq_time = queue_time;
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->batch_id,entry->txn_id);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
	}
	return msg;
}

void QWorkQueue::work_enqueue(uint64_t thd_id, Message* msg, bool not_ready, CARACAL_PHASE phase) {
	uint64_t starttime = get_sys_clock();
	assert(CC_ALG == CARACAL);
	assert(msg);
	DEBUG_M("QWorkQueue::enqueue work_queue_entry alloc\n");
	work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
	entry->msg = msg;
	entry->rtype = msg->rtype;
	entry->txn_id = msg->txn_id;
	entry->batch_id = msg->batch_id;
	// ! 分布式下需要考虑return_node_id
	entry->original_return_node_id = msg->return_node_id;
	entry->starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	DEBUG("Work Enqueue (%ld,%ld) %s\n",entry->batch_id,entry->txn_id,entry->get_message_name().c_str());

	assert(msg->rtype == CL_QRY || msg->rtype == CARACAL_SUB_TXN || msg->rtype == RFWD);
	
	if(not_ready) {
		INC_STATS(thd_id,work_queue_conflict_cnt,1);
	}
	if (msg->rtype == RFWD) {
		while( !work_queue->push(entry) && !simulation->is_done()) {}
	} else {
		switch (phase) {
		case CARACAL_INIT:
			// printf("thd_id: %ld add txn: %ld to read queue\n", thd_id, msg->txn_id);
			while (!caracal_init_queue->push(entry) && !simulation->is_done()) {}
			break;
		case CARACAL_EXECUTION:
			// printf("thd_id: %ld add txn: %ld to reserve queue\n", thd_id, msg->txn_id);
			#if OPEN_SPLIT_ON_DEMAND
			pthread_mutex_lock(caracal_execute_queues_mutex);
			caracal_execute_queues[thd_id % g_thread_cnt].insert(entry);
			pthread_mutex_unlock(caracal_execute_queues_mutex);
			#else
			caracal_execute_queues[thd_id % g_thread_cnt].insert(entry);
			#endif
			break;
		default:
			assert(false);
			break;
		}
	}
	sem_wait(&_semaphore);
	work_queue_size ++;
	work_enqueue_size ++;
	sem_post(&_semaphore);

	INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,work_queue_enq_cnt,1);
	INC_STATS(thd_id,trans_work_queue_item_total,txn_queue_size+work_queue_size);
}

Message* QWorkQueue::work_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(CC_ALG == CARACAL);
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	bool valid = false;

	// 加一个百分比，让线程有一定概率直接从work_queue里pop，有一定概率根据caracal_phase从caracal_init_queue或者caracal_execute_queue里pop
	if (rand() % 100 < 50) {
		valid = work_queue->pop(entry);
	}
	
	if (!valid) {
		switch (simulation->caracal_phase)
		{
		case CARACAL_COLLECT:
		case CARACAL_INIT:
		case CARACAL_INIT_SYNC:{
			#if OPEN_SPLIT_ON_DEMAND
			uint64_t hot_thread_cnt = ceil(HOT_THREAD_PERCENT * g_thread_cnt);
			if (thd_id < hot_thread_cnt) {
				valid = false;
			} else {
				valid = caracal_init_queue->pop(entry);
				if (valid) {
					// printf("thd_id: %ld pop txn: %ld from read queue\n", thd_id, entry->msg->txn_id);
				}
			}
			#else
			valid = caracal_init_queue->pop(entry);
			if (valid) {
				// printf("thd_id: %ld pop txn: %ld from read queue\n", thd_id, entry->msg->txn_id);
			}
			#endif
			break;
		}
		case CARACAL_APPEND:
		case CARACAL_APPEND_SYNC:
			valid = caracal_init_queue->pop(entry);
			if (valid) {
				assert(false);
			}
			break;
		case CARACAL_EXECUTION:
		case CARACAL_EXECUTION_SYNC:
			valid = caracal_execute_queues[thd_id % g_thread_cnt].get_next(0, entry);
			// valid = caracal_execute_queue->pop(entry);
			if (valid) {
				// printf("thd_id: %ld pop txn: %ld from reserve queue\n", thd_id, entry->msg->txn_id);
			}
			break;
		default:
			break;
		}
	}

	if(valid) {
		msg = entry->msg;
		assert(msg);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		statqueue(thd_id, entry);
		if(msg->rtype == CL_QRY) {
			sem_wait(&_semaphore);
			work_queue_size ++;
			work_enqueue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			// printf("recieve msg type: %d\n", msg->rtype);
			sem_wait(&_semaphore);
			work_queue_size ++;
			work_enqueue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_old_cnt,1);
		}
		msg->wq_time = queue_time;
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->batch_id,entry->txn_id);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
	}
	return msg;
}

Message* QWorkQueue::phase_ack_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(CC_ALG == CARACAL);
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	bool valid = false;

	valid = caracal_ack_queue->pop(entry);

	if(valid) {
		msg = entry->msg;
		assert(msg);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		statqueue(thd_id, entry);
		
		msg->wq_time = queue_time;
		DEBUG("Phase Ack Dequeue (%ld,%ld)\n",entry->batch_id,entry->txn_id);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
	}
	return msg;
}

void QWorkQueue::phase_ack_enqueue(uint64_t thd_id, Message* msg) {
	uint64_t starttime = get_sys_clock();
	assert(CC_ALG == CARACAL);
	assert(msg);
	DEBUG_M("QWorkQueue::enqueue work_queue_entry alloc\n");
	work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
	entry->msg = msg;
	entry->rtype = msg->rtype;
	entry->txn_id = msg->txn_id;
	entry->batch_id = msg->batch_id;
	entry->starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	DEBUG("Phase Ack Enqueue (%ld,%ld) %s\n",entry->batch_id,entry->txn_id,entry->get_message_name().c_str());

	while (!caracal_ack_queue->push(entry) && !simulation->is_done()) {}
	
	sem_wait(&_semaphore);
	work_queue_size ++;
	work_enqueue_size ++;
	sem_post(&_semaphore);

	INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,work_queue_enq_cnt,1);
	INC_STATS(thd_id,trans_work_queue_item_total,txn_queue_size+work_queue_size);
}
#endif // CC_ALG == CARACAL

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
#include <boost/lockfree/queue.hpp>

void QWorkQueue::init() {

	last_sched_dq = NULL;
	sched_ptr = 0;
#ifdef NEW_WORK_QUEUE
	work_queue.set_capacity(QUEUE_CAPACITY_NEW);
	new_txn_queue.set_capacity(QUEUE_CAPACITY_NEW);
	sem_init(&mt, 0, 1);
	sem_init(&mw, 0, 1);
#else
	seq_queue = new boost::lockfree::queue<work_queue_entry* > (0);
	work_queue = new boost::lockfree::queue<work_queue_entry* > (0);
	new_txn_queue = new boost::lockfree::queue<work_queue_entry* >(0);
#if CC_ALG == ARIA
	aria_read_queue = new boost::lockfree::queue<work_queue_entry* >(0);
	aria_reserve_queue = new boost::lockfree::queue<work_queue_entry* >(0);
	aria_check_queue = new boost::lockfree::queue<work_queue_entry* >(0);
	aria_commit_queue = new boost::lockfree::queue<work_queue_entry* >(0);
#endif
	sched_queue = new boost::lockfree::queue<work_queue_entry* > * [g_node_cnt];
	for ( uint64_t i = 0; i < g_node_cnt; i++) {
		sched_queue[i] = new boost::lockfree::queue<work_queue_entry* > (0);
	}
#endif
	txn_queue_size = 0;
	work_queue_size = 0;

	work_enqueue_size = 0;
	work_dequeue_size = 0;
	txn_enqueue_size = 0;
	txn_dequeue_size = 0;
#if CC_ALG == HDCC
	calvin_txn_queue = new boost::lockfree::queue<work_queue_entry* >(0);
	calvin_work_queue = new boost::lockfree::queue<work_queue_entry* >(0);
	calvin_txn_queue_size = 0;
	calvin_txn_enqueue_size = 0;
	calvin_txn_dequeue_size = 0;
	calvin_work_queue_size = 0;
	calvin_work_enqueue_size = 0;
	calvin_work_dequeue_size = 0;
	sem_init(&_calvin_semaphore, 0, 1);
#endif

#if LONG_TXN_WORKLOAD && LONG_TXN_SCHEDULE
	// calvin_scheduled_list = new LockFreeLinkedList<TxnManager *>();
	// calvin_scheduled_list = new LockFreeLinkedList();
	sched_ready = true;

	// calvin_scheduled_list_lockfree = new LockFreeList<list_node_entry *>();

	calvin_scheduled_list_lockfree = new LockList<list_node_entry *>();
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

	DEBUG("Seq Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
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
		DEBUG("Seq Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
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
		if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
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
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
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
	DEBUG("Work Enqueue (%ld,%ld) %d\n",entry->txn_id,entry->batch_id,entry->rtype);

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

	valid = work_queue->pop(entry);
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
		if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
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
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
	}
	return msg;
}
#endif

void QWorkQueue::sched_enqueue(uint64_t thd_id, Message * msg) {
	assert(CC_ALG == CALVIN || CC_ALG == HDCC || CC_ALG == SNAPPER);
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

	DEBUG("Sched Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
	uint64_t mtx_time_start = get_sys_clock();
	while (!sched_queue[msg->get_return_id()]->push(entry) && !simulation->is_done()) {
	}
	INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_time_start);

	INC_STATS(thd_id,sched_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,sched_queue_enq_cnt,1);
}

Message * QWorkQueue::sched_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();

	assert(CC_ALG == CALVIN || CC_ALG == HDCC || CC_ALG == SNAPPER);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;

	// 暂时先通过封锁来做吧，之后考虑把sched_queue合成一个，在IOThread中处理顺序问题。
#if LONG_TXN_WORKLOAD && LONG_TXN_SCHEDULE
	while (!ATOM_CAS(sched_ready, true, false)) {
	}
	bool valid = sched_queue[sched_ptr]->pop(entry);

	if(valid) {
		msg = entry->msg;
		DEBUG("Sched Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);

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
#else
	bool valid = sched_queue[sched_ptr]->pop(entry);

	if(valid) {

		msg = entry->msg;
		DEBUG("Sched Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);

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
// #if CC_ALG != SNAPPER
			msg->release();
			msg = NULL;
// #endif

		} else {
			simulation->inc_epoch_txn_cnt();
			DEBUG("Sched msg dequeue %ld (%ld,%ld) %ld\n", sched_ptr, msg->txn_id, msg->batch_id,
						simulation->get_worker_epoch());
			assert(msg->batch_id == simulation->get_worker_epoch());
		}

		INC_STATS(thd_id,sched_queue_dequeue_time,get_sys_clock() - starttime);
	}
#endif

	return msg;
}


#ifdef NEW_WORK_QUEUE
void QWorkQueue::enqueue(uint64_t thd_id, Message * msg,bool busy) {
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
	DEBUG("Work Enqueue (%ld,%ld) %d\n",entry->txn_id,entry->batch_id,entry->rtype);

	uint64_t mtx_wait_starttime = get_sys_clock();
	if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
		// boost::unique_lck<boost::mutex> lk(mt);
		// cvt.wait(lk);
		sem_wait(&mt);
		new_txn_queue.push_back(entry);
		sem_post(&mt);
		// cvt.notify_one();
		// while(!new_txn_queue->push(entry) && !simulation->is_done()) {}
		sem_wait(&_semaphore);
		txn_queue_size ++;
		txn_enqueue_size ++;
		sem_post(&_semaphore);
	} else {
		// boost::unique_lock<boost::mutex> lk(mw);
		// cvw.wait(lk);
		sem_wait(&mw);
		work_queue.push_back(entry);
		sem_post(&mw);
		// cvw.notify_one();
		// while(!work_queue->push(entry) && !simulation->is_done()) {}
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
}

Message * QWorkQueue::dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	uint64_t mtx_wait_starttime = get_sys_clock();
	bool valid = false;
	// bool iswork = false;
	// if ((thd_id % THREAD_CNT) % 2 == 0)
	// std::unique_lock<std::mutex> lk(mw);
	// cvw.wait(lk};
	sem_wait(&mw);
	valid = work_queue.size() > 0;
	if (valid) {
		entry = work_queue[0];
		work_queue.pop_front();
		// iswork = true;
	}
	sem_post(&mw);
	// cvw.notify_one();
		// valid = work_queue->pop(entry);
	// else
	//   valid = new_txn_queue->pop(entry);
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
		// if ((thd_id % THREAD_CNT) % 2 == 0)
			// valid = new_txn_queue->pop(entry);
		// lk(mt);
		// cvt.wait(lk};
		sem_wait(&mt);
		valid = new_txn_queue.size() > 0;
		if (valid) {
			entry = new_txn_queue[0];
			new_txn_queue.pop_front();
		}
		sem_post(&mt);
		// cvt.notify_one();
		// else
		//   valid = work_queue->pop(entry);
#endif
	}
	INC_STATS(thd_id,mtx[14],get_sys_clock() - mtx_wait_starttime);

	if(valid) {
		msg = entry->msg;
		assert(msg);
		//printf("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
			sem_wait(&_semaphore);
			txn_queue_size --;
			txn_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			sem_wait(&_semaphore);
			work_queue_size --;
			work_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_old_cnt,1);
		}
		msg->wq_time = queue_time;
		//DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
		DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
		// mem_allocator.free(entry,sizeof(work_queue_entry));

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

//elioyan TODO
Message * QWorkQueue::queuetop(uint64_t thd_id)
{
	uint64_t starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	uint64_t mtx_wait_starttime = get_sys_clock();
	bool valid = false;
	sem_wait(&mw);
	valid = work_queue.size() > 0;
	if (valid) {
		entry = work_queue[0];
		work_queue.pop_front();
		// iswork = true;
	}
	sem_post(&mw);
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
		sem_wait(&mt);
		valid = new_txn_queue.size() > 0;
		if (valid) {
			entry = new_txn_queue[0];
			new_txn_queue.pop_front();
		}
		sem_post(&mt);
#endif
	}
	INC_STATS(thd_id,mtx[14],get_sys_clock() - mtx_wait_starttime);

	if(valid) {
		msg = entry->msg;
		assert(msg);
		//printf("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
			sem_wait(&_semaphore);
			txn_queue_size --;
			txn_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			sem_wait(&_semaphore);
			work_queue_size --;
			work_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_old_cnt,1);
		}
		msg->wq_time = queue_time;
		//DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
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

#else

#if CC_ALG == HDCC
void QWorkQueue::calvin_enqueue(uint64_t thd_id,Message * msg, bool busy) {
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
	DEBUG("Work Enqueue (%ld,%ld) %d\n",entry->txn_id,entry->batch_id,entry->rtype);

	uint64_t mtx_wait_starttime = get_sys_clock();
	if (msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
		while (!calvin_txn_queue->push(entry) && !simulation->is_done()) {}
		sem_wait(&_calvin_semaphore);
		calvin_txn_queue_size ++;
		calvin_txn_enqueue_size ++;
		sem_post(&_calvin_semaphore);
	} else {
		while (!calvin_work_queue->push(entry) && !simulation->is_done()) {}
		sem_wait(&_calvin_semaphore);
		calvin_work_queue_size ++;
		calvin_work_enqueue_size ++;
		sem_post(&_calvin_semaphore);
	}
	INC_STATS(thd_id,mtx[13],get_sys_clock() - mtx_wait_starttime);

	if(busy) {
		INC_STATS(thd_id,work_queue_conflict_cnt,1);
	}
	INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
	INC_STATS(thd_id,work_queue_enq_cnt,1);
	INC_STATS(thd_id,trans_work_queue_item_total,txn_queue_size+work_queue_size);
}

Message * QWorkQueue::calvin_dequeue(uint64_t thd_id) {
	uint64_t starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	uint64_t mtx_wait_starttime = get_sys_clock();
	bool valid = false;

	valid = calvin_work_queue->pop(entry);
	if(!valid) {
		valid = calvin_txn_queue->pop(entry);
	}
	INC_STATS(thd_id,mtx[14],get_sys_clock() - mtx_wait_starttime);

	if(valid) {
		msg = entry->msg;
		assert(msg);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
			sem_wait(&_calvin_semaphore);
			calvin_txn_queue_size --;
			calvin_txn_dequeue_size ++;
			sem_post(&_calvin_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			sem_wait(&_calvin_semaphore);
			calvin_work_queue_size --;
			calvin_work_dequeue_size ++;
			sem_post(&_calvin_semaphore);
			INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_old_cnt,1);
		}
		msg->wq_time = queue_time;
		mem_allocator.free(entry,sizeof(work_queue_entry));
		INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
	}
	return msg;
}
#endif

void QWorkQueue::enqueue(uint64_t thd_id, Message * msg,bool busy) {
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
	DEBUG("Work Enqueue (%ld,%ld) %d\n",entry->txn_id,entry->batch_id,entry->rtype);

	uint64_t mtx_wait_starttime = get_sys_clock();
	if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
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
	}else if (msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
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
		if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
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
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
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

#if LONG_TXN_WORKLOAD && LONG_TXN_SCHEDULE
void QWorkQueue::insert_calvin_list_lockfree(uint64_t thd_id, TxnManager * txn) {
	// uint64_t key = (txn->get_batch_id() << 32) + (txn->return_id << 24) + txn->get_txn_id() + 1;
	uint64_t key = get_calvin_key(txn->get_batch_id(), txn->return_id, txn->get_txn_id());
	list_node_entry * entry = (list_node_entry*)mem_allocator.align_alloc(sizeof(list_node_entry));
	entry->key = key;
	entry->txn = txn;
	// Initialize snapshot fields from txn/message state so predicate can read them
	assert(txn->last_msg != NULL);
	if (txn->last_msg) {
		YCSBClientQueryMessage * msg = (YCSBClientQueryMessage*) txn->last_msg;
		entry->snapshot_lock_ready_cnt.store(txn->lock_ready_cnt);
		int deps = 0;
		// If msg has deps_left, use it; otherwise 0
		// msg->deps_left may not be initialized for non-split workloads
		#if LONG_TXN_WORKLOAD
			deps = msg->deps_left.load();
		#endif
		entry->snapshot_dep_count.store(deps);
	} else {
		entry->snapshot_lock_ready_cnt.store(txn->lock_ready_cnt);
		entry->snapshot_dep_count.store(0);
	}

	// remember node pointer in txn so parents/children can update snapshot later
	txn->scheduled_entry = entry;
	//
	calvin_scheduled_list_lockfree->insert(entry, thd_id);
}

TxnManager * QWorkQueue::get_from_calvin_list_lockfree(uint64_t thd_id, uint64_t &key) {
	// 第一个函数
	std::function<bool(list_node_entry*)> func = [](list_node_entry * arg) -> bool {
		list_node_entry * entry = arg;
		if (!entry) return false;
		if (entry->key <= minSid) {
			return true;
		}
		// DEBUG_LOCKFREE("[LockFreeList] get_from_calvin_list_lockfree cond1 skip txn %p key=%lu minSid=%lu\n",entry->txn, entry->key, minSid);
		return false;
	};
	// 第二个函数
	std::function<bool(list_node_entry*)> func2 = [](list_node_entry * arg) -> bool {
		list_node_entry * entry = arg;
		if (!entry) return false;
		ClientQueryMessage * last_msg = (ClientQueryMessage*)entry->txn->last_msg;
		if (entry->key <= minSid && entry->txn->lock_ready_cnt <= 0 && last_msg->deps_left.load() <= 0) {
			// if (entry->txn->lock_ready_cnt < 0) {
				// DEBUG_LOCKFREE("[LockFreeList] get_from_calvin_list_lockfree txn %p lock_ready_cnt=%d\n", entry->txn, entry->txn->lock_ready_cnt);
			// }
			// if (last_msg->deps_left.load() < 0) {
				// DEBUG_LOCKFREE("[LockFreeList] get_from_calvin_list_lockfree txn %p deps_left=%d\n", entry->txn, last_msg->deps_left.load());
			// }
			return true;
		}
		// DEBUG_LOCKFREE("[LockFreeList] get_from_calvin_list_lockfree cond2 skip txn %p key=%lu lock_ready_cnt=%d deps_left=%d\n",entry->txn, entry->key, entry->txn->lock_ready_cnt, last_msg->deps_left.load());
		return false;
	};
	list_node_entry * entry = NULL;
	key = 0;

	bool succ = calvin_scheduled_list_lockfree->try_take(func, func2, entry, thd_id);

	if (succ) {
		// DEBUG("[LockFreeList] thd %ld get_from_calvin_list_lockfree key=%lu txn=%p\n", thd_id, entry->key, entry->txn);
		TxnManager * txn = entry->txn;
		// Avoid leaving a dangling pointer in the TxnManager that refers to this list node.
		// The memory backing `entry` must NOT be freed immediately because other threads
		// or deferred readers may still access it. Clear the back-pointer and leave
		// reclamation to a safe, deferred mechanism (epoch/hazard pointers) or a GC list.
		// txn->scheduled_entry = NULL;
		// NOTE: do NOT free(entry) here; if you need reclamation, push `entry` to a
		// thread-local garbage list to be reclaimed when safe.
		return txn;
	} else {
		return NULL;
	}
}
#endif

//elioyan TODO
Message * QWorkQueue::queuetop(uint64_t thd_id)
{
	uint64_t starttime = get_sys_clock();
	assert(ISSERVER || ISREPLICA);
	Message * msg = NULL;
	work_queue_entry * entry = NULL;
	uint64_t mtx_wait_starttime = get_sys_clock();
		bool valid = work_queue->pop(entry);
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
		valid = new_txn_queue->pop(entry);
#endif
	}
	INC_STATS(thd_id,mtx[14],get_sys_clock() - mtx_wait_starttime);

	if(valid) {
		msg = entry->msg;
		assert(msg);
		//printf("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
		uint64_t queue_time = get_sys_clock() - entry->starttime;
		INC_STATS(thd_id,work_queue_wait_time,queue_time);
		INC_STATS(thd_id,work_queue_cnt,1);
		if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
			sem_wait(&_semaphore);
			txn_queue_size --;
			txn_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_new_cnt,1);
		} else {
			sem_wait(&_semaphore);
			work_queue_size --;
			work_dequeue_size ++;
			sem_post(&_semaphore);
			INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
			INC_STATS(thd_id,work_queue_old_cnt,1);
		}
		msg->wq_time = queue_time;
		//DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
		DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
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

#endif

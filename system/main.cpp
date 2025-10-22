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

#include "abort_thread.h"
#include "aria_sequencer.h"
#include "aria_thread.h"
#include "calvin_thread.h"
#include "conflict_thread.h"
#include "snapper_check_thread.h"
#include "client_query.h"
#include "dli.h"
#include "dta.h"
#include "global.h"
#include "io_thread.h"
#include "key_xid.h"
#include "log_thread.h"
#include "logger.h"
#include "maat.h"
#include "manager.h"
#include "math.h"
#include "msg_queue.h"
#include "occ.h"
#include "pps.h"
#include "query.h"
#include "rts_cache.h"
#include "sequencer.h"
#include "sim_manager.h"
#include "abort_queue.h"
#include "thread.h"
#include "tpcc.h"
#include "transport.h"
#include "work_queue.h"
#include "worker_thread.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "da.h"
#include "maat.h"
#include "ssi.h"
#include "wsi.h"
#include "focc.h"
#include "bocc.h"
#include "client_query.h"
#include "wkdb.h"
#include "tictoc.h"
#include "key_xid.h"
#include "rts_cache.h"

void network_test();
void network_test_recv();
void * run_thread(void *);

WorkerThread * worker_thds;
StatsPerIntervalThread * stats_per_interval_thds;
InputThread * input_thds;
OutputThread * output_thds;
AbortThread * abort_thds;
LogThread * log_thds;
WorkerNumThread * worker_num_thds;
#if CC_ALG == CALVIN
CalvinLockThread * calvin_lock_thds;
CalvinSequencerThread * calvin_seq_thds;
#endif
#if CC_ALG == SNAPPER
CalvinLockThread * calvin_lock_thds;
CalvinSequencerThread * calvin_seq_thds;
SnapperCheckThread * snapper_check_thd;
#endif
#if CC_ALG == HDCC
CalvinLockThread * calvin_lock_thds;
CalvinSequencerThread * calvin_seq_thds;
#endif

#if CC_ALG == HDCC || LONG_TXN_SCHEDULE
ConflictThread * conflict_thd;
#endif

#if CC_ALG == ARIA
AriaSequencerThread * aria_seq_thds;
#endif

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char *argv[]) {
	// 0. initialize global data structure
	parser(argc, argv);
#if SEED != 0
	uint64_t seed = SEED + g_node_id;
#else
	uint64_t seed = get_sys_clock();
#endif
	srand(seed);
	printf("Random seed: %ld\n",seed);

	int64_t starttime;
	int64_t endtime;
	starttime = get_server_clock();
	printf("Initializing stats... ");
	fflush(stdout);
	stats.init(g_total_thread_cnt);
	printf("Done\n");

	printf("Initializing global manager... ");
	fflush(stdout);
	glob_manager.init();
	printf("Done\n");
	printf("Initializing transport manager... ");
	fflush(stdout);
	tport_man.init();
	printf("Done\n");
	fflush(stdout);
	printf("Initializing simulation... ");
	fflush(stdout);
	simulation = new SimManager;
	simulation->init();
	printf("Done\n");
	fflush(stdout);
	Workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new YCSBWorkload;
			break;
		case TPCC :
			m_wl = new TPCCWorkload;
			break;
		case PPS :
			m_wl = new PPSWorkload;
			break;
		case DA:
			m_wl = new DAWorkload;
			is_server=true;
			break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("Workload initialized!\n");
	fflush(stdout);
#if NETWORK_TEST
	tport_man.init(g_node_id,m_wl);
	sleep(3);
	if(g_node_id == 0)
		network_test();
	else if(g_node_id == 1)
		network_test_recv();

	return 0;
#endif

#if LONG_TXN_WORKLOAD && LONG_TXN_SCHEDULE
	sids = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t) * g_scheduler_thread_cnt);
	for (uint64_t i = 0; i < g_scheduler_thread_cnt; i++) {
		sids[i] = 0;
	}
#endif


	printf("Initializing work queue... ");
	fflush(stdout);
	work_queue.init();
	printf("Done\n");
	printf("Initializing abort queue... ");
	fflush(stdout);
	abort_queue.init();
	printf("Done\n");
	printf("Initializing message queue... ");
	fflush(stdout);
	msg_queue.init();
	printf("Done\n");
	printf("Initializing transaction manager pool... ");
	fflush(stdout);
	txn_man_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing transaction pool... ");
	fflush(stdout);
	txn_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing row pool... ");
	fflush(stdout);
	row_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing access pool... ");
	fflush(stdout);
	access_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing txn node table pool... ");
	fflush(stdout);
	txn_table_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing query pool... ");
	fflush(stdout);
	qry_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing msg pool... ");
	fflush(stdout);
	msg_pool.init(m_wl,0);
	printf("Done\n");
	printf("Initializing transaction table... ");
	fflush(stdout);
	txn_table.init();
	printf("Done\n");
#if CC_ALG == ARIA
	printf("Initializing sequencer... ");
	fflush(stdout);
	aria_seq.init(m_wl);
	printf("Done\n");
#endif
#if CC_ALG == CALVIN || CC_ALG == HDCC || CC_ALG == SNAPPER
	printf("Initializing sequencer... ");
	fflush(stdout);
	seq_man.init(m_wl);
	printf("Done\n");
#endif
#if CC_ALG == HDCC
	printf("Initializing cc selector... ");
	fflush(stdout);
	cc_selector.init();
	printf("Done\n");
#endif
#if CC_ALG == MAAT
	printf("Initializing Time Table... ");
	fflush(stdout);
	time_table.init();
	printf("Done\n");
	printf("Initializing MaaT manager... ");
	fflush(stdout);
	maat_man.init();
	printf("Done\n");
#endif
#if CC_ALG == SSI
	printf("Initializing In Out Table... ");
	fflush(stdout);
	inout_table.init();
	printf("Done\n");
	printf("Initializing SSI manager... ");
	fflush(stdout);
	ssi_man.init();
	printf("Done\n");
#endif
#if CC_ALG == WSI
	printf("Initializing WSI manager... ");
	fflush(stdout);
	wsi_man.init();
	printf("Done\n");
#endif
#if CC_ALG == WOOKONG
	printf("Initializing WKDB Time Table... ");
	fflush(stdout);
	wkdb_time_table.init();
	printf("Done\n");
	// printf("Initializing WKDB KeyxidCache and RtsCache... ");
	// fflush(stdout);
	// wkdb_key_xid_cache.init();
	// wkdb_rts_cache.init();
	// printf("Done\n");
	printf("Initializing WKDB manager... ");
	fflush(stdout);
	wkdb_man.init();
	printf("Done\n");
#endif

#if CC_ALG == TICTOC
	printf("Initializing MaaT manager... ");
	fflush(stdout);
	tictoc_man.init();
	printf("Done\n");
#endif
#if CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
	printf("Initializing DTA Time Table... ");
	fflush(stdout);
	dta_time_table.init();
	printf("Done\n");
	// printf("Initializing DTA KeyxidCache and RtsCache... ");
	// fflush(stdout);
	// dta_key_xid_cache.init();
	// dta_rts_cache.init();
	// printf("Done\n");
	printf("Initializing DTA manager... ");
	fflush(stdout);
	dta_man.init();
	printf("Done\n");
#endif
#if LOGGING
	printf("Initializing logger... ");
	fflush(stdout);
	logger.init("logfile.log", "txnfile.log");
	printf("Done\n");
#endif

#if SERVER_GENERATE_QUERIES
	printf("Initializing client query queue... ");
	fflush(stdout);
	client_query_queue.init(m_wl);
	printf("Done\n");
	fflush(stdout);
#endif

#if WORKLOAD==DA
	commit_file.open("commit_histroy.txt",ios::app);
	abort_file.open("abort_histroy.txt",ios::app);
#endif

	// 2. spawn multiple threads
	uint64_t thd_cnt = g_thread_cnt;
	uint64_t wthd_cnt = thd_cnt;
	uint64_t rthd_cnt = g_rem_thread_cnt;
	uint64_t sthd_cnt = g_send_thread_cnt;
	uint64_t all_thd_cnt = thd_cnt + rthd_cnt + sthd_cnt + g_abort_thread_cnt + g_stats_per_interval_thread_cnt;
#if LOGGING
		all_thd_cnt += g_logger_thread_cnt;
#endif
#if CC_ALG == CALVIN
#if LONG_TXN_WORKLOAD && LONG_TXN_SCHEDULE
		all_thd_cnt += (g_scheduler_thread_cnt + 1);
#else
		all_thd_cnt += 2; // sequencer + scheduler thread
#endif
#endif
#if CC_ALG == SNAPPER
	all_thd_cnt += 3;	// sequencer + scheduler thread + sanpper_check_thread
#endif
#if CC_ALG == HDCC
		all_thd_cnt += 3; //sequencer + scheduler thread + conflict thread
#endif
#if CC_ALG == ARIA
	all_thd_cnt += 1;	//sequencer thread
	all_thd_cnt -= 1; 	//abort thread
#endif


	printf("%ld, %ld, %ld, %d \n", thd_cnt, rthd_cnt, sthd_cnt, g_abort_thread_cnt);
	printf("all_thd_cnt: %ld, g_this_total_thread_cnt: %d \n", all_thd_cnt, g_this_total_thread_cnt);
	fflush(stdout);
	assert(all_thd_cnt == g_this_total_thread_cnt);

	pthread_t *p_thds = (pthread_t *)malloc(sizeof(pthread_t) * (all_thd_cnt));
	pthread_attr_t attr;
	pthread_attr_init(&attr);

	worker_thds = new WorkerThread[wthd_cnt];

	input_thds = new InputThread[rthd_cnt];
	output_thds = new OutputThread[sthd_cnt];
	abort_thds = new AbortThread[1];
	log_thds = new LogThread[g_logger_thread_cnt];
#if STATS_EVERY_INTERVAL
	stats_per_interval_thds = new StatsPerIntervalThread[1];
#endif

#if CC_ALG == CALVIN
#if LONG_TXN_WORKLOAD && LONG_TXN_SCHEDULE
	calvin_lock_thds = new CalvinLockThread[g_scheduler_thread_cnt];
	calvin_seq_thds = new CalvinSequencerThread[1];
#else
	calvin_lock_thds = new CalvinLockThread[1];
	calvin_seq_thds = new CalvinSequencerThread[1];
#endif
#endif
#if CC_ALG == SNAPPER
	calvin_lock_thds = new CalvinLockThread[1];
	calvin_seq_thds = new CalvinSequencerThread[1];
	snapper_check_thd = new SnapperCheckThread;
#endif
#if CC_ALG == HDCC
	calvin_lock_thds = new CalvinLockThread[1];
	calvin_seq_thds = new CalvinSequencerThread[1];
	// conflict_thds=new ConflictThread[1];
#endif
#if CC_ALG == HDCC || LONG_TXN_SCHEDULE
	conflict_thd=new ConflictThread[1];
#endif
#if CC_ALG == ARIA
	aria_seq_thds = new AriaSequencerThread[1];
#endif
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	//if (WORKLOAD != TEST) {
	//	query_queue.init(m_wl);
	//}
#if CC_ALG == OCC
	printf("Initializing occ lock manager... ");
	occ_man.init();
	printf("Done\n");
#endif

#if CC_ALG == BOCC
	printf("Initializing occ lock manager... ");
	bocc_man.init();
	printf("Done\n");
#endif

#if CC_ALG == FOCC
	printf("Initializing occ lock manager... ");
	focc_man.init();
	printf("Done\n");
#endif

#if CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || \
		CC_ALG == DLI_MVCC
	printf("Initializing si manager... ");
	dli_man.init();
	printf("Done\n");
#endif
	/*
	printf("Initializing threads... ");
	fflush(stdout);
		for (uint32_t i = 0; i < all_thd_cnt; i++)
				m_thds[i].init(i, g_node_id, m_wl);
	printf("Done\n");
	fflush(stdout);
	*/

	endtime = get_server_clock();
	printf("Initialization Time = %ld\n", endtime - starttime);
	fflush(stdout);
	warmup_done = true;
	pthread_barrier_init( &warmup_bar, NULL, all_thd_cnt);

#if SET_AFFINITY
	uint64_t cpu_cnt = 0;
	cpu_set_t cpus;
#endif
	// spawn and run txns again.
	starttime = get_server_clock();
	simulation->run_starttime = starttime;
	simulation->last_da_query_time = starttime;

	uint64_t id = 0;
	
	for (uint64_t i = 0; i < wthd_cnt; i++) {
#if SET_AFFINITY
		CPU_ZERO(&cpus);
		CPU_SET(cpu_cnt, &cpus);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
		cpu_cnt++;
#endif
		assert(id >= 0 && id < wthd_cnt);
		worker_thds[i].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_thds[i]);
	}
	for (uint64_t j = 0; j < rthd_cnt ; j++) {
		assert(id >= wthd_cnt && id < wthd_cnt + rthd_cnt);
		input_thds[j].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], NULL, run_thread, (void *)&input_thds[j]);
	}

	for (uint64_t j = 0; j < sthd_cnt; j++) {
		assert(id >= wthd_cnt + rthd_cnt && id < wthd_cnt + rthd_cnt + sthd_cnt);
		output_thds[j].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], NULL, run_thread, (void *)&output_thds[j]);
	}
#if LOGGING
	for (uint64_t i = 0; i < g_logger_thread_cnt; i++) {
#if SET_AFFINITY
		CPU_ZERO(&cpus);
		CPU_SET(cpu_cnt, &cpus);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
		cpu_cnt++;
#endif
		assert(id >= wthd_cnt + rthd_cnt + sthd_cnt && id < wthd_cnt + rthd_cnt + sthd_cnt + g_logger_thread_cnt);
		log_thds[i].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], NULL, run_thread, (void *)&log_thds[i]);
	}
#endif

#if CC_ALG != CALVIN && CC_ALG != ARIA
	abort_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], NULL, run_thread, (void *)&abort_thds[0]);
#endif

#if CC_ALG == CALVIN
#if LONG_TXN_WORKLOAD && LONG_TXN_SCHEDULE
	the_first_scheduler_id = id; 
	for (uint64_t i = 0; i < g_scheduler_thread_cnt; i++) {
#if SET_AFFINITY
		CPU_ZERO(&cpus);
		CPU_SET(cpu_cnt, &cpus);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
		cpu_cnt++;
#endif

		calvin_lock_thds[i].init(id,g_node_id,m_wl);
		pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_lock_thds[i]);
}
#else
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif

	calvin_lock_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_lock_thds[0]);
#endif
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif

	calvin_seq_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_seq_thds[0]);
#endif
#if CC_ALG == SNAPPER
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif

	calvin_lock_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_lock_thds[0]);
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif

	calvin_seq_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_seq_thds[0]);
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif
	snapper_check_thd->init(id, g_node_id, m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)snapper_check_thd);
#endif

#if CC_ALG == HDCC
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif

	calvin_lock_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_lock_thds[0]);
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif

	calvin_seq_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_seq_thds[0]);
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif
	conflict_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&conflict_thds[0]);
#endif

#if CC_ALG == ARIA
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif
	aria_seq_thds[0].init(id,g_node_id,m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&aria_seq_thds[0]);
#endif

#if STATS_EVERY_INTERVAL
#if SET_AFFINITY
	CPU_ZERO(&cpus);
	CPU_SET(cpu_cnt, &cpus);
	pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
	cpu_cnt++;
#endif
	stats_per_interval_thds[0].init(id, g_node_id, m_wl);
	pthread_create(&p_thds[id++], &attr, run_thread, (void *)&stats_per_interval_thds[0]);
#endif
	for (uint64_t i = 0; i < all_thd_cnt; i++) pthread_join(p_thds[i], NULL);

	endtime = get_server_clock();

	fflush(stdout);
	printf("PASS! SimTime = %f\n", (float)(endtime - starttime) / BILLION);
	if (STATS_ENABLE) stats.print(false);
	//malloc_stats_print(NULL, NULL, NULL);
	printf("\n");
	fflush(stdout);
	// Free things
	//tport_man.shutdown();
	m_wl->index_delete_all();

	/*
	txn_table.delete_all();
	txn_pool.free_all();
	access_pool.free_all();
	txn_table_pool.free_all();
	msg_pool.free_all();
	qry_pool.free_all();
	*/

	return 0;
}

void * run_thread(void * id) {
		Thread * thd = (Thread *) id;
	thd->run();
	return NULL;
}

void network_test() {

			/*
	ts_t start;
	ts_t end;
	ts_t time;
	int bytes;
	float total = 0;
	for (int i = 0; i < 4; ++i) {
		time = 0;
		int num_bytes = (int) pow(10,i);
		printf("Network Bytes: %d\nns: ", num_bytes);
		for(int j = 0;j < 1000; j++) {
			start = get_sys_clock();
			tport_man.simple_send_msg(num_bytes);
			while((bytes = tport_man.simple_recv_msg()) == 0) {}
			end = get_sys_clock();
			assert(bytes == num_bytes);
			time = end-start;
			total += time;
			//printf("%lu\n",time);
		}
		printf("Avg(s): %f\n",total/BILLION/1000);
		fflush(stdout);
		//time = time/1000;
		//printf("Network Bytes: %d, s: %f\n",i,time/BILLION);
		//printf("Network Bytes: %d, ns: %.3f\n",i,time);

	}
			*/

}

void network_test_recv() {
	/*
	int bytes;
	while(1) {
		if( (bytes = tport_man.simple_recv_msg()) > 0)
			tport_man.simple_send_msg(bytes);
	}
	*/
}

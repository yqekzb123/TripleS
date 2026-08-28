#include "logger.h"
#include "work_queue.h"
#include "message.h"
#include "mem_alloc.h"
#include <fstream>


void Logger::init(const char * log_file_name, const char * txn_file_name) {
  this->log_file_name = log_file_name;
  this->txn_file_name = txn_file_name;
  log_file = new std::ofstream[g_logger_thread_cnt];
  for (uint64_t i = 0; i < g_logger_thread_cnt; i++) {
    log_file[i].open(std::string(log_file_name) + "_" + std::to_string(i), ios::out | ios::app | ios::binary);
    assert(log_file[i].is_open());
  }
  txn_file.open(txn_file_name, ios::out | ios::app | ios::binary);
  assert(txn_file.is_open());
  log_queue = new boost::lockfree::queue<LogRecord *> *[g_logger_thread_cnt];
  for (uint64_t i = 0; i < g_logger_thread_cnt; i++) {
    log_queue[i] = new boost::lockfree::queue<LogRecord *> (0);
  }
}

void Logger::release() {
  for (uint64_t i = 0; i < g_logger_thread_cnt; i++) {
    log_file[i].close();
  }
  txn_file.close();
}

LogRecord* Logger::createRecord(uint64_t txn_id, LogIUD iud, uint64_t table_id, uint64_t key) {
  LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord));
  record->rcd.init();
  record->rcd.lsn = ATOM_FETCH_ADD(lsn,1);
  record->rcd.iud = iud;
  record->rcd.txn_id = txn_id;
  record->rcd.table_id = table_id;
  record->rcd.key = key;
  return record;
}

LogRecord* Logger::createRecord(LogRecord* record) {
  LogRecord * my_record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord));
  my_record->rcd.init();
  my_record->copyRecord(record);
  return my_record;
}

void LogRecord::copyRecord(LogRecord* record) {
  rcd.init();
  rcd.lsn = record->rcd.lsn;
  rcd.iud = record->rcd.iud;
  rcd.type = record->rcd.type;
  rcd.txn_id = record->rcd.txn_id;
  rcd.table_id = record->rcd.table_id;
  rcd.key = record->rcd.key;
}


void Logger::enqueueRecord(LogRecord* record) {
  DEBUG("Enqueue Log Record %ld\n",record->rcd.txn_id);
  uint64_t id = record->rcd.txn_id % g_logger_thread_cnt;
  log_queue[id]->push(record);
}

void Logger::processRecord(uint64_t thd_id, uint64_t id) {
  LogRecord * record = NULL;
  bool valid = log_queue[id]->pop(record);

  if(valid) {
    uint64_t starttime = get_sys_clock();
    DEBUG("Dequeue Log Record %ld\n",record->rcd.txn_id);
    if(record->rcd.iud == L_C_FLUSH) {
      flushBuffer(thd_id, false, id);
    } else if(record->rcd.iud == L_FLUSH) {
      flushBuffer(thd_id, true, id);
    } else {
      writeToBuffer(thd_id, record, id);
      log_buf_cnt++;

      if(record->rcd.iud == L_COMMIT || record->rcd.iud == L_ABORT) {
        flushBuffer(thd_id, true, id);
      }
      if (record->rcd.iud == L_COMMIT) {
        if (IS_LOCAL(record->rcd.txn_id)) {
          work_queue.enqueue(thd_id,Message::create_message(record->rcd.txn_id,LOG_FLUSHED),false);
        }
      }
    }
    mem_allocator.free(record,sizeof(LogRecord));
    INC_STATS(thd_id,log_process_time,get_sys_clock() - starttime);
  }

}

uint64_t Logger::reserveBuffer(uint64_t size) { return ATOM_FETCH_ADD(aries_write_offset, size); }

//void Logger::writeToBuffer(char * data, uint64_t offset, uint64_t size) {
void Logger::writeToBuffer(uint64_t thd_id, char * data, uint64_t size) {
  //memcpy(aries_log_buffer + offset, data, size);
  //aries_write_offset += size;
  uint64_t starttime = get_sys_clock();
  txn_file.write(data,size);
  INC_STATS(thd_id,log_write_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,log_write_cnt,1);

}

void Logger::notify_on_sync(uint64_t txn_id) {
  LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord));
  record->rcd.init();
  record->rcd.txn_id = txn_id;
  record->rcd.iud = L_COMMIT;
  enqueueRecord(record);
}

void Logger::writeToBuffer(uint64_t thd_id, LogRecord * record, uint64_t id) {
  DEBUG("Buffer Write\n");
  //memcpy(aries_log_buffer + offset, data, size);
  //aries_write_offset += size;
  uint64_t starttime = get_sys_clock();
#if LOG_COMMAND

  WRITE_VAL(log_file,record->rcd.checksum);
  WRITE_VAL(log_file,record->rcd.lsn);
  WRITE_VAL(log_file,record->rcd.type);
  WRITE_VAL(log_file,record->rcd.txn_id);
  //WRITE_VAL(log_file,record->rcd.partid);
#if WORKLOAD == TPCC || WORKLOAD == CHBENCHMARK
  WRITE_VAL(log_file,record->rcd.txntype);
#endif
  WRITE_VAL_SIZE(log_file,record->rcd.params,record->rcd.params_size);

#else

  WRITE_VAL(log_file[id],record->rcd.checksum);
  WRITE_VAL(log_file[id],record->rcd.lsn);
  WRITE_VAL(log_file[id],record->rcd.type);
  WRITE_VAL(log_file[id],record->rcd.iud);
  WRITE_VAL(log_file[id],record->rcd.txn_id);
  //WRITE_VAL(log_file,record->rcd.partid);
  WRITE_VAL(log_file[id],record->rcd.table_id);
  WRITE_VAL(log_file[id],record->rcd.key);
  /*
  WRITE_VAL(log_file,record->rcd.n_cols);
  WRITE_VAL(log_file,record->rcd.cols);
  WRITE_VAL_SIZE(log_file,record->rcd.before_image,record->rcd.before_image_size);
  WRITE_VAL_SIZE(log_file,record->rcd.after_image,record->rcd.after_image_size);
  */

#endif
  INC_STATS(thd_id,log_write_time,get_sys_clock() - starttime);

}

void Logger::flushBuffer(uint64_t thd_id, bool isLog, uint64_t id) {
  DEBUG("Flush Buffer\n");
  uint64_t starttime = get_sys_clock();
  if (isLog) {
    log_file[id].flush();
  } else {
    txn_file.flush();
  }
  INC_STATS(thd_id,log_flush_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,log_flush_cnt,1);

  last_flush = get_sys_clock();
  log_buf_cnt = 0;
}

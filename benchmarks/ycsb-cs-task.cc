#include "ycsb-cs-task.h"


extern YcsbWorkload ycsb_workload;

bench_worker::workload_desc_vec ycsb_coro_task_worker::get_workload() const {
  workload_desc_vec w;
  if (ycsb_workload.insert_percent())
    w.push_back(workload_desc("Insert",
                              double(ycsb_workload.insert_percent()) / 100.0,
                              nullptr, nullptr, TxnInsert));
  if (ycsb_workload.read_percent())
    w.push_back(workload_desc("Read",
                              double(ycsb_workload.read_percent()) / 100.0,
                              nullptr, nullptr, TxnRead));
  if (ycsb_workload.update_percent())
    w.push_back(workload_desc("Update",
                              double(ycsb_workload.update_percent()) / 100.0,
                              nullptr, nullptr, TxnUpdate));
  if (ycsb_workload.scan_percent())
    w.push_back(workload_desc("Scan",
                              double(ycsb_workload.scan_percent()) / 100.0,
                              nullptr, nullptr, TxnScan));
  if (ycsb_workload.rmw_percent())
    w.push_back(workload_desc("RMW",
                              double(ycsb_workload.rmw_percent()) / 100.0,
                              nullptr, nullptr, TxnRMW));
  return w;
}

void ycsb_coro_task_worker::MyWork(char *) {

}


void ycsb_usertable_coro_task_loader::load() {

}

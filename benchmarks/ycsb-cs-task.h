#include "ycsb.h"

class ycsb_coro_task_worker : public ycsb_worker {
public:
  ycsb_coro_task_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
      : ycsb_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b) {}

  virtual workload_desc_vec get_workload() const override;
  virtual void MyWork(char *) override;
};

class ycsb_usertable_coro_task_loader : public ycsb_usertable_loader {
public:
  ycsb_usertable_coro_task_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      uint32_t loader_id): ycsb_usertable_loader(seed, db, open_tables, loader_id) {}

  virtual void load() override;
};

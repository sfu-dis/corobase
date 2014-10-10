#pragma once
#include <type_traits>
#include <thread>
#include <numa.h>
#include <unistd.h>
#include <iostream>
#include "../macros.h"
#include "../core.h"
#include "../util.h"
#include "epoch.h"

/*
 * The garbage collector for ERMIA to remove dead tuple versions.
 */
namespace GC {
  // if _allocated_memory reaches this many, start GC
  // FIXME: tzwang: problem: if this watermark is too low, then some tx or
  // thread might stuck in some epoch (or we could say some epoch is stuck)
  // because the tx needs more memory, i.e., this tx might need to cross
  // epoch boundaries.
  static const size_t WATERMARK = 128*1024*1024; // TODO. should be based on physical memory size or GC performance

  class gc_thread {
    public:
      gc_thread();
    private:
      static void gc_daemon();
  };
  void report_malloc(size_t nbytes);
  void epoch_enter();
  void epoch_exit();
  void epoch_quiesce();
  
  // percore memory allocation counter for GC
  extern percore<size_t, false, false> allocated_memory;
  extern gc_thread GC_thread;


};  // end of namespace


// -*- mode:c++ -*-
#pragma once

#include "sm-common.h"

/* The heap storage manager.

   Like the log, the heap is log-structured with multiple
   segments. Unlike the log manager, the heap is fully unstructured
   (no log blocks), and provides only space management (no memory
   buffering or background writes provided). Like the log manager,
   heap segments can only be reclaimed if a higher level task has
   moved all valid records to somewhere else.

   At recovery time, the heap manager searches for the end of the
   latest segment and resumes allocating beyond that point. It makes
   no attempt to validate heap contents, because the application
   should not publish a heap pointer until it is durable.

   At segment reclamation time, a record is considered valid if a
   pointer to it is reachable from some OID array or log entry;
   otherwise it is dead will be reclaimed.

   The heap is intended for large objects that cannot be placed
   directly in the log at creation time, and also for cold records
   that remain live after their log record is reclaimed. The heap also
   stores checkpoints (which are typically sized in GB).

   The heap should normally be several factors larger than the log,
   but can be placed in cheaper/slower storage.
 */
struct sm_heap_mgr {
    /* Allocate and return a new heap manager. If [dname] exists, it
       will be mounted and used. Otherwise, a new (empty) heap
       directory will be created.
     */
    static
    sm_heap_mgr *make(char const *dname, size_t segment_size);
    
    /* Allocate space for an object in the heap and return its
       location, but do not write anything yet.
     */
    fat_ptr alloc_object(uint8_t szcode, size_t align_bits=DEFAULT_ALIGNMENT_BITS);

    /* Store an object of the specified size in the heap at the
       specified location, and return once it is durable.

       WARNING: Attempting to overwrite an existing object is strongly
       discouraged and will void your warranty. A caller that insists
       on overwriting an object is responsible to deal with partial or
       interrupted writes, and to update any in-memory copies of the
       object that might exist.
     */
    void store_object(fat_ptr dest, char *buf, size_t bufsz, size_t align_bits=DEFAULT_ALIGNMENT_BITS);
    
    /* Load the object referenced by [ptr] from the heap. The pointer
       must reference the log (ASI_HEAP) and the given buffer must be
       large enough to hold the object.
     */
    void load_object(char *buf, size_t bufsz, fat_ptr ptr, size_t align_bits=DEFAULT_ALIGNMENT_BITS);

    virtual ~sm_heap_mgr() { }
    
protected:
    // Forbid direct instantiation
    sm_heap_mgr() { }
};

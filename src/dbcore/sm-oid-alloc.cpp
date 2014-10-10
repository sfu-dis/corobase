#include "oid-alloc.h"

#include "dynarray.h"


namespace {
#if 0
} // disable autoindent
#endif

#warning Watch out for thread-local allocations!
/* ^^^
   
   To reduce contention, we use a thread-local caching mechanism that
   hands out chunks of the OID space to threads, which they can then
   use at their leisure. In a sense, the system considers those OIDs
   allocated as soon as a thread takes ownership. However, checkpoints
   have to count cached OIDs as unallocated (because they have not, in
   fact, been used yet).

   The solution is to first make a consistent snapshot of each
   allocator, ignoring thread-local caches entirely. Any allocator
   activity that occurs after the snapshot was taken will be properly
   logged and therefore not a problem. Once all allocators have been
   copied, then claw back all OIDs held in thread-local caches, and
   then patch up the local snapshots by freeing the OIDs into it. In
   order to avoid any risk of growing the allocator (snapshots cannot
   be resized), the snapshotting process must ensure that there is
   space in the snapshot's L1 and/or L2 for all OIDs that might be
   reclaimed from the various threads in the system.
*/


# if 0
{ // disable autoindent
#endif
}

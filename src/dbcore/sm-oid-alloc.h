// -*- mode:c++ -*-
#include "sm-common.h"

/* This is an allocator designed specifically for managing multiple
   OID sets in parallel, each identified by an FID. It also supports
   checkpoint and restore of internal allocator state.

   The allocator manages peristent objects, and thus must itself be
   persistent. That functionality is not implemented here, and all
   allocator activity is volatile by default. Higher-level code is
   responsible to manage and checkpoint allocator state, presumably by
   storing snapshots in an FID dedicated to that task (and probably
   with an allocator to manage the set of in-use FIDs). Checkpointing
   code can use oid_allocator_copy and oid_allocator_restore to
   checkpoint and resore allocator state. Allocator activity not
   captured by any checkpoint must be inferred from the log during
   recovery and restored using the oid_alloc_restore and
   sm_oid_free_restore functions described below.

   The allocator has a two-part API. During forward processing,
   threads request to allocate and deallocate OIDs; during recovery,
   threads announce allocation and deallocation of allocators and
   OIDs, as inferred from checkpoints and the log. Recovery functions
   are subject to fewer consistency checks, because we use soft
   checkpointing: a checkpoint could easily account for activity that
   occurred after the checkpoint began. However, transaction epochs
   mean that no checkpoint can span multiple alloc-dealloc cycles of
   the same OID (the checkpoint is created by a special transaction).

   Implementation note: the allocator uses one bit for each OID it has
   ever allocated. For a maximum-sized file with 2**32 OIDs, that
   translates to roughly 512MB of allocator state. While admittedly
   very large, this is less than 2% of the total space overhead of the
   OID table, whose entries would occupy 32GB; further, the objects
   managed by the OID table would almost certainly several factors
   more space than that.
 */



/* [CHECKPOINTING] [RECOVERY]

   The scaling factor to use when decoding OID allocator snapshot
   version sizes.

   WARNING: OID allocator snapshots can be quite large (100MB or more
   for high-cardinality tables). significant memory leaks can result
   if allocator snapshot versions are not disposed of properly!
 */
static size_t const SM_OID_ALLOC_ALIGN_BITS = 10;


/* Create a new OID allocator, associated with file [f]. OIDs can then
   be allocated and freed by passing [f] to the appropriate functions.
   The caller is responsible to call destroy_allocator() when the
   allocator is no longer needed.
       
   In the event of a crash, a newly-created allocator will be freed
   unless its pointer was persisted somewhere.

   NOTE: the returned version contains a copy of the allocator's
   initial state, and is suitable for checkpointing. The version
   becomes progressively more stale, however. A fresh version can be
   obtained at any time by calling oid_copy_allocator(). Successive
   versions should be tracked using an OID array slot so the GC can
   free them.
   
   NOTE: [f] serves only as a key when looking up which allocator
   object to use. It is the caller's responsibility to track which
   FIDs are in use by the system, as not every FID has an allocator
   (some FID are "slaves" to others). During forward processing,
   attempts to allcoate or free OIDs in an FID that has no allocator
   will raise an exception; during recovery, any inserts or deletes to
   files having no allocator are silently ignored.
*/
version *sm_oid_allocator_create(FID f);

/* Destroy an existing OID allocator and free any resources associated
   with it.
   
   In the event of a crash, the allocator will be restored unless
   the persistent pointer to it was also removed.
*/
void sm_oid_allocator_destroy(FID f);

/* Place up to [n] fragmented OIDs into [arr] and return the number
   actually placed.

   A naive bitmap-based allocator implementation could occupy 100MB or
   more for large tables, with O(n) per-OID allocation cost in the
   worst case. To avoid these undesirable outcomes, the implementation
   uses several optimizations to reduce time and space
   requirements. As with most allocators, however, deletions can cause
   internal fragmentation over time. Peak allocator performance can be
   maintained by periodically finding and relocating records with
   inconvenient OIDs, but relocation requires updating all references
   to the OID that is to be moved, which requires knowledge of the
   database layout at a minimum, and almost certainly shoudl be done
   by a system transaction in order to maintain consistency.
 */
size_t sm_oid_allocator_fragments(OID *arr, size_t n);

/* Allocate a new OID in file [f] and return it. The caller is
   responsible to call sm_oid_free() when the OID is no longer needed.

   In the event of a crash, the OID will be freed unless data was
   actually placed in it by a committed transaction's insert.

   Throw fid_is_full if no more OIDs are available for allocation in
   this file.
 */
OID sm_oid_alloc(FID f);

/* Free an in-use OID. It (and its contents) will be reclaimed by the
   garbage collector, after which the OID may be recycled.

   In the event of a crash, the OID will be preserved unless the data
   was actually removed by a committed transaction's delete.
 */
void sm_oid_free(FID f, OID o);

/* [CHECKPOINTING]

   Take a fuzzy snapshot the internal state of allocator [f] and
   return the resulting version, suitable for checkpointing. It is the
   caller's responsibility to ensure the GC machinery eventually finds
   and reclaims stale versions (which can be quite large, you have
   been warned).
 */
version *sm_oid_allocator_copy(FID f);

/* [RECOVERY]

   Restore an allocator using the contents of [v], which must be a
   snapshot previously returned by either sm_oid_allocator_create or
   sm_oid_allocator_copy. Note that the recovery logic is responsible
   to detect log records which imply the need to call this function.
   
   Throw illegal_argument if the target FID already has an
   allocator. The implication is that snapshots of an existing
   allocator should only be found in checkpoints.

   NOTE: for allocators created after the most recent checkpoint, the
   recovery system can choose whether to restore or re-create the
   allocator; the outcome is the same either way.

   NOTE: no recovery-centric alloctor destroy function exists because
   allocator destruction can only occur for already-existing
   allocators. A call to sm_oid_allocator_destroy thus suffices.
 */
void sm_oid_allocator_restore(FID f, version *v);

/* [RECOVERY]

   Notify allocator [f] that allocation of OID [o] has been found (or
   inferred) in the log during recovery. This operation succeeds even
   if [o] was already marked as allocated, in order to accommodate the
   imprecision that comes with fuzzy checkpointing.
 */
void sm_oid_alloc_restore(FID f, OID o);

/* [RECOVERY]

   Similar to sm_oid_alloc_restore, except it flags OID [o] as deleted.
*/
void sm_oid_free_restore(FID f, OID o);


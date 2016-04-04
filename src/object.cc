#include "object.h"
#include "tuple.h"
#include "dbcore/sm-log-recover.h"
#include "dbcore/sm-log.h"
fat_ptr
object::create_tuple_object(const varstr *tuple_value, bool do_write)
{
    if (tuple_value)
        do_write = true;
    // Calculate Tuple Size
    const uint32_t data_sz = tuple_value ? tuple_value->size(): 0;
    size_t alloc_sz = sizeof(dbtuple) + sizeof(object) + data_sz;

    // Allocate a version
    object *obj = new (MM::allocate(alloc_sz)) object();

    // Tuple setup
    dbtuple* tuple = obj->tuple();
    new (tuple) dbtuple(data_sz);
    tuple->pvalue = (varstr *)tuple_value;
    if (do_write)
        tuple->do_write();

    size_t aligned_sz = encode_size_aligned(alloc_sz);
    ASSERT(aligned_sz != INVALID_SIZE_CODE);
    return fat_ptr::make(obj, aligned_sz);
}

// Dig out a tuple from the durable log
// ptr should point to some position in the log
// Returns a fat_ptr to the object created
fat_ptr
object::create_tuple_object(fat_ptr ptr, fat_ptr nxt, sm_log_recover_mgr *lm)
{
    ASSERT(ptr.asi_type() == fat_ptr::ASI_LOG);
    auto sz = decode_size_aligned(ptr.size_code()) + sizeof(object) + sizeof(dbtuple);

    object *obj = new (MM::allocate(sz)) object(ptr, nxt);

    // Load tuple varstr from the log
    dbtuple* tuple = obj->tuple();
    new (tuple) dbtuple(sz);
    if (lm)
        lm->load_object((char *)tuple->get_value_start(), sz, ptr);
    else {
        ASSERT(logmgr);
        logmgr->load_object((char *)tuple->get_value_start(), sz, ptr);
    }

    // Strip out the varstr stuff
    tuple->size = ((varstr *)tuple->get_value_start())->size();
    ASSERT(tuple->size < sz);
    memmove(tuple->get_value_start(),
            (char *)tuple->get_value_start() + sizeof(varstr),
            tuple->size);

    obj->_clsn = ptr;   // XXX (tzwang): use the tx's cstamp!
    ASSERT(obj->_clsn.asi_type() == fat_ptr::ASI_LOG);
    return fat_ptr::make(obj, ptr.size_code());   // asi_type=0 (memory)
}

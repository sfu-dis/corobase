#pragma once
#include "varstr.h"
#include "dbcore/sm-common.h"

class dbtuple;
struct sm_log_recover_mgr;

// An object wraps a tuple with its physical location in storage (the log)
// and the older version it supersedes. If the version is not in memory,
// the object will have no payload and the corresponding OID entry in the
// OID array will indicate this by having an ASI_LOG flag. The reader of
// this tuple then needs to look at _pdest and dig the version out from
// the log, ensure_tuple() does this.
class object
{
	public:
        object(fat_ptr pdest, fat_ptr next) : _pdest(pdest), _next(next), _clsn(NULL_PTR) {}
        object() : _pdest(NULL_PTR), _next(NULL_PTR), _clsn(NULL_PTR) {}

        fat_ptr _pdest; // permanent location in storage
        fat_ptr _next;
        fat_ptr _clsn;

		inline char* payload() { return (char*)((char*)this + sizeof(object)); }
        dbtuple *tuple() { return (dbtuple *)payload(); }
        static fat_ptr create_tuple_object(fat_ptr ptr, fat_ptr nxt, sm_log_recover_mgr *lm = NULL);
        static fat_ptr create_tuple_object(const varstr *tuple_value, bool do_write);
};


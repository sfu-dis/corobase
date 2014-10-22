#include "sm-log-impl.h"

#include <cstring>

using namespace RCU;

void
sm_log::load_object(char *buf, size_t bufsz, fat_ptr ptr, size_t align_bits)
{
    get_impl(this)->_lm._lm.load_object(buf, bufsz, ptr, align_bits);
}

fat_ptr
sm_log::load_ext_pointer(fat_ptr ptr)
{
    return get_impl(this)->_lm._lm.load_ext_pointer(ptr);
}


sm_log *
sm_log::new_log(char const *dname, size_t segsz,
                sm_log_recover_function *rfn, void *rarg,
                size_t bufsz)
{
    return new sm_log_impl(dname, segsz, rfn, rarg, bufsz);
}

sm_log_scan_mgr *
sm_log::get_scan_mgr()
{
    return get_impl(this)->_lm._lm.scanner;
}

sm_tx_log *
sm_log::new_tx_log()
{
    auto *self = get_impl(this);
    typedef _impl_of<sm_tx_log>::type Impl;
    return new (Impl::alloc_storage()) Impl(self);
}

fat_ptr
sm_log_impl::lsn2ptr(LSN lsn, bool is_ext) {
    return get_impl(this)->_lm._lm.lsn2ptr(lsn, is_ext);
}

LSN
sm_log_impl::ptr2lsn(fat_ptr ptr) {
    return _lm._lm.ptr2lsn(ptr);
}

LSN
sm_log::cur_lsn()
{
    auto *log = &get_impl(this)->_lm;
    auto offset = log->cur_lsn_offset();
    auto *sid = log->_lm.get_offset_segment(offset);

	if (not sid) {
		/* must have raced a new segment opening */
		while (1) {
			sid = log->_lm._newest_segment();
			if (sid->start_offset >= offset)
				break;
		}
	}
    ASSERT(sid);
    return sid->make_lsn(offset);
}

LSN
sm_log::durable_lsn()
{
    auto *log = &get_impl(this)->_lm;
    auto offset = log->dur_lsn_offset();
    auto *sid = log->_lm.get_offset_segment(offset);
    ASSERT(sid);
    return sid->make_lsn(offset);
}

void
sm_log::wait_for_durable_lsn(LSN dlsn)
{
    auto *self = get_impl(this);
    self->_lm.wait_for_durable(dlsn.offset());
}

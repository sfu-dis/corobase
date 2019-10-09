#include "context_mock.h"

#include <dbcore/sm-config.h>

static ermia::XID dummy_xid = ermia::XID::make(0, 0);
static ermia::TXN::xid_context dummy_xid_context;

static void reset_dummy_context() {
    dummy_xid_context.begin_epoch = 0;
    dummy_xid_context.owner = dummy_xid;
    dummy_xid_context.xct = nullptr;
}

ermia::TXN::xid_context *mock_xid_get_context() {
    reset_dummy_context();
    return &dummy_xid_context;
}

ermia::epoch_num mock_get_cur_epoch() {
    return 0;
}


#pragma once

#include <dbcore/xid.h>
#include <dbcore/sm-oid.h>
#include <dbcore/epoch.h>

ermia::TXN::xid_context *mock_xid_get_context();

ermia::epoch_num mock_get_cur_epoch();


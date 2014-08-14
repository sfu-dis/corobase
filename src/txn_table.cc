#include "txn_table.h"
txn_table txn_table::instance;

// assume we always have space for now...
txn_table::txn_descriptor*
txn_table::td_alloc()
{
  unsigned int& idx = next_descs_.my();
  txn_descriptor* td = my_table() + idx;

  INVARIANT(idx < tds_per_core());
  INVARIANT(!td->in_use);

  td->in_use = true;
  td->xid = xid_alloc();
  td->state = TXN_EMBRYO;
  //std::cout << "NEW TXN " << idx << " " << td->xid.epoch() << " " << td->xid.local() << std::endl;

  txn_descriptor* next_td = NULL;
  do {
    idx++;
    idx %= tds_per_core();
    next_td = my_table() + idx;
  }
  while (next_td->in_use);

  return td;
}

void
txn_table::td_free(txn_descriptor *td, txn_state ts)
{
  //std::cout << "END TXN " << td->xid.epoch() << " " << td->xid.local() << std::endl;
  td->state = ts;
  xid_free(td->xid);
  td->in_use = false;
}

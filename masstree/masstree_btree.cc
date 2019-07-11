#include "masstree_btree.h"
#include "../txn.h"

namespace ermia {

// Multi-key search using Coroutines
template <typename P>
ermia::dia::generator<bool>
mbtree<P>::ycsb_read_coro(ermia::transaction *txn, const std::vector<key_type *> &keys,
                          threadinfo &ti, versioned_node_t *search_info) const {
  std::vector<OIDAMACState> version_requests;
  for (uint32_t i = 0; i < keys.size(); ++i) {
    auto &k = *keys[i];

    Masstree::unlocked_tcursor<P> lp(table_, k.data(), k.size());

    // variables in find_unlocked
    int match;
    key_indexed_position kx;
    Masstree::node_base<P>* root = const_cast<Masstree::node_base<P>*>(lp.root_);

  retry:
    // variables in reach_leaf
    const Masstree::node_base<P>* n[2];
    typename Masstree::node_base<P>::nodeversion_type v[2];
    bool sense;

  retry2:
    sense = false;
    n[sense] = lp.root_;
    while (1) {
      v[sense] = n[sense]->stable_annotated(ti.stable_fence());
      if (!v[sense].has_split()) break;
      n[sense] = n[sense]->unsplit_ancestor();
    }

    // Loop over internal nodes.
    while (!v[sense].isleaf()) {
      const Masstree::internode<P>* in = static_cast<const Masstree::internode<P>*>(n[sense]);
      in->prefetch();
      co_await std::experimental::suspend_always{};
      int kp = Masstree::internode<P>::bound_type::upper(lp.ka_, *in);
      n[!sense] = in->child_[kp];
      if (!n[!sense]) goto retry2;
      v[!sense] = n[!sense]->stable_annotated(ti.stable_fence());

      if (likely(!in->has_changed(v[sense]))) {
        sense = !sense;
        continue;
      }

      typename Masstree::node_base<P>::nodeversion_type oldv = v[sense];
      v[sense] = in->stable_annotated(ti.stable_fence());
      if (oldv.has_split(v[sense]) &&
          in->stable_last_key_compare(lp.ka_, v[sense], ti) > 0) {
        goto retry2;
      }
    }

    lp.v_ = v[sense];
    lp.n_ = const_cast<Masstree::leaf<P>*>(static_cast<const Masstree::leaf<P>*>(n[sense]));

  forward:
    if (lp.v_.deleted()) goto retry;

    lp.n_->prefetch();
    co_await std::experimental::suspend_always{};
    lp.perm_ = lp.n_->permutation();
    kx = Masstree::leaf<P>::bound_type::lower(lp.ka_, lp);
    if (kx.p >= 0) {
      lp.lv_ = lp.n_->lv_[kx.p];
      lp.lv_.prefetch(lp.n_->keylenx_[kx.p]);
      co_await std::experimental::suspend_always{};
      match = lp.n_->ksuf_matches(kx.p, lp.ka_);
    } else
      match = 0;
    if (lp.n_->has_changed(lp.v_)) {
      lp.n_ = lp.n_->advance_to_key(lp.ka_, lp.v_, ti);
      goto forward;
    }

    if (match < 0) {
      lp.ka_.shift_by(-match);
      root = lp.lv_.layer();
      goto retry;
    }
    
    if (match) {
      auto o = lp.value();
      version_requests.emplace_back(o);
      /*
      auto tuple = oidmgr->oid_get_version(descriptor_->GetTupleArray(), o, txn->GetXIDContext());
      if (tuple) {
        varstr value;
        auto rc = txn->DoTupleRead(tuple, &value);
      }
      */
    }
    if (search_info) {
      *search_info = versioned_node_t(lp.node(), lp.full_version_value());
    }
  }

  oidmgr->oid_get_version_amac(descriptor_->GetTupleArray(), version_requests, txn->GetXIDContext());
  uint32_t i = 0;
  for (auto &vr : version_requests) {
    if (vr.tuple) {
      varstr value;
      txn->DoTupleRead(vr.tuple, &value);
    } else if (config::phantom_prot) {
      //DoNodeRead(txn, sinfo.first, sinfo.second);
    }
  }

  // Use AMAC to access the versions
  txn->commit();
  txn->~transaction();
  co_return true;
}

template
ermia::dia::generator<bool>
mbtree<masstree_params>::ycsb_read_coro(ermia::transaction *txn,
                                        const std::vector<varstr *> &keys,
                                        threadinfo &ti, versioned_node_t *search_info) const;
} // namespace ermia

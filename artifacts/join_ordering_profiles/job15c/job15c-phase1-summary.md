# Samply summary: job15c-phase1.json.gz

Stack filter: `JoinOrdering`. Matching samples: **597**.

## Inclusive samples

| Function | Samples | Share |
|:---|---:|---:|
| `core::ops::function::FnOnce::call_once{{vtable.shim}}` | 597 | 100.0% |
| `tokio::runtime::scheduler::current_thread::CoreGuard::block_on` | 597 | 100.0% |
| `tokio::runtime::context::scoped::Scoped<T>::set` | 597 | 100.0% |
| `tokio::runtime::context::runtime::enter_runtime` | 597 | 100.0% |
| `optd_core::optimize::PassManager::run_inner` | 597 | 100.0% |
| `tokio::runtime::runtime::Runtime::block_on` | 597 | 100.0% |
| `<optd_core::optimize::join_ordering::JoinOrdering<M> as optd_core::optimize::QueryPass>::run` | 597 | 100.0% |
| `optd_datafusion::explain_udfs::build_ir_trace::{{closure}}` | 597 | 100.0% |
| `std::sys::backtrace::__rust_begin_short_backtrace` | 597 | 100.0% |
| `<std::sys::thread::unix::Thread>::new::thread_start` | 597 | 100.0% |
| `optd_core::optimize::PassManager::run_with_trace` | 597 | 100.0% |
| `_pthread_start` | 597 | 100.0% |
| `optd_core::optimize::join_ordering::dphyp::DPhyp<M>::solve_subset` | 596 | 99.8% |
| `optd_core::optimize::join_ordering::dphyp::DPhyp<M>::emit_csg` | 595 | 99.7% |
| `optd_core::optimize::join_ordering::dphyp::DPhyp<M>::emit_csg_cmp` | 584 | 97.8% |
| `optd_core::optimize::join_ordering::candidate::best_join_candidate` | 572 | 95.8% |
| `optd_core::cost::CostModel::total_cost_from_children` | 565 | 94.6% |
| `<optd_core::cost::DefaultCostModel as optd_core::cost::CostModel>::operator_cost` | 563 | 94.3% |
| `optd_core::analysis::CardinalityEstimationV1::get_shared` | 563 | 94.3% |
| `optd_core::cost::cardinality_profile` | 563 | 94.3% |
| `<optd_core::analysis::CardinalityEstimationV1 as optd_core::analysis::CachedAnalysis>::compute` | 554 | 92.8% |
| `optd_core::analysis::join_profile_from_predicate` | 552 | 92.5% |
| `optd_core::optimize::join_ordering::dphyp::DPhyp<M>::enumerate_csg_rec` | 481 | 80.6% |
| `optd_core::optimize::join_ordering::dphyp::DPhyp<M>::enumerate_cmp_rec` | 465 | 77.9% |
| `optd_core::analysis::combine_join_columns` | 123 | 20.6% |

## Self samples

| Function | Samples | Share |
|:---|---:|---:|
| `optd_core::analysis::join_profile_from_predicate` | 86 | 14.4% |
| `<core::hash::sip::Hasher<S> as core::hash::Hasher>::write` | 64 | 10.7% |
| `core::hash::BuildHasher::hash_one` | 42 | 7.0% |
| `optd_core::analysis::combine_join_columns` | 35 | 5.9% |
| `alloc::collections::btree::node::Handle<alloc::collections::btree::node::NodeRef<alloc::collections::btree::node::marker::Mut,K,V,alloc::collections::btree::node::marker::Leaf>,alloc::collections::btree::node::marker::Edge>::insert_recursing` | 35 | 5.9% |
| `tiny_malloc_from_free_list` | 33 | 5.5% |
| `tiny_free_no_lock` | 25 | 4.2% |
| `optd_core::analysis::EquivalenceClassState::find` | 23 | 3.9% |
| `tiny_free_list_remove_ptr` | 21 | 3.5% |
| `hashbrown::raw::RawTable<T,A>::reserve_rehash` | 17 | 2.8% |
| `_platform_memmove` | 17 | 2.8% |
| `free_tiny` | 15 | 2.5% |
| `hashbrown::raw::RawTable<T,A>::remove_entry` | 14 | 2.3% |
| `_platform_memset` | 14 | 2.3% |
| `<alloc::vec::Vec<T> as alloc::vec::spec_from_iter_nested::SpecFromIterNested<T,I>>::from_iter` | 11 | 1.8% |
| `alloc::collections::btree::map::entry::VacantEntry<K,V,A>::insert_entry` | 10 | 1.7% |
| `<optd_core::analysis::ColumnProfile as core::clone::Clone>::clone` | 9 | 1.5% |
| `tiny_free_list_add_ptr` | 8 | 1.3% |
| `core::ops::function::impls::<impl core::ops::function::FnMut<A> for &mut F>::call_mut` | 8 | 1.3% |
| `tiny_malloc_should_clear` | 8 | 1.3% |
| `optd_core::analysis::filter_equivalence_classes` | 6 | 1.0% |
| `small_free_list_add_ptr` | 6 | 1.0% |
| `optd_core::optimize::join_ordering::graph::JoinGraph::neighborhood_within` | 5 | 0.8% |
| `get_tiny_previous_free_msize` | 5 | 0.8% |
| `hashbrown::map::HashMap<K,V,S,A>::insert` | 5 | 0.8% |

## Matching threads

| Function | Samples | Share |
|:---|---:|---:|
| `Thread <44543440> (44543440)` | 303 | 50.8% |
| `Thread <44543147> (44543147)` | 294 | 49.2% |

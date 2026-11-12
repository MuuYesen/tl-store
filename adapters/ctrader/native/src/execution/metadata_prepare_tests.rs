fn mark_ready(carrier: &Carrier, needed: HashSet<u64>, age_ns: u64) {
    let mut state=carrier.shared.lock().unwrap();
    state.needed=needed;
    state.ready=true;
    state.metadata_mono_ns=crate::clock::monotonic_ns().unwrap()-age_ns;
    state.changed.notify_waiters();
}
#[tokio::test]
async fn a_slow_caller_refreshes_expired_metadata_before_returning() {
    let carrier=Carrier::new(1,42);
    mark_ready(&carrier,HashSet::from([7]),31_000_000_000);
    let mut changes=carrier.changes.subscribe();
    let work=carrier.prepare_metadata(HashSet::from([7]));tokio::pin!(work);
    assert!(futures_util::poll!(work.as_mut()).is_pending());
    assert!(changes.has_changed().unwrap());changes.borrow_and_update();
    mark_ready(&carrier,HashSet::from([7]),0);
    work.await.unwrap();
    assert!(!changes.has_changed().unwrap(),"Preparing one requested sample must not keep polling");
}
#[tokio::test]
async fn fresh_matching_metadata_does_not_issue_another_refresh() {
    let carrier=Carrier::new(1,42);
    mark_ready(&carrier,HashSet::from([7]),1_000_000_000);
    let changes=carrier.changes.subscribe();
    carrier.prepare_metadata(HashSet::from([7])).await.unwrap();
    assert!(!changes.has_changed().unwrap());
}
#[tokio::test]
async fn changed_symbol_set_cannot_reuse_another_inventories_metadata() {
    let carrier=Carrier::new(1,42);
    mark_ready(&carrier,HashSet::from([7]),0);
    let work=carrier.prepare_metadata(HashSet::from([8]));tokio::pin!(work);
    assert!(futures_util::poll!(work.as_mut()).is_pending());
    mark_ready(&carrier,HashSet::from([7]),0);
    assert!(futures_util::poll!(work.as_mut()).is_pending());
    mark_ready(&carrier,HashSet::from([8]),0);
    work.await.unwrap();
}
#[tokio::test]
async fn cancelling_a_wait_does_not_create_recurring_account_requests() {
    let carrier=Carrier::new(1,42);let mut changes=carrier.changes.subscribe();
    assert!(tokio::time::timeout(std::time::Duration::from_millis(10),carrier.prepare_metadata(HashSet::from([7]))).await.is_err());
    assert!(changes.has_changed().unwrap());changes.borrow_and_update();
    tokio::task::yield_now().await;
    assert!(!changes.has_changed().unwrap());
}

#[test]
fn flat_transition_keeps_subscribed_originals_but_exports_only_required_symbols() {
    let mut cache=RawSpots::new(1,42).unwrap();
    assert!(cache.set_required(HashSet::from([7])));
    cache.record(1,&json!({"ctidTraderAccountId":42,"symbolId":7,"timestamp":10,"bid":100,"ask":101}),100);
    assert!(cache.set_required(HashSet::new()));
    cache.record(1,&json!({"ctidTraderAccountId":42,"symbolId":7,"timestamp":20,"bid":200,"ask":201}),200);
    assert!(cache.originals(0).is_empty(),"Flat snapshots must not export unrelated symbols");
    assert!(cache.set_required(HashSet::from([7])));
    let originals=cache.originals(0);
    assert_eq!(originals.len(),1,"The broker subscription never stopped; current originals must survive");
    assert_eq!(originals[0].payload["timestamp"],20);
    assert_eq!(originals[0].payload["bid"],200);
    assert_eq!(originals[0].received_mono_ns,200,"Original receive clock must not be relabelled");
}
#[test]
fn retained_quote_lifetime_bound_cannot_be_bypassed_by_flat_transitions() {
    let mut cache=RawSpots::new(1,42).unwrap();
    assert!(cache.set_required((1..=MAX_SYMBOLS as u64).collect()));
    assert!(cache.set_required(HashSet::new()));
    assert!(!cache.set_required(HashSet::from([MAX_SYMBOLS as u64+1])));
}

fn quote_scope(carrier:&Carrier) {
    mark_ready(carrier,HashSet::from([7]),0);
    assert!(carrier.shared.lock().unwrap().spots.as_mut().unwrap().set_required(HashSet::from([7])));
}
fn fresh_spot(carrier:&Carrier,both:bool) {
    let timestamp=crate::engine::wall_ns().unwrap()/1_000_000;
    let mut payload=json!({"ctidTraderAccountId":42,"symbolId":7,"timestamp":timestamp,"bid":100});
    if both {payload["ask"]=json!(101);}
    spot(&carrier.shared,&carrier.dropped,1,&payload,crate::clock::monotonic_ns().unwrap());
}
#[tokio::test]
async fn metadata_ack_does_not_complete_preparation_before_both_original_sides() {
    let carrier=Carrier::new(1,42);quote_scope(&carrier);
    let changes=carrier.changes.subscribe();
    let work=carrier.prepare_evidence(HashSet::from([7]));tokio::pin!(work);
    assert!(futures_util::poll!(work.as_mut()).is_pending());
    fresh_spot(&carrier,false);
    assert!(futures_util::poll!(work.as_mut()).is_pending(),"A bid alone is not a complete original quote");
    fresh_spot(&carrier,true);
    work.await.unwrap();
    assert!(!changes.has_changed().unwrap(),"Quote preparation must not poll account APIs");
}
#[tokio::test]
async fn old_sides_are_not_relabelled_fresh_by_preparation() {
    let carrier=Carrier::new(1,42);quote_scope(&carrier);
    let timestamp=crate::engine::wall_ns().unwrap()/1_000_000-2000;
    spot(&carrier.shared,&carrier.dropped,1,&json!({"ctidTraderAccountId":42,"symbolId":7,"timestamp":timestamp,"bid":100,"ask":101}),crate::clock::monotonic_ns().unwrap()-2_000_000_000);
    let work=carrier.prepare_evidence(HashSet::from([7]));tokio::pin!(work);
    assert!(futures_util::poll!(work.as_mut()).is_pending());
    fresh_spot(&carrier,true);work.await.unwrap();
    assert!(carrier.shared.lock().unwrap().spots.as_ref().unwrap().sides.values().all(|(source,_,_)|*source>timestamp as i64));
}
#[tokio::test]
async fn lost_source_watermark_requires_new_original_sides() {
    let carrier=Carrier::new(1,42);quote_scope(&carrier);fresh_spot(&carrier,true);
    carrier.dropped.store(crate::clock::monotonic_ns().unwrap(),Ordering::Release);
    let work=carrier.prepare_evidence(HashSet::from([7]));tokio::pin!(work);
    assert!(futures_util::poll!(work.as_mut()).is_pending());
    fresh_spot(&carrier,true);work.await.unwrap();
}
#[tokio::test]
async fn cancelling_quote_preparation_does_not_schedule_background_queries() {
    let carrier=Carrier::new(1,42);quote_scope(&carrier);let changes=carrier.changes.subscribe();
    assert!(tokio::time::timeout(std::time::Duration::from_millis(10),carrier.prepare_evidence(HashSet::from([7]))).await.is_err());
    tokio::task::yield_now().await;
    assert!(!changes.has_changed().unwrap());
}

use super::*;
fn intent(cid:&str)->OrderIntent {
    OrderIntent{client_order_id:cid.into(),created_utc_ms:1000,command:IntentCommand::OpenApiClose{
        native_account_id:42,symbol_id:7,side:2,quantity:"0.01".into(),position_id:81,envelope_id:format!("E{cid}")}}
}
fn raw()->Value {json!({"ctidTraderAccountId":"42","executionType":3,"order":{"orderId":72,"positionId":"81","closingOrder":true,
    "orderType":1,"timeInForce":2,"orderStatus":2,"executedVolume":"1","utcLastUpdateTimestamp":"1002",
    "tradeData":{"symbolId":"7","volume":"1","tradeSide":2,"openTimestamp":"1000"}}})}
fn event(cid:&str,payload:Value)->OpenApiEvent {OpenApiEvent{client_message_id:Some(format!("E{cid}")),payload_type:2126,payload,
    account_revision:1,received_at:std::time::Instant::now(),received_wall_time:std::time::SystemTime::now(),received_mono_ns:1000}}
fn resolution(payload:Value)->IntentResolution {IntentResolution{kind:ResolutionKind::BrokerTerminal,
    evidence:json!({"ctid_trader_account_id":42,"order_id":72,"order_status":2,"quantity":"0.01","filled_quantity":"0.01","symbol_id":7,"side":2,"raw":payload})}}
#[tokio::test]
async fn request_is_in_memory_and_duplicate_is_not_resent() {
    let b=InFlightRequests::new(42,4).unwrap();b.begin(intent("A")).await.unwrap();
    assert_eq!(b.pending().await.unwrap().len(),1);assert!(b.begin(intent("A")).await.is_err());
    assert!(b.get("MISSING").await.unwrap().is_none());
    let fresh=InFlightRequests::new(42,4).unwrap();assert!(fresh.pending().await.unwrap().is_empty());
}
#[tokio::test]
async fn response_before_submit_future_resumes_keeps_exact_close_binding() {
    let b=InFlightRequests::new(42,4).unwrap();b.begin(intent("A")).await.unwrap();
    assert_eq!(b.correlate_event(&event("A",raw())).await.unwrap(),Some("A".into()));
    b.written_openapi("A","EA").await.unwrap();b.resolve("A",resolution(raw())).await.unwrap();
    let proof=b.native_close_binding("A").await.unwrap().unwrap();
    assert_eq!(proof["request"]["payloadType"],2111);assert_eq!(proof["request"]["payload"]["positionId"],81);
    assert_eq!(proof["matched_response"]["raw"],raw());assert!(b.pending().await.unwrap().is_empty());
}
#[tokio::test]
async fn missing_position_in_matched_ack_does_not_require_an_extra_rpc() {
    let b=InFlightRequests::new(42,4).unwrap();b.begin(intent("A")).await.unwrap();
    let mut r=raw();r["order"].as_object_mut().unwrap().remove("positionId");
    b.correlate_event(&event("A",r.clone())).await.unwrap();
    assert_eq!(b.native_close_binding("A").await.unwrap().unwrap()["matched_response"]["raw"],r);
}
#[tokio::test]
async fn unknown_envelope_is_not_adopted_from_identical_terms() {
    let b=InFlightRequests::new(42,4).unwrap();b.begin(intent("A")).await.unwrap();
    assert!(b.correlate_event(&event("OTHER",raw())).await.unwrap().is_none());
    assert!(b.native_close_binding("A").await.unwrap().is_none());
}
#[tokio::test]
async fn wrong_account_terms_and_position_are_rejected_before_binding() {
    for field in ["account","side","quantity","position","symbol"] {
        let b=InFlightRequests::new(42,4).unwrap();b.begin(intent("A")).await.unwrap();let mut r=raw();
        match field {"account"=>r["ctidTraderAccountId"]=json!("43"),"side"=>r["order"]["tradeData"]["tradeSide"]=json!(1),
            "quantity"=>r["order"]["tradeData"]["volume"]=json!("2"),"position"=>r["order"]["positionId"]=json!("82"),
            _=>r["order"]["tradeData"]["symbolId"]=json!("8")}
        assert!(b.correlate_event(&event("A",r)).await.is_err(),"{field}");
        assert!(b.native_close_binding("A").await.unwrap().is_none());
    }
}
#[tokio::test]
async fn completed_response_does_not_allow_later_foreign_identity() {
    let b=InFlightRequests::new(42,4).unwrap();b.begin(intent("A")).await.unwrap();
    b.correlate_event(&event("A",raw())).await.unwrap();b.resolve("A",resolution(raw())).await.unwrap();
    let mut changed=raw();changed["order"]["tradeData"]["symbolId"]=json!("8");
    assert!(b.correlate_event(&event("A",changed)).await.is_err());
}
#[tokio::test]
async fn unknown_sent_outcome_never_becomes_not_sent() {
    let b=InFlightRequests::new(42,4).unwrap();b.begin(intent("A")).await.unwrap();b.written_openapi("A","EA").await.unwrap();
    assert!(b.resolve("A",IntentResolution{kind:ResolutionKind::NotSent,evidence:json!({"reason":"timeout"})}).await.is_err());
    assert_eq!(b.pending().await.unwrap().len(),1);
}
#[tokio::test]
async fn capacity_never_evicts_an_unresolved_request() {
    let b=InFlightRequests::new(42,1).unwrap();b.begin(intent("A")).await.unwrap();
    assert!(b.begin(intent("B")).await.is_err());assert!(b.get("A").await.unwrap().is_some());
}
#[tokio::test]
async fn completed_requests_can_be_retired_without_reclaiming_pending() {
    let b=InFlightRequests::new(42,1).unwrap();b.begin(intent("A")).await.unwrap();
    b.correlate_event(&event("A",raw())).await.unwrap();b.resolve("A",resolution(raw())).await.unwrap();
    b.begin(intent("B")).await.unwrap();assert!(b.get("B").await.unwrap().is_some());
    assert!(b.native_close_binding("A").await.unwrap().is_none());
}
#[tokio::test]
async fn conflicting_terminal_summary_is_not_accepted() {
    let b=InFlightRequests::new(42,4).unwrap();b.begin(intent("A")).await.unwrap();
    b.correlate_event(&event("A",raw())).await.unwrap();let mut r=resolution(raw());r.evidence["filled_quantity"]=json!("0.02");
    assert!(b.resolve("A",r).await.is_err());assert_eq!(b.pending().await.unwrap().len(),1);
}

#[tokio::test]
async fn submission_response_lifetime_retains_binding_until_submit_returns() {
 let b=InFlightRequests::new(42,1).unwrap();let guard=b.begin(intent("A")).await.unwrap();
 b.correlate_event(&event("A",raw())).await.unwrap();b.resolve("A",resolution(raw())).await.unwrap();
 assert!(b.begin(intent("B")).await.is_err(),"An event cannot retire correlation while the original submit future still owns it");
 assert!(b.native_close_binding("A").await.unwrap().is_some());drop(guard);
 assert!(b.begin(intent("B")).await.is_ok());
}

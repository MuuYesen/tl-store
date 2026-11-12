use super::*;
use futures_util::{SinkExt, StreamExt};
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use tokio::{net::TcpListener, sync::Mutex as AsyncMutex};
use tokio_tungstenite::{accept_async, tungstenite::Message};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let p = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("ctrader-independent-{}", uuid::Uuid::new_v4()));
        std::fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
        Self(p)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[derive(Default)]
struct Broker {
    operations: Vec<u64>,
    spots_before_execution: bool,
    orders: Vec<Value>,
    deals: Vec<Value>,
    writes: usize,
    lose_reply: bool,
    view_only: bool,
    track_positions: bool,
    positions: Vec<Value>,
    partial_close: bool,
    lose_close_reply: bool,
    close_writes: usize,
    close_frames: Vec<Value>,
}
fn light() -> Value {
    json!({"symbolId":"7","symbolName":"BTCUSD","baseAssetId":"1","quoteAssetId":"2"})
}
fn detail() -> Value {
    json!({"symbolId":"7","digits":2,"minVolume":"1","maxVolume":"1000000","stepVolume":"1","lotSize":"100","enableShortSelling":true})
}
fn assets() -> Value {
    json!([{"assetId":"1","name":"BTC","digits":8},{"assetId":"2","name":"USD","digits":2}])
}
fn trader() -> Value {
    json!({"ctidTraderAccountId":"42","trader":{"ctidTraderAccountId":"42","traderLogin":"12345","depositAssetId":"2","moneyDigits":2,"balance":"10000","balanceVersion":"1","accountType":0,"accessRights":0}})
}
fn command(cid: &str) -> Value {
    json!({"kind":"market","client_order_id":cid,"envelope_id":format!("E{cid}"),"quantity":"0.01","side":1,"light":light(),"detail":detail(),"assets":assets()})
}
async fn peer(state: Arc<AsyncMutex<Broker>>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let state = state.clone();
            tokio::spawn(async move {
                let mut socket = accept_async(socket).await.unwrap();
                while let Some(Ok(Message::Text(text))) = socket.next().await {
                    let req: Value = serde_json::from_str(&text).unwrap();
                    let kind = req["payloadType"].as_u64().unwrap();
                    if kind == 51 {
                        continue;
                    }
                    let mut state = state.lock().await;
                    state.operations.push(kind);
                    let (response, payload) = match kind {
                        2100 => (2101, json!({})),
                        2149 => (
                            2150,
                            json!({"accessToken":"mock-token","permissionScope":if state.view_only{0}else{1},"ctidTraderAccount":[{"ctidTraderAccountId":"42","traderLogin":"12345","isLive":false}]}),
                        ),
                        2102 => (2103, json!({"ctidTraderAccountId":"42"})),
                        2121 => (2122, trader()),
                        2124 => (
                            2125,
                            json!({"ctidTraderAccountId":"42","position":state.positions}),
                        ),
                        2187 => (
                            2188,
                            json!({"ctidTraderAccountId":"42","moneyDigits":2,"positionUnrealizedPnL":state.positions.iter().map(|p|json!({"positionId":p["positionId"],"netUnrealizedPnL":"0"})).collect::<Vec<_>>()}),
                        ),
                        2114 => (2115, json!({"ctidTraderAccountId":"42","symbol":[light()]})),
                        2116 => (
                            2117,
                            json!({"ctidTraderAccountId":"42","symbol":[detail()]}),
                        ),
                        2112 => (2113, json!({"ctidTraderAccountId":"42","asset":assets()})),
                        2175 => (
                            2176,
                            json!({"ctidTraderAccountId":"42","hasMore":false,"order":state.orders}),
                        ),
                        2133 => (
                            2134,
                            json!({"ctidTraderAccountId":"42","hasMore":false,"deal":state.deals}),
                        ),
                        2181 => {
                            let id = number(&req["payload"], "orderId").unwrap();
                            let order = state
                                .orders
                                .iter()
                                .find(|o| number(o, "orderId").unwrap() == id)
                                .unwrap();
                            (
                                2182,
                                json!({"ctidTraderAccountId":"42","order":order,"deal":state.deals.iter().filter(|d|number(d,"orderId").ok()==Some(id)).collect::<Vec<_>>(),"hasMore":false}),
                            )
                        }
                        2106 => {
                            state.writes += 1;
                            let q = &req["payload"];
                            let now = now_ms().unwrap();
                            let order = json!({"orderId":"71","positionId":"81","clientOrderId":q["clientOrderId"],"orderType":1,"timeInForce":3,"orderStatus":2,
                            "executedVolume":q["volume"],"executionPrice":"100.01","utcLastUpdateTimestamp":now,
                            "tradeData":{"symbolId":q["symbolId"],"volume":q["volume"],"tradeSide":q["tradeSide"],"openTimestamp":now}});
                            let deal = json!({"dealId":"91","orderId":"71","positionId":"81","symbolId":"7","volume":q["volume"],"filledVolume":q["volume"],"tradeSide":1,"dealStatus":2,
                            "createTimestamp":now,"executionTimestamp":now,"executionPrice":"100.01","commission":"-1","moneyDigits":2});
                            state.orders.push(order.clone());
                            state.deals.push(deal.clone());
                            if state.track_positions {
                                state.positions = vec![
                                    json!({"positionId":"81","positionStatus":1,"price":"100.01","moneyDigits":2,"usedMargin":"0","utcLastUpdateTimestamp":now,
                                    "tradeData":{"symbolId":"7","tradeSide":1,"volume":q["volume"],"openTimestamp":now}}),
                                ];
                            }
                            if state.lose_reply {
                                let _ = socket.close(None).await;
                                break;
                            }
                            (
                                2126,
                                json!({"ctidTraderAccountId":"42","executionType":3,"order":order,"deal":deal}),
                            )
                        }
                        2127 => (2128, json!({"ctidTraderAccountId":"42"})),
                        2129 => (2130, json!({"ctidTraderAccountId":"42"})),
                        2111 => {
                            let q = &req["payload"];
                            assert!(
                                q.get("symbolId").is_none()
                                    && q.get("tradeSide").is_none()
                                    && q.get("clientOrderId").is_none()
                            );
                            state.close_frames.push(req.clone());
                            state.writes += 1;
                            let oid = 72 + state.close_writes as u64;
                            let did = 92 + state.close_writes as u64;
                            state.close_writes += 1;
                            assert_eq!(number(q, "positionId").unwrap(), 81);
                            let requested = number(q, "volume").unwrap();
                            let position = state
                                .positions
                                .first()
                                .expect("Native preflight must not close an absent position");
                            let remaining = number(&position["tradeData"], "volume").unwrap();
                            assert!(requested > 0 && requested <= remaining);
                            let filled = if state.partial_close {
                                requested / 2
                            } else {
                                requested
                            };
                            assert!(filled > 0);
                            let now = now_ms().unwrap();
                            let order = json!({"orderId":oid,"positionId":"81","orderType":1,"timeInForce":2,"orderStatus":if state.partial_close{1}else{2},
                                "closingOrder":true,"executedVolume":filled,"executionPrice":"100.03","utcLastUpdateTimestamp":now,
                                "tradeData":{"symbolId":"7","tradeSide":2,"volume":requested,"openTimestamp":now}});
                            let deal = json!({"dealId":did,"orderId":oid,"positionId":"81","symbolId":"7","volume":requested,"filledVolume":filled,
                                "tradeSide":2,"dealStatus":if state.partial_close{3}else{2},"createTimestamp":now,"executionTimestamp":now,
                                "executionPrice":"100.03","commission":"-1","moneyDigits":2,
                                "closePositionDetail":{"entryPrice":"100.01","grossProfit":"10","swap":"0","commission":"-1","closedVolume":filled,"moneyDigits":2,"pnlConversionFee":"0"}});
                            state.orders.push(order.clone());
                            state.deals.push(deal.clone());
                            if remaining == filled {
                                state.positions.clear();
                            } else {
                                state.positions[0]["tradeData"]["volume"] =
                                    json!(remaining - filled);
                            }
                            if state.lose_close_reply {
                                let _ = socket.close(None).await;
                                break;
                            }
                            (
                                2126,
                                json!({"ctidTraderAccountId":"42","executionType":3,"order":order,"deal":deal}),
                            )
                        }
                        _ => panic!("Unexpected replay operation {kind}"),
                    };
                    let send_spots = state.spots_before_execution && response == 2126;
                    drop(state);
                    if send_spots {
                        for index in 0..8 {
                            socket.send(Message::Text(json!({"payloadType":2131,"payload":{
                                "ctidTraderAccountId":"42","symbolId":"7","timestamp":now_ms().unwrap(),
                                "bid":10000000+index,"ask":10000100+index}}).to_string().into())).await.unwrap();
                        }
                    }
                    socket.send(Message::Text(json!({"clientMsgId":req["clientMsgId"],"payloadType":response,"payload":payload}).to_string().into())).await.unwrap();
                }
            });
        }
    });
    (endpoint, task)
}
async fn connect(endpoint: &str, _path: PathBuf) -> Connection {
    let mut config = OpenApiConfig::demo(12345);
    config.request_timeout = std::time::Duration::from_secs(1);
    let session = Arc::new(OpenApiAccountSession::new(config.clone()).unwrap());
    let lease = session
        .acquire_connected_at(
            OpenApiClient::connect_to(endpoint, config, crate::openapi::tests::mock_credentials()),
            0,
        )
        .await
        .unwrap();
    Connection::from_lease(lease, false, true, 1000)
        .await
        .unwrap()
}
async fn initialize(c: &Connection) {
    c.request(ReadOnlyRequest::Symbols, json!({}))
        .await
        .unwrap();
    c.request(ReadOnlyRequest::SymbolDetails, json!({"symbolId":[7]}))
        .await
        .unwrap();
    c.request(ReadOnlyRequest::Assets, json!({})).await.unwrap();
    c.snapshot(1000, false).await.unwrap();
    c.funds().await.unwrap();
}

#[tokio::test]
async fn execution_next_consumes_quote_burst_natively_without_a_python_roundtrip() {
    let directory = Directory::new();
    let state = Arc::new(AsyncMutex::new(Broker {
        spots_before_execution: true,
        ..Broker::default()
    }));
    let (endpoint, server) = peer(state).await;
    let c = connect(&endpoint, directory.0.clone()).await;
    initialize(&c).await;
    c.mutate(command("native-filter")).await.unwrap();
    let event = c.next().await.unwrap();
    assert_eq!(
        event["payload_type"], 2126,
        "Execution consumers must not receive quotes they immediately discard"
    );
    assert_eq!(event["payload"]["order"]["clientOrderId"], "native-filter");
    server.abort();
}

#[tokio::test]
async fn market_next_preserves_the_same_original_quote_delivery() {
    let directory = Directory::new();
    let state = Arc::new(AsyncMutex::new(Broker {
        spots_before_execution: true,
        ..Broker::default()
    }));
    let (endpoint, server) = peer(state).await;
    let mut c = connect(&endpoint, directory.0.clone()).await;
    initialize(&c).await;
    c.mutate(command("market-route")).await.unwrap();
    c.requests = None; // The same event route used by non-execution connections.
    let event = c.next().await.unwrap();
    assert_eq!(event["payload_type"], 2131);
    assert_eq!(event["payload"]["bid"], 10000000);
    assert!(event["received_mono_ns"].as_u64().unwrap() > 0);
    assert!(event["payload"]["timestamp"].as_i64().unwrap() > 0);
    server.abort();
}

#[tokio::test]
async fn account_queries_are_not_a_private_three_second_trade_heartbeat() {
    let directory = Directory::new();
    let state = Arc::new(AsyncMutex::new(Broker::default()));
    let (endpoint, server) = peer(state.clone()).await;
    let c = connect(&endpoint, directory.0.clone()).await;
    initialize(&c).await;
    tokio::time::sleep(std::time::Duration::from_millis(3100)).await;
    let result = c.mutate(command("ON-DEMAND")).await.unwrap();
    assert_eq!(result["state"], "execution");
    assert_eq!(state.lock().await.writes, 1);
    server.abort();
}

#[tokio::test]
async fn forged_contract_and_bad_volume_never_touch_wire() {
    let directory = Directory::new();
    let state = Arc::new(AsyncMutex::new(Broker::default()));
    let (endpoint, server) = peer(state.clone()).await;
    let c = connect(&endpoint, directory.0.clone()).await;
    initialize(&c).await;
    let mut bad = command("BAD");
    bad["quantity"] = json!("0.001");
    assert!(c.mutate(bad).await.is_err());
    let mut bad = command("BAD2");
    bad["detail"]["minVolume"] = json!("0");
    assert!(c.mutate(bad).await.is_err());
    assert_eq!(state.lock().await.writes, 0);
    assert!(
        c.requests
            .as_ref()
            .unwrap()
            .pending()
            .await
            .unwrap()
            .is_empty()
    );
    server.abort();
}

#[tokio::test]
async fn three_fresh_startup_observations_share_execution_epoch_and_lease_release_is_scoped() {
    let directory = Directory::new();
    let state = Arc::new(AsyncMutex::new(Broker::default()));
    let (endpoint, server) = peer(state).await;
    let config = OpenApiConfig::demo(12345);
    let session = Arc::new(OpenApiAccountSession::new(config.clone()).unwrap());
    let observer_lease = session
        .acquire_connected_at(
            OpenApiClient::connect_to(
                &endpoint,
                config.clone(),
                crate::openapi::tests::mock_credentials(),
            ),
            0,
        )
        .await
        .unwrap();
    let observer = Connection::from_lease(observer_lease, false, false, 1000)
        .await
        .unwrap();
    let mut last = 0;
    for _ in 0..3 {
        let snapshot = observer.observe().await.unwrap();
        let obs = &snapshot["observation"];
        assert_eq!(
            snapshot["complete"],
            json!({"positions":true,"orders":true})
        );
        assert_eq!(snapshot["positions"], json!([]));
        assert_eq!(snapshot["orders"], json!([]));
        assert!(obs["started_mono_ns"].as_u64().unwrap() > last);
        last = obs["completed_mono_ns"].as_u64().unwrap();
        observer
            .check_epoch(
                obs["epoch"].as_u64().unwrap(),
                obs["revision_after"].as_u64(),
            )
            .await
            .unwrap();
    }
    let execution_lease = session
        .acquire_connected_at(
            OpenApiClient::connect_to(&endpoint, config, crate::openapi::tests::mock_credentials()),
            0,
        )
        .await
        .unwrap();
    let execution = Connection::from_lease(execution_lease, false, true, 1000)
        .await
        .unwrap();
    assert_eq!(observer.lease.epoch(), execution.lease.epoch());
    drop(execution);
    observer
        .check_epoch(observer.lease.epoch(), None)
        .await
        .unwrap();
    assert!(
        observer
            .check_epoch(observer.lease.epoch() + 1, None)
            .await
            .is_err()
    );
    observer.lease.client().close().await;
    assert!(
        observer
            .check_epoch(observer.lease.epoch(), None)
            .await
            .is_err()
    );
    server.abort();
}
#[tokio::test]
async fn view_permission_refuses_execution_before_creating_journal_or_wire_mutation() {
    let directory = Directory::new();
    let state = Arc::new(AsyncMutex::new(Broker {
        view_only: true,
        ..Default::default()
    }));
    let (endpoint, server) = peer(state.clone()).await;
    let config = OpenApiConfig::demo(12345);
    let session = Arc::new(OpenApiAccountSession::new(config.clone()).unwrap());
    let lease = session
        .acquire_connected_at(
            OpenApiClient::connect_to(&endpoint, config, crate::openapi::tests::mock_credentials()),
            0,
        )
        .await
        .unwrap();
    assert!(
        Connection::from_lease(lease, false, true, 1000)
            .await
            .is_err()
    );
    assert!(!directory.0.join("execution-intents.json").exists());
    assert!(!directory.0.join("execution-intents.lock").exists());
    assert_eq!(state.lock().await.writes, 0);
    server.abort();
}

#[tokio::test]
async fn raw_carrier_and_exact_funds_share_one_real_replay_window() {
    let directory = Directory::new();
    let state = Arc::new(AsyncMutex::new(Broker::default()));
    let (endpoint, server) = peer(state).await;
    let c = connect(&endpoint, directory.0.clone()).await;
    initialize(&c).await;
    let mut sample = Value::Null;
    for _ in 0..20 {
        sample = c.funds().await.unwrap();
        if sample["ctrader_raw_snapshot"]["schema_version"] == "leafquant.ctrader-raw-facts.v2" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let raw = &sample["ctrader_raw_snapshot"];
    let funds = &sample["funds"];
    assert_eq!(raw["schema_version"], "leafquant.ctrader-raw-facts.v2");
    assert_eq!(
        raw["capture"]["before"]["mono_ns"],
        funds["started_mono_ns"]
    );
    assert_eq!(
        raw["capture"]["core"]["mono_ns"],
        funds["completed_mono_ns"]
    );
    assert_eq!(raw["capture"]["core"]["utc_ns"], funds["observed_utc_ns"]);
    assert_eq!(funds["raw"]["equity"], "100");
    if let Ok(path) = std::env::var("LEAFQUANT_CTRADER_REPLAY_OUTPUT") {
        std::fs::write(path, serde_json::to_vec(&sample).unwrap()).unwrap();
    }
    server.abort();
}

fn close_command(cid: &str, quantity: &str) -> Value {
    let mut c = command(cid);
    c["kind"] = json!("close");
    c["position_id"] = json!(81);
    c["side"] = json!(2);
    c["quantity"] = json!(quantity);
    c
}
async fn open_owned(c: &Connection, quantity: &str) {
    let mut request = command("OWNED-ENTRY");
    request["quantity"] = json!(quantity);
    assert_eq!(c.mutate(request).await.unwrap()["state"], "execution");
    c.next().await.unwrap();
    c.funds().await.unwrap();
}

#[tokio::test]
async fn standard_execution_has_no_operation_state_files() {
    let directory = Directory::new();
    let state = Arc::new(AsyncMutex::new(Broker::default()));
    let (endpoint, server) = peer(state.clone()).await;
    let c = connect(&endpoint, directory.0.clone()).await;
    initialize(&c).await;
    c.mutate(command("STANDARD-NO-IO")).await.unwrap();
    c.next().await.unwrap();
    assert_eq!(
        std::fs::read_dir(&directory.0).unwrap().count(),
        0,
        "Na adapter must not create an additional execution journal or proof files"
    );
    server.abort();
}
#[tokio::test]
async fn standard_close_has_no_per_order_reconcile_rpc() {
    let directory = Directory::new();
    let state = Arc::new(AsyncMutex::new(Broker {
        track_positions: true,
        ..Default::default()
    }));
    let (endpoint, server) = peer(state.clone()).await;
    let c = connect(&endpoint, directory.0.clone()).await;
    initialize(&c).await;
    open_owned(&c, "0.01").await;
    state.lock().await.operations.clear();
    c.mutate(close_command("STANDARD-CLOSE", "0.01"))
        .await
        .unwrap();
    assert!(
        !state.lock().await.operations.contains(&2124),
        "Explicit close must not add a private Reconcile round-trip before the broker request"
    );
    server.abort();
}

#[tokio::test]
async fn standard_close_keeps_exact_request_binding_without_disk() {
    let d = Directory::new();
    let state = Arc::new(AsyncMutex::new(Broker {
        track_positions: true,
        ..Default::default()
    }));
    let (endpoint, server) = peer(state.clone()).await;
    let c = connect(&endpoint, d.0.clone()).await;
    initialize(&c).await;
    open_owned(&c, "0.01").await;
    let response = c
        .mutate(close_command("STANDARD-BOUND", "0.01"))
        .await
        .unwrap();
    assert_eq!(response["state"], "execution");
    let event = c.next().await.unwrap();
    assert_eq!(event["local_client_order_id"], "STANDARD-BOUND");
    let funds = c.funds().await.unwrap();
    let rows = funds["execution_batch"]["events"].as_array().unwrap();
    let binding = &rows
        .iter()
        .find(|e| e["payload"]["order"]["closingOrder"] == true)
        .unwrap()["request_binding"];
    assert_eq!(
        binding["pending"]["intent"]["client_order_id"],
        "STANDARD-BOUND"
    );
    assert_eq!(binding["request"]["payloadType"], 2111);
    assert_eq!(
        binding["matched_response"]["client_message_id"],
        "ESTANDARD-BOUND"
    );
    assert_eq!(std::fs::read_dir(&d.0).unwrap().count(), 0);
    drop(c);
    let restored = connect(&endpoint, d.0.clone()).await;
    let snapshot = restored.snapshot(10000, false).await.unwrap();
    assert_eq!(
        snapshot["close_bindings"],
        json!({}),
        "Restart must not guess a strategy close CID from history"
    );
    assert_eq!(state.lock().await.close_writes, 1);
    server.abort();
}
#[tokio::test]
async fn standard_lost_reply_stays_unknown_and_is_not_replayed() {
    let d = Directory::new();
    let state = Arc::new(AsyncMutex::new(Broker {
        lose_reply: true,
        ..Default::default()
    }));
    let (endpoint, server) = peer(state.clone()).await;
    let c = connect(&endpoint, d.0.clone()).await;
    initialize(&c).await;
    let response = c.mutate(command("STANDARD-UNKNOWN")).await.unwrap();
    assert_eq!(response["state"], "unknown");
    assert_eq!(
        c.requests.as_ref().unwrap().pending().await.unwrap().len(),
        1
    );
    assert_eq!(state.lock().await.writes, 1);
    drop(c);
    let restored = connect(&endpoint, d.0.clone()).await;
    initialize(&restored).await;
    assert!(
        restored
            .requests
            .as_ref()
            .unwrap()
            .pending()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(state.lock().await.writes, 1);
    assert_eq!(std::fs::read_dir(&d.0).unwrap().count(), 0);
    server.abort();
}

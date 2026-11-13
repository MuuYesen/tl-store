use crate::{engine::Connection, openapi::*};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
};
use serde_json::Value;
use std::{path::PathBuf, sync::Arc, time::Duration};
fn runtime(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}
fn value(e: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(e.to_string())
}
fn operation(name: &str) -> PyResult<ReadOnlyRequest> {
    ReadOnlyRequest::named(name).ok_or_else(|| value("Unknown read-only operation"))
}
// One thin Python view over the existing native book/decoder. It owns no
// connection, OAuth state, execution loop or second quote authority.
#[pyclass(name = "DepthBook", module = "tl_store.ctrader._native")]
pub struct NativeDepthBook {
    inner: crate::openapi::depth::DepthBook,
    account_id: u64,
    symbol_id: u64,
    epoch: u64,
}
fn positive_depth_integer(input: &Bound<'_, PyAny>) -> PyResult<u64> {
    if input.is_instance_of::<pyo3::types::PyBool>() {
        return Err(value("Depth identity/bound must be a positive integer"));
    }
    let number: u64 = input.extract().map_err(value)?;
    if number == 0 { return Err(value("Depth identity/bound must be positive")); }
    Ok(number)
}
impl NativeDepthBook {
    fn serialized_levels(&self) -> PyResult<String> {
        let (bids, asks) = self.inner.levels().map_err(value)?;
        let pairs = |levels: &[(u64, u64)]| levels.iter()
            .map(|(p, q)| [p.to_string(), q.to_string()]).collect::<Vec<_>>();
        serde_json::to_string(&serde_json::json!({
            "bids": pairs(bids), "asks": pairs(asks),
            "complete": false, "source_ts_ns": null,
            "price_scale": 100000, "size_scale": 100
        })).map_err(value)
    }
}
#[pymethods]
impl NativeDepthBook {
    #[new]
    fn new(account_id: &Bound<'_, PyAny>, symbol_id: &Bound<'_, PyAny>,
           epoch: &Bound<'_, PyAny>, max_entries: &Bound<'_, PyAny>) -> PyResult<Self> {
        let account_id = positive_depth_integer(account_id)?;
        let symbol_id = positive_depth_integer(symbol_id)?;
        let epoch = positive_depth_integer(epoch)?;
        let max_entries = usize::try_from(positive_depth_integer(max_entries)?).map_err(value)?;
        let inner = crate::openapi::depth::DepthBook::new(symbol_id, max_entries).map_err(value)?;
        Ok(Self { inner, account_id, symbol_id, epoch })
    }
    fn apply(&mut self, payload_json: &str, epoch: &Bound<'_, PyAny>) -> PyResult<String> {
        let result = (|| {
            if positive_depth_integer(epoch)? != self.epoch {
                return Err(value("Depth connection epoch mismatch"));
            }
            if payload_json.len() > 2 * 1024 * 1024 {
                return Err(value("Depth event exceeds wire bound"));
            }
            let payload: Value = serde_json::from_str(payload_json).map_err(value)?;
            crate::openapi::depth_wire::apply_event(&mut self.inner,
                self.account_id, self.symbol_id, &payload).map_err(value)?;
            self.serialized_levels()
        })();
        if result.is_err() { self.inner.invalidate(); }
        result
    }
    fn levels(&self) -> PyResult<String> { self.serialized_levels() }
    fn invalidate(&mut self) { self.inner.invalidate(); }
}

#[pyclass(module = "tl_store.ctrader._native")]
pub struct AccountSession {
    inner: Arc<OpenApiAccountSession>,
}
#[pymethods]
impl AccountSession {
    #[new]
    #[pyo3(signature=(expected_login,live=false,request_timeout_ms=10000,event_capacity=1024,*,client_id=None,client_secret=None,access_token=None,refresh_token=None,token_state=None,grant_id=None,token_role="owner"))]
    fn new(
        expected_login: u64,
        live: bool,
        request_timeout_ms: u64,
        event_capacity: usize,
        client_id: Option<String>, client_secret: Option<String>,
        access_token: Option<String>, refresh_token: Option<String>,
        token_state: Option<PathBuf>, grant_id: Option<String>, token_role: &str,
    ) -> PyResult<Self> {
        let mut c = OpenApiConfig::demo(expected_login);
        if live {
            c.environment = OpenApiEnvironment::Live;
        }
        c.request_timeout = Duration::from_millis(request_timeout_ms);
        c.event_capacity = event_capacity;
        let explicit = client_id.is_some() || client_secret.is_some() || access_token.is_some()
            || refresh_token.is_some() || token_state.is_some() || grant_id.is_some() || token_role != "owner";
        let inner = if explicit {
            use crate::openapi::oauth::{OAuthConfig, OAuthRole};
            let role = match token_role {
                "owner" => OAuthRole::Owner, "follower" => OAuthRole::Follower,
                _ => return Err(value("OAuth token role must be owner or follower")),
            };
            let required = |value: Option<String>| value.ok_or_else(|| self::value("Incomplete explicit OAuth credentials"));
            let oauth = OAuthConfig::new(role,
                token_state.ok_or_else(|| value("Explicit OAuth token state required"))?,
                required(client_id)?, required(grant_id)?, expected_login, live).map_err(value)?;
            let seed = match (access_token, refresh_token) {
                (Some(access), Some(refresh)) => Some((access, refresh)),
                (None, None) => None,
                _ => return Err(value("Both OAuth seed tokens or neither required")),
            };
            OpenApiAccountSession::new_with_credentials(c, oauth, required(client_secret)?, seed).map_err(value)?
        } else {
            OpenApiAccountSession::new(c).map_err(value)?
        };
        Ok(Self { inner: Arc::new(inner) })
    }
    fn invalidate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            session.invalidate().await;
            Ok(())
        })
    }
    #[pyo3(signature=(execution=false,max_history_records=100000,isolated=false))]
    fn acquire<'py>(
        &self,
        py: Python<'py>,
        execution: bool,
        max_history_records: usize,
        isolated: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let session = if isolated {
                Arc::new(session.isolated_peer().map_err(runtime)?)
            } else {
                session
            };
            let connection = Connection::acquire(
                session,
                execution,
                max_history_records,
            )
            .await
            .map_err(runtime)?;
            Ok(AccountConnection {
                inner: Arc::new(connection),
            })
        })
    }
}
#[pyclass(module = "tl_store.ctrader._native")]
pub struct AccountConnection {
    inner: Arc<Connection>,
}
#[pymethods]
impl AccountConnection {
    #[getter]
    fn account_id(&self) -> PyResult<u64> {
        self.inner
            .lease
            .client()
            .authenticated_account_id()
            .map_err(runtime)
    }
    #[getter]
    fn epoch(&self) -> u64 {
        self.inner.lease.epoch()
    }
    fn request<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        payload_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let op = operation(name)?;
        let payload = serde_json::from_str(payload_json).map_err(value)?;
        let c = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = c.request(op, payload).await.map_err(runtime)?;
            serde_json::to_string(&result).map_err(runtime)
        })
    }
    #[pyo3(signature=(lookback_ms=86400000,require_flat=false))]
    fn snapshot<'py>(
        &self,
        py: Python<'py>,
        lookback_ms: i64,
        require_flat: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let c = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = c
                .snapshot(lookback_ms, require_flat)
                .await
                .map_err(runtime)?;
            serde_json::to_string(&result).map_err(runtime)
        })
    }
    fn state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            serde_json::to_string(&c.state().await.map_err(runtime)?).map_err(runtime)
        })
    }
    fn observe<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            serde_json::to_string(&c.observe().await.map_err(runtime)?).map_err(runtime)
        })
    }
    #[pyo3(signature=(epoch,revision=None))]
    fn check_epoch<'py>(
        &self,
        py: Python<'py>,
        epoch: u64,
        revision: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let c = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            c.check_epoch(epoch, revision).await.map_err(runtime)
        })
    }
    fn funds<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = match c.funds().await {
                Ok(value) => value,
                Err(e)
                    if e.downcast_ref::<crate::engine::FundsRevisionChanged>()
                        .is_some() =>
                {
                    serde_json::json!({"status":"revision_changed"})
                }
                Err(e)
                    if e.downcast_ref::<crate::openapi::funds::FundsObservationChanged>()
                        .is_some() =>
                {
                    serde_json::json!({"status":"observation_changed"})
                }
                Err(e) => return Err(runtime(e)),
            };
            serde_json::to_string(&result).map_err(runtime)
        })
    }
    fn next_event<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = c.next().await.map_err(runtime)?;
            serde_json::to_string(&result).map_err(runtime)
        })
    }
    fn mutate<'py>(&self, py: Python<'py>, command_json: &str) -> PyResult<Bound<'py, PyAny>> {
        let command: Value = serde_json::from_str(command_json).map_err(value)?;
        let c = self.inner.clone();
        let (completion, start) = crate::owned_operation::prepare_owned(async move {
            c.mutate(command).await
        });
        let future = pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = completion.await.map_err(runtime)?.map_err(runtime)?;
            serde_json::to_string(&result).map_err(runtime)
        })?;
        // Allocate the Python completion future before committing the owned request.
        // Dispatch does not wait for that bridge task's first poll.
        start(pyo3_async_runtimes::tokio::get_runtime().handle());
        Ok(future)
    }
    fn halt(&self) {
        self.inner.lease.client().request_close();
    }
    fn acknowledge_executions(&self, through: u64) -> PyResult<()> {
        self.inner.acknowledge_executions(through).map_err(runtime)
    }
    fn pending<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let journal = c
                .requests
                .as_ref()
                .ok_or_else(|| runtime("Read-only connection has no in-flight execution state"))?;
            serde_json::to_string(&journal.pending().await.map_err(runtime)?).map_err(runtime)
        })
    }
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            c.lease.client().close().await;
            Ok(())
        })
    }
}
#[pyfunction]
fn decode(kind: &str, payload_json: &str) -> PyResult<String> {
    let payload: Value = serde_json::from_str(payload_json).map_err(value)?;
    let result = match kind {
        "order" => serde_json::to_value(
            crate::openapi::model::BrokerOrder::from_wire(&payload).map_err(value)?,
        ),
        "deal" => serde_json::to_value(
            crate::openapi::model::BrokerDeal::from_wire(&payload).map_err(value)?,
        ),
        "position" => serde_json::to_value(
            crate::openapi::model::BrokerPosition::from_execution_wire(&payload).map_err(value)?,
        ),
        _ => return Err(value("Unknown broker DTO kind")),
    }
    .map_err(value)?;
    serde_json::to_string(&result).map_err(value)
}
#[pyfunction]
#[pyo3(signature=(state_path,client_id,grant_id,expected_login,access_token,refresh_token,issued_at_secs,expires_in_secs,*,live=false,expected_generation=None))]
#[allow(clippy::too_many_arguments)]
fn import_oauth_token_state(
    state_path: String,
    client_id: String,
    grant_id: String,
    expected_login: u64,
    access_token: String,
    refresh_token: String,
    issued_at_secs: u64,
    expires_in_secs: u64,
    live: bool,
    expected_generation: Option<u64>,
) -> PyResult<()> {
    use crate::openapi::oauth::{OAuthConfig, OAuthRole, OAuthTokenStore};
    let config = OAuthConfig::new(
        OAuthRole::Owner,
        state_path.into(),
        client_id,
        grant_id,
        expected_login,
        live,
    )
    .map_err(value)?;
    let store = OAuthTokenStore::new(config).map_err(value)?;
    match expected_generation {
        None => store.import_initial(access_token, refresh_token, issued_at_secs, expires_in_secs),
        Some(generation) => store.import_reauthorization(
            &access_token,
            &refresh_token,
            issued_at_secs,
            expires_in_secs,
            generation,
        ),
    }
    .map_err(runtime)
}
#[pymodule]
pub fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    m.add_function(wrap_pyfunction!(import_oauth_token_state, m)?)?;
    m.add_class::<AccountSession>()?;
    m.add_class::<AccountConnection>()?;
    m.add_class::<NativeDepthBook>()?;
    Ok(())
}

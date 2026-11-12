// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Managed, account-bound OAuth grants with a single persistent refresh owner.

use super::{OpenApiConfig, OpenApiCredentials};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::watch, task::JoinHandle};

const TOKEN_ENDPOINT: &str = "https://openapi.ctrader.com/apps/token";
const MAX_STATE_BYTES: u64 = 65_536;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Only the owner may rotate the grant; followers consume committed generations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OAuthRole {
    Owner,
    Follower,
}

/// Immutable grant binding. The state directory must be private and shared by all consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OAuthConfig {
    pub role: OAuthRole,
    pub state_path: PathBuf,
    pub client_id: String,
    pub grant_id: String,
    pub expected_login: u64,
    pub is_live: bool,
}
impl OAuthConfig {
    pub fn new(
        role: OAuthRole,
        state_path: PathBuf,
        client_id: String,
        grant_id: String,
        expected_login: u64,
        is_live: bool,
    ) -> Result<Self> {
        ensure!(
            state_path.is_absolute()
                && state_path.file_name().is_some()
                && !state_path
                    .components()
                    .any(|p| matches!(p, std::path::Component::ParentDir)),
            "OAuth state path must be absolute without parent traversal"
        );
        ensure!(
            valid_secret(&client_id)
                && valid_secret(&grant_id)
                && expected_login > 0
                && i64::try_from(expected_login).is_ok(),
            "Invalid OAuth grant binding"
        );
        Ok(Self {
            role,
            state_path,
            client_id,
            grant_id,
            expected_login,
            is_live,
        })
    }
    pub fn from_env(account: &OpenApiConfig) -> Result<Option<Self>> {
        let path = std::env::var_os("CTRADER_OPENAPI_TOKEN_STATE");
        let role = std::env::var("CTRADER_OPENAPI_TOKEN_ROLE").ok();
        if path.is_none()
            && role.is_none()
            && std::env::var_os("CTRADER_OPENAPI_GRANT_ID").is_none()
        {
            return Ok(None);
        }
        let role = match role.as_deref() {
            Some("owner") => OAuthRole::Owner,
            Some("follower") => OAuthRole::Follower,
            _ => bail!("OAuth token role must be owner or follower"),
        };
        Ok(Some(Self::new(
            role,
            PathBuf::from(path.context("Missing OAuth token state path")?),
            required_env("CTRADER_OPENAPI_CLIENT_ID")?,
            required_env("CTRADER_OPENAPI_GRANT_ID")?,
            account.expected_trader_login,
            account.environment.is_live(),
        )?))
    }
    pub fn matches_account(&self, account: &OpenApiConfig) -> bool {
        self.expected_login == account.expected_trader_login
            && self.is_live == account.environment.is_live()
    }
}
pub(super) fn required_env(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|v| valid_secret(v))
        .with_context(|| format!("Missing OAuth credential configuration: {name}"))
}
fn valid_secret(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 16_384
        && value.is_ascii()
        && !value
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
}
#[cfg(unix)]
fn current_uid() -> libc::uid_t {
    // SAFETY: geteuid has no arguments, pointers, or failure mode.
    unsafe { libc::geteuid() }
}
fn now_secs() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("OAuth clock precedes Unix epoch")?
        .as_secs())
}
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct TokenRecord {
    schema_version: u32,
    client_id: String,
    grant_id: String,
    expected_login: u64,
    is_live: bool,
    generation: u64,
    issued_at_secs: u64,
    expires_at_secs: u64,
    status: TokenStatus,
    access_token: String,
    refresh_token: String,
}
impl std::fmt::Debug for TokenRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TokenRecord([REDACTED])")
    }
}
#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum TokenStatus {
    Bootstrap,
    Ready,
    Refreshing,
    Failed,
}
impl TokenRecord {
    fn validate_binding(&self, config: &OAuthConfig) -> Result<()> {
        ensure!(
            self.schema_version == 1
                && self.client_id == config.client_id
                && self.grant_id == config.grant_id
                && self.expected_login == config.expected_login
                && self.is_live == config.is_live,
            "OAuth token identity mismatch"
        );
        ensure!(self.generation > 0, "Invalid OAuth generation");
        Ok(())
    }
    fn validate_for_refresh(&self, config: &OAuthConfig, now: u64) -> Result<()> {
        self.validate_binding(config)?;
        ensure!(
            self.generation > 0
                && self.issued_at_secs > 0
                && self.issued_at_secs <= now
                && (self.expires_at_secs > self.issued_at_secs
                    || (self.status == TokenStatus::Bootstrap
                        && self.expires_at_secs == self.issued_at_secs)),
            "OAuth token is expired or has invalid time bounds"
        );
        ensure!(
            matches!(self.status, TokenStatus::Ready | TokenStatus::Bootstrap),
            "OAuth rotation is incomplete or failed; explicit authorization recovery required"
        );
        ensure!(
            valid_secret(&self.access_token) && valid_secret(&self.refresh_token),
            "Invalid OAuth token pair"
        );
        Ok(())
    }
    fn validate(&self, config: &OAuthConfig, now: u64) -> Result<()> {
        self.validate_for_refresh(config, now)?;
        ensure!(self.status == TokenStatus::Ready && self.expires_at_secs > now,
                "OAuth access token expired or awaiting initial refresh");
        Ok(())
    }
}

/// Explicit token import and atomic persistence; startup never creates token state.
#[derive(Clone, Debug)]
pub struct OAuthTokenStore {
    config: OAuthConfig,
}
impl OAuthTokenStore {
    pub fn new(config: OAuthConfig) -> Result<Self> {
        OAuthConfig::new(
            config.role,
            config.state_path.clone(),
            config.client_id.clone(),
            config.grant_id.clone(),
            config.expected_login,
            config.is_live,
        )?;
        Ok(Self { config })
    }
    /// First-use seed has unknown expiry and cannot admit a connection. An owner
    /// must obtain an actual rotated pair/expiry; never overwrite a committed or
    /// possibly-consumed generation on restart.
    pub fn initialize_seed(&self, access_token: String, refresh_token: String) -> Result<()> {
        ensure!(self.config.role == OAuthRole::Owner, "OAuth follower cannot initialize state");
        ensure!(valid_secret(&access_token) && valid_secret(&refresh_token), "Invalid OAuth seed");
        self.check_directory(true)?;
        let _lock = self.lock_owner()?;
        if self.config.state_path.try_exists().context("Cannot inspect OAuth state existence")? {
            return self.read_record()?.validate_for_refresh(&self.config, now_secs()?);
        }
        let now = now_secs()?;
        self.persist(&TokenRecord {
            schema_version: 1, client_id: self.config.client_id.clone(),
            grant_id: self.config.grant_id.clone(), expected_login: self.config.expected_login,
            is_live: self.config.is_live, generation: 1, issued_at_secs: now,
            expires_at_secs: now, status: TokenStatus::Bootstrap, access_token, refresh_token,
        })
    }
    fn parent(&self) -> Result<&std::path::Path> {
        self.config
            .state_path
            .parent()
            .context("OAuth state directory missing")
    }
    #[cfg(unix)]
    fn check_directory(&self, create: bool) -> Result<()> {
        let parent = self.parent()?;
        if create {
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(parent)
                .map_err(|_| anyhow::anyhow!("Cannot create private OAuth directory"))?;
        }
        let metadata = fs::symlink_metadata(parent)
            .map_err(|_| anyhow::anyhow!("OAuth private directory unavailable"))?;
        ensure!(
            metadata.is_dir()
                && !metadata.file_type().is_symlink()
                && metadata.permissions().mode() & 0o777 == 0o700
                && metadata.uid() == current_uid(),
            "OAuth directory must be owned by this user with mode 0700"
        );
        Ok(())
    }
    #[cfg(windows)]
    fn check_directory(&self, create: bool) -> Result<()> {
        crate::windows_private::check_directory(self.parent()?, create)
    }
    #[cfg(unix)]
    fn open_private(&self, path: &std::path::Path, create: bool) -> Result<File> {
        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(create)
            .custom_flags(libc::O_NOFOLLOW)
            .mode(0o600);
        if create {
            options.create(true);
        }
        let file = options
            .open(path)
            .map_err(|_| anyhow::anyhow!("OAuth private file unavailable"))?;
        let metadata = file
            .metadata()
            .map_err(|_| anyhow::anyhow!("Cannot inspect OAuth private file"))?;
        ensure!(
            metadata.is_file()
                && metadata.permissions().mode() & 0o777 == 0o600
                && metadata.uid() == current_uid(),
            "OAuth file must be owned by this user with mode 0600"
        );
        Ok(file)
    }
    #[cfg(windows)]
    fn open_private(&self, path: &std::path::Path, create: bool) -> Result<File> {
        crate::windows_private::open_file(path, create, false)
    }
    fn lock_owner(&self) -> Result<File> {
        self.check_directory(false)?;
        let name = self
            .config
            .state_path
            .file_name()
            .context("OAuth state file name missing")?
            .to_string_lossy();
        let file = self.open_private(&self.parent()?.join(format!("{name}.lock")), true)?;
        file.try_lock().map_err(|_| {
            anyhow::anyhow!("OAuth grant already has an owner or lock is unavailable")
        })?;
        Ok(file)
    }
    fn read_record(&self) -> Result<TokenRecord> {
        self.check_directory(false)?;
        let file = self.open_private(&self.config.state_path, false)?;
        let mut bytes = Vec::new();
        file.take(MAX_STATE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| anyhow::anyhow!("Cannot read OAuth token state"))?;
        ensure!(
            u64::try_from(bytes.len()).is_ok_and(|length| length <= MAX_STATE_BYTES),
            "OAuth token state exceeds bound"
        );
        let record: TokenRecord = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("Invalid OAuth token state schema"))?;
        record.validate_binding(&self.config)?;
        Ok(record)
    }
    fn read(&self, now: u64) -> Result<TokenRecord> {
        let record = self.read_record()?;
        record.validate(&self.config, now)?;
        Ok(record)
    }
    /// Imports a fresh authorization pair after an operator-controlled OAuth flow.
    ///
    /// The caller supplies the exact generation being replaced; restart never invokes
    /// this method. A possibly consumed pair cannot be recycled as recovery evidence.
    pub fn import_reauthorization(
        &self,
        access_token: &str,
        refresh_token: &str,
        issued_at_secs: u64,
        expires_in_secs: u64,
        expected_generation: u64,
    ) -> Result<()> {
        ensure!(
            self.config.role == OAuthRole::Owner,
            "Only the OAuth owner may import authorization"
        );
        let _lock = self.lock_owner()?;
        let old = self.read_record()?;
        ensure!(
            old.generation == expected_generation,
            "OAuth recovery generation changed"
        );
        let response = serde_json::json!({"accessToken":access_token,"refreshToken":refresh_token,"tokenType":"bearer","expiresIn":expires_in_secs});
        let next = refreshed_record(&old, &response, issued_at_secs)?;
        next.validate(&self.config, now_secs()?)?;
        self.persist(&next)
    }
    /// Imports actual initial authorization response evidence, never replacing existing state.
    /// `issued_at_secs` is the caller's conservative local request-start anchor,
    /// not an inferred broker timestamp; `expires_in_secs` comes from that response.
    pub fn import_initial(
        &self,
        access_token: String,
        refresh_token: String,
        issued_at_secs: u64,
        expires_in_secs: u64,
    ) -> Result<()> {
        ensure!(
            self.config.role == OAuthRole::Owner,
            "Only the OAuth owner may import initial authorization"
        );
        self.check_directory(true)?;
        let _lock = self.lock_owner()?;
        ensure!(
            !self
                .config
                .state_path
                .try_exists()
                .context("Cannot inspect OAuth state existence")?,
            "OAuth initial state already exists; refusing overwrite"
        );
        let record = TokenRecord {
            schema_version: 1,
            client_id: self.config.client_id.clone(),
            grant_id: self.config.grant_id.clone(),
            expected_login: self.config.expected_login,
            is_live: self.config.is_live,
            generation: 1,
            issued_at_secs,
            expires_at_secs: issued_at_secs
                .checked_add(expires_in_secs)
                .context("OAuth expiry overflow")?,
            status: TokenStatus::Ready,
            access_token,
            refresh_token,
        };
        record.validate(&self.config, now_secs()?)?;
        self.persist(&record)
    }
    fn persist(&self, record: &TokenRecord) -> Result<()> {
        self.check_directory(false)?;
        if fs::symlink_metadata(&self.config.state_path).is_ok() {
            self.open_private(&self.config.state_path, false)?;
        }
        let bytes = serde_json::to_vec(record)
            .map_err(|_| anyhow::anyhow!("Cannot encode OAuth token state"))?;
        ensure!(
            u64::try_from(bytes.len()).is_ok_and(|length| length <= MAX_STATE_BYTES),
            "OAuth state exceeds bound"
        );
        let path = self.parent()?.join(format!(
            ".oauth-{}-{}.tmp",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let result = (|| -> Result<()> {
            #[cfg(unix)]
            let mut file = {
                let mut options = OpenOptions::new();
                options.write(true).create_new(true).mode(0o600).custom_flags(libc::O_NOFOLLOW);
                options.open(&path)
            }
                .map_err(|_| anyhow::anyhow!("Cannot create OAuth commit file"))?;
            #[cfg(windows)]
            let mut file = crate::windows_private::open_file(&path, true, true)?;
            file.write_all(&bytes)
                .map_err(|_| anyhow::anyhow!("Cannot write OAuth commit file"))?;
            file.sync_all()
                .map_err(|_| anyhow::anyhow!("Cannot sync OAuth commit file"))?;
            #[cfg(unix)]
            {
                fs::rename(&path, &self.config.state_path)
                    .map_err(|_| anyhow::anyhow!("Cannot atomically commit OAuth state"))?;
                File::open(self.parent()?).and_then(|dir| dir.sync_all())
                    .map_err(|_| anyhow::anyhow!("Cannot sync OAuth directory"))?;
            }
            #[cfg(windows)]
            crate::windows_private::commit(&path, &self.config.state_path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(path);
        }
        result
    }
}

/// No secrets are included in status notifications.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OAuthStatus {
    pub generation: u64,
    pub ready: bool,
    pub expires_at_secs: u64,
}
struct AdmissionState {
    generation: AtomicU64,
    expires_at_secs: AtomicU64,
}
/// A generation-specific, memory-only authorization gate; it does not retain the owner.
#[derive(Clone)]
pub struct OAuthAdmission {
    state: Arc<AdmissionState>,
    generation: u64,
}
impl std::fmt::Debug for OAuthAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthAdmission")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
impl OAuthAdmission {
    pub fn check(&self) -> Result<()> {
        let generation = self.state.generation.load(Ordering::Acquire);
        let expiry = self.state.expires_at_secs.load(Ordering::Acquire);
        let confirmed = self.state.generation.load(Ordering::Acquire);
        ensure!(
            generation == self.generation
                && confirmed == generation
                && generation > 0
                && now_secs()? < expiry,
            "OAuth authorization generation is fenced, expired, or replaced"
        );
        Ok(())
    }
}
struct GrantInner {
    store: OAuthTokenStore,
    secret: String,
    current: Mutex<Option<TokenRecord>>,
    changes: watch::Sender<OAuthStatus>,
    http: reqwest::Client,
    endpoint: String,
    rotation: tokio::sync::Mutex<()>,
    admission: Arc<AdmissionState>,
    owner_lock: Option<Arc<File>>,
}
/// One owner/follower lifecycle. All disk and HTTP refresh work runs in its background task.
pub struct OAuthGrant {
    inner: Arc<GrantInner>,
    task: Mutex<Option<JoinHandle<()>>>,
}
impl std::fmt::Debug for OAuthGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthGrant")
            .field("binding", &self.inner.store.config)
            .finish_non_exhaustive()
    }
}
impl Drop for OAuthGrant {
    fn drop(&mut self) {
        invalidate(&self.inner);
        if let Ok(task) = self.task.get_mut()
            && let Some(task) = task.take()
        {
            task.abort();
        }
    }
}
impl OAuthGrant {
    pub fn start_seeded(config: OAuthConfig, client_secret: String,
                        access_token: String, refresh_token: String) -> Result<Arc<Self>> {
        ensure!(valid_secret(&client_secret), "Invalid OAuth client secret");
        if config.role == OAuthRole::Owner {
            OAuthTokenStore::new(config.clone())?.initialize_seed(access_token, refresh_token)?;
        }
        Self::start(config, client_secret)
    }
    pub fn start(config: OAuthConfig, client_secret: String) -> Result<Arc<Self>> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .https_only(true)
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| anyhow::anyhow!("Cannot initialize OAuth HTTPS transport"))?;
        Self::start_inner(config, client_secret, TOKEN_ENDPOINT.to_owned(), http)
    }
    fn start_inner(
        config: OAuthConfig,
        client_secret: String,
        endpoint: String,
        http: reqwest::Client,
    ) -> Result<Arc<Self>> {
        ensure!(
            valid_secret(&client_secret),
            "OAuth client secret missing or invalid"
        );
        let runtime = tokio::runtime::Handle::try_current()
            .context("OAuth grant requires an async runtime")?;
        let store = OAuthTokenStore::new(config)?;
        let lock = if store.config.role == OAuthRole::Owner {
            Some(Arc::new(store.lock_owner()?))
        } else {
            None
        };
        let now = now_secs()?;
        let record = store.read_record()?;
        record.validate_for_refresh(&store.config, now)?;
        let ready = record.expires_at_secs > now;
        let admission = Arc::new(AdmissionState {
            generation: AtomicU64::new(if ready { record.generation } else { 0 }),
            expires_at_secs: AtomicU64::new(record.expires_at_secs),
        });
        let (changes, _) = watch::channel(OAuthStatus {
            generation: record.generation,
            ready,
            expires_at_secs: record.expires_at_secs,
        });
        let inner = Arc::new(GrantInner {
            store,
            secret: client_secret,
            current: Mutex::new(Some(record)),
            changes,
            http,
            endpoint,
            rotation: tokio::sync::Mutex::new(()),
            admission,
            owner_lock: lock,
        });
        let grant = Arc::new(Self {
            inner: inner.clone(),
            task: Mutex::new(None),
        });
        *grant
            .task
            .lock()
            .map_err(|_| anyhow::anyhow!("OAuth task lock poisoned"))? =
            Some(runtime.spawn(manage(inner)));
        Ok(grant)
    }
    pub fn admission(&self, generation: u64) -> OAuthAdmission {
        OAuthAdmission {
            state: self.inner.admission.clone(),
            generation,
        }
    }
    pub fn config(&self) -> &OAuthConfig {
        &self.inner.store.config
    }
    pub fn subscribe(&self) -> watch::Receiver<OAuthStatus> {
        self.inner.changes.subscribe()
    }
    /// Waits for a committed usable pair during connection startup, never on order paths.
    /// An expired access token can wait for its owner's background refresh without
    /// aborting that refresh when the native client starts.
    pub async fn ready_credentials(&self) -> Result<(u64, OpenApiCredentials)> {
        let mut changes = self.subscribe();
        tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                if let Ok(credentials) = self.credentials() {
                    return Ok(credentials);
                }
                let stopped = self
                    .task
                    .lock()
                    .map_err(|_| anyhow::anyhow!("OAuth task state unavailable"))?
                    .as_ref()
                    .is_none_or(JoinHandle::is_finished);
                ensure!(
                    !stopped,
                    "OAuth refresh lifecycle stopped; authorization recovery required"
                );
                changes
                    .changed()
                    .await
                    .map_err(|_| anyhow::anyhow!("OAuth refresh lifecycle ended"))?;
            }
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!("OAuth credentials did not become ready before startup deadline")
        })?
    }
    /// Memory-only lookup used when establishing a new account session.
    pub fn credentials(&self) -> Result<(u64, OpenApiCredentials)> {
        let current = self
            .inner
            .current
            .lock()
            .map_err(|_| anyhow::anyhow!("OAuth memory state unavailable"))?;
        let record = current.as_ref().context("OAuth grant is not ready")?;
        record.validate(&self.inner.store.config, now_secs()?)?;
        self.admission(record.generation).check()?;
        Ok((
            record.generation,
            OpenApiCredentials {
                client_id: self.inner.store.config.client_id.clone(),
                client_secret: self.inner.secret.clone(),
                access_token: record.access_token.clone(),
            },
        ))
    }
}
fn invalidate(inner: &GrantInner) {
    inner.admission.generation.store(0, Ordering::Release);
    if let Ok(mut current) = inner.current.lock() {
        *current = None;
    }
    let mut status = *inner.changes.borrow();
    status.ready = false;
    inner.changes.send_replace(status);
}
fn publish(inner: &GrantInner, record: TokenRecord) -> Result<()> {
    let status = OAuthStatus {
        generation: record.generation,
        ready: true,
        expires_at_secs: record.expires_at_secs,
    };
    *inner
        .current
        .lock()
        .map_err(|_| anyhow::anyhow!("OAuth memory state unavailable"))? = Some(record);
    inner
        .admission
        .expires_at_secs
        .store(status.expires_at_secs, Ordering::Release);
    inner
        .admission
        .generation
        .store(status.generation, Ordering::Release);
    inner.changes.send_replace(status);
    Ok(())
}
async fn owner_commit(
    owner_lock: Option<Arc<File>>,
    operation: impl FnOnce() -> Result<()> + Send + 'static,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let result = operation();
        // An aborted async caller must not release ownership while this worker
        // can still rename a token generation into the shared state directory.
        drop(owner_lock);
        result
    })
    .await
    .map_err(|_| anyhow::anyhow!("OAuth persistence worker stopped"))?
}
async fn disk_persist(inner: &GrantInner, record: TokenRecord) -> Result<()> {
    let store = inner.store.clone();
    owner_commit(inner.owner_lock.clone(), move || store.persist(&record)).await
}
fn refreshed_record(old: &TokenRecord, response: &Value, now: u64) -> Result<TokenRecord> {
    ensure!(
        response.get("errorCode").is_none_or(Value::is_null)
            && response.get("tokenType").and_then(Value::as_str) == Some("bearer"),
        "OAuth refresh rejected or invalid token type"
    );
    let token = |field| {
        response
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| valid_secret(value))
            .map(str::to_owned)
            .context("OAuth refresh token pair is incomplete")
    };
    let expires = response
        .get("expiresIn")
        .and_then(Value::as_u64)
        .filter(|v| *v > 0)
        .context("OAuth refresh expiry is invalid")?;
    let mut next = old.clone();
    next.access_token = token("accessToken")?;
    next.refresh_token = token("refreshToken")?;
    ensure!(
        next.access_token != old.access_token && next.refresh_token != old.refresh_token,
        "OAuth refresh did not rotate token pair"
    );
    next.generation = old
        .generation
        .checked_add(1)
        .context("OAuth generation exhausted")?;
    next.issued_at_secs = now;
    next.expires_at_secs = now.checked_add(expires).context("OAuth expiry overflow")?;
    next.status = TokenStatus::Ready;
    Ok(next)
}
async fn rotate(inner: &GrantInner) -> Result<()> {
    let _rotation = inner.rotation.lock().await;
    ensure!(
        inner.store.config.role == OAuthRole::Owner,
        "OAuth follower cannot refresh"
    );
    let old = inner
        .current
        .lock()
        .map_err(|_| anyhow::anyhow!("OAuth memory state unavailable"))?
        .as_ref()
        .context("OAuth grant is not ready")?
        .clone();
    let mut intent = old.clone();
    intent.status = TokenStatus::Refreshing;
    invalidate(inner);
    // The durable intent prevents retrying a possibly consumed pair after a crash.
    disk_persist(inner, intent).await?;
    // Anchor validity at request start, conservatively excluding transport time.
    // The broker response has expiresIn, not an independently verifiable issued-at timestamp.
    let validity_anchor = now_secs()?;
    let response = inner
        .http
        .post(&inner.endpoint)
        .query(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", old.refresh_token.as_str()),
            ("client_id", inner.store.config.client_id.as_str()),
            ("client_secret", inner.secret.as_str()),
        ])
        .send()
        .await
        .map_err(|_| anyhow::anyhow!("OAuth refresh transport failed; rotation outcome unknown"))?;
    ensure!(
        response.status().is_success(),
        "OAuth refresh HTTP status {}",
        response.status().as_u16()
    );
    ensure!(
        response
            .content_length()
            .is_none_or(|n| n <= MAX_STATE_BYTES),
        "OAuth refresh response exceeds bound"
    );
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| {
        anyhow::anyhow!("OAuth refresh response read failed; rotation outcome unknown")
    })? {
        ensure!(
            u64::try_from(bytes.len().saturating_add(chunk.len()))
                .is_ok_and(|length| length <= MAX_STATE_BYTES),
            "OAuth refresh response exceeds bound"
        );
        bytes.extend_from_slice(&chunk);
    }
    let value = serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("OAuth refresh response is invalid JSON"))?;
    let next = refreshed_record(&old, &value, validity_anchor)?;
    next.validate(&inner.store.config, now_secs()?)?;
    disk_persist(inner, next.clone()).await?;
    publish(inner, next)
}
async fn manage(inner: Arc<GrantInner>) {
    let Ok(start_wall) = now_secs() else {
        invalidate(&inner);
        return;
    };
    let start_mono = std::time::Instant::now();
    let mut last = inner.current.lock().ok().and_then(|v| v.clone());
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    loop {
        interval.tick().await;
        let Ok(now) = now_secs() else {
            invalidate(&inner);
            return;
        };
        if now.abs_diff(start_wall.saturating_add(start_mono.elapsed().as_secs())) > 5 {
            invalidate(&inner);
            return;
        }
        let observation = inner.rotation.lock().await;
        let store = inner.store.clone();
        let observed = tokio::task::spawn_blocking(move || {
            if store.config.role == OAuthRole::Owner {
                let record = store.read_record()?;
                record.validate_for_refresh(&store.config, now)?;
                Ok(record)
            } else {
                store.read(now)
            }
        })
        .await;
        drop(observation);
        let Ok(Ok(record)) = observed else {
            invalidate(&inner);
            if inner.store.config.role == OAuthRole::Owner {
                return;
            }
            continue;
        };
        if let Some(previous) = &last {
            if record.generation < previous.generation
                || (record.generation == previous.generation && &record != previous)
            {
                invalidate(&inner);
                continue;
            }
            if record.generation == previous.generation
                && !inner.changes.borrow().ready
                && inner.store.config.role == OAuthRole::Follower
            {
                continue;
            }
        }
        if inner.store.config.role == OAuthRole::Owner {
            let current = inner.current.lock().ok().and_then(|value| value.clone());
            if current.as_ref() != Some(&record) {
                invalidate(&inner);
                return;
            }
            let ttl = record.expires_at_secs - record.issued_at_secs;
            let ahead = 60.min((ttl / 2).max(1));
            if now >= record.expires_at_secs - ahead {
                if rotate(&inner).await.is_err() {
                    log::error!("cTrader OAuth refresh failed; authorization recovery required");
                    invalidate(&inner);
                    return;
                }
                last = inner.current.lock().ok().and_then(|v| v.clone());
            }
        } else {
            if last
                .as_ref()
                .is_none_or(|previous| record.generation > previous.generation)
                && publish(&inner, record.clone()).is_err()
            {
                invalidate(&inner);
                return;
            }
            last = Some(record);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(1);
    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "ctrader-oauth-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )))
        }
        fn config(&self, role: OAuthRole) -> OAuthConfig {
            OAuthConfig::new(
                role,
                self.0.join("tokens.json"),
                "synthetic-client".into(),
                "synthetic-grant".into(),
                12345,
                false,
            )
            .unwrap()
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn import(dir: &Directory) -> OAuthTokenStore {
        let store = OAuthTokenStore::new(dir.config(OAuthRole::Owner)).unwrap();
        store
            .import_initial(
                "old-access".into(),
                "old-refresh".into(),
                now_secs().unwrap(),
                3600,
            )
            .unwrap();
        store
    }
    #[rstest::rstest]
    fn explicit_import_is_private_exact_and_never_overwrites_existing_state() {
        let dir = Directory::new();
        let store = import(&dir);
        let record = store.read(now_secs().unwrap()).unwrap();
        assert_eq!(record.generation, 1);
        assert_eq!(record.access_token, "old-access");
        assert_eq!(record.refresh_token, "old-refresh");
        assert_eq!(record.expires_at_secs - record.issued_at_secs, 3600);
        assert_eq!(
            fs::metadata(&dir.0).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&store.config.state_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert!(
            store
                .import_initial(
                    "replacement".into(),
                    "replacement".into(),
                    now_secs().unwrap(),
                    60
                )
                .is_err()
        );
        assert_eq!(
            store.read(now_secs().unwrap()).unwrap().access_token,
            "old-access"
        );
    }
    #[rstest::rstest]
    fn absent_expired_foreign_or_overpermissive_state_cannot_admit_credentials() {
        let dir = Directory::new();
        let store = OAuthTokenStore::new(dir.config(OAuthRole::Owner)).unwrap();
        assert!(store.read(now_secs().unwrap()).is_err());
        let store = import(&dir);
        let record = store.read(now_secs().unwrap()).unwrap();
        assert!(store.read(record.expires_at_secs).is_err());
        let mut foreign = store.config.clone();
        foreign.expected_login = 999;
        assert!(
            OAuthTokenStore::new(foreign)
                .unwrap()
                .read(now_secs().unwrap())
                .is_err()
        );
        fs::set_permissions(&store.config.state_path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(store.read(now_secs().unwrap()).is_err());
    }
    #[tokio::test]
    async fn one_owner_lock_survives_state_rename_and_follower_never_refreshes() {
        let dir = Directory::new();
        let store = import(&dir);
        let owner =
            OAuthGrant::start(dir.config(OAuthRole::Owner), "synthetic-secret".into()).unwrap();
        let record = store.read(now_secs().unwrap()).unwrap();
        store.persist(&record).unwrap();
        assert!(
            OAuthGrant::start(dir.config(OAuthRole::Owner), "synthetic-secret".into()).is_err()
        );
        let follower =
            OAuthGrant::start(dir.config(OAuthRole::Follower), "synthetic-secret".into()).unwrap();
        assert_eq!(follower.credentials().unwrap().0, 1);
        assert!(!format!("{owner:?}").contains("old-access"));
        assert!(!format!("{owner:?}").contains("synthetic-secret"));
        drop(owner);
        drop(follower);
    }
    #[rstest::rstest]
    fn refresh_response_is_strict_and_error_text_cannot_echo_secrets() {
        let dir = Directory::new();
        let store = import(&dir);
        let old = store.read(now_secs().unwrap()).unwrap();
        let valid = serde_json::json!({"accessToken":"new-access", "refreshToken":"new-refresh", "tokenType":"bearer", "expiresIn":100, "errorCode":null});
        let next = refreshed_record(&old, &valid, now_secs().unwrap()).unwrap();
        assert_eq!(next.generation, 2);
        assert_eq!(next.access_token, "new-access");
        for field in ["accessToken", "refreshToken", "tokenType", "expiresIn"] {
            let mut bad = valid.clone();
            bad.as_object_mut().unwrap().remove(field);
            assert!(refreshed_record(&old, &bad, now_secs().unwrap()).is_err());
        }
        let error = refreshed_record(&old, &serde_json::json!({"errorCode":"token-old-access-synthetic-secret", "description":"old-refresh"}), now_secs().unwrap()).unwrap_err();
        assert!(!error.to_string().contains("old-access"));
        assert!(!error.to_string().contains("synthetic-secret"));
    }
    fn start_at_endpoint(
        config: OAuthConfig,
        secret: String,
        endpoint: String,
    ) -> Result<Arc<OAuthGrant>> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .no_proxy()
            .build()
            .unwrap();
        OAuthGrant::start_inner(config, secret, endpoint, http)
    }
    async fn refresh_peer(
        status: u16,
        response: serde_json::Value,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/apps/token", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut byte = [0; 1];
            while !request.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                assert!(request.len() < 16384);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("POST /apps/token?"));
            assert!(request.contains("grant_type=refresh_token"));
            assert!(request.contains("refresh_token=old-refresh"));
            assert!(request.contains("client_id=synthetic-client"));
            let body = response.to_string();
            stream.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        (endpoint, peer)
    }
    #[tokio::test]
    async fn real_http_rotation_persists_pair_before_follower_can_admit_generation_two() {
        let dir = Directory::new();
        let store = import(&dir);
        let (endpoint, server) = refresh_peer(200, serde_json::json!({"accessToken":"new-access", "refreshToken":"new-refresh", "tokenType":"bearer", "expiresIn":100, "errorCode":null})).await;
        let owner = start_at_endpoint(
            dir.config(OAuthRole::Owner),
            "synthetic-secret".into(),
            endpoint,
        )
        .unwrap();
        let follower =
            OAuthGrant::start(dir.config(OAuthRole::Follower), "synthetic-secret".into()).unwrap();
        let mut changed = follower.subscribe();
        rotate(&owner.inner).await.unwrap();
        assert_eq!(owner.credentials().unwrap().0, 2);
        let persisted = store.read(now_secs().unwrap()).unwrap();
        assert_eq!(persisted.refresh_token, "new-refresh");
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                changed.changed().await.unwrap();
                if changed.borrow().ready && changed.borrow().generation == 2 {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(follower.credentials().unwrap().1.access_token, "new-access");
        server.await.unwrap();
    }
    #[tokio::test]
    async fn http_failure_is_redacted_and_durable_intent_forbids_old_token_restart() {
        let dir = Directory::new();
        let store = import(&dir);
        let (endpoint, server) = refresh_peer(
            400,
            serde_json::json!({"description":"old-access old-refresh synthetic-secret"}),
        )
        .await;
        let owner = start_at_endpoint(
            dir.config(OAuthRole::Owner),
            "synthetic-secret".into(),
            endpoint,
        )
        .unwrap();
        let error = rotate(&owner.inner).await.unwrap_err().to_string();
        assert!(!error.contains("old-refresh"));
        assert!(!error.contains("synthetic-secret"));
        assert!(owner.credentials().is_err());
        assert!(store.read(now_secs().unwrap()).is_err());
        drop(owner);
        assert!(
            OAuthGrant::start(dir.config(OAuthRole::Owner), "synthetic-secret".into()).is_err()
        );
        server.await.unwrap();
    }
    #[tokio::test]
    async fn follower_rejects_generation_rollback_and_same_generation_pair_replacement() {
        let dir = Directory::new();
        let store = import(&dir);
        let old = store.read(now_secs().unwrap()).unwrap();
        let follower =
            OAuthGrant::start(dir.config(OAuthRole::Follower), "synthetic-secret".into()).unwrap();
        let mut changes = follower.subscribe();
        let mut next = old.clone();
        next.generation = 2;
        next.access_token = "new-access".into();
        next.refresh_token = "new-refresh".into();
        store.persist(&next).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                changes.changed().await.unwrap();
                if changes.borrow().generation == 2 && changes.borrow().ready {
                    break;
                }
            }
        })
        .await
        .unwrap();
        store.persist(&old).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                changes.changed().await.unwrap();
                if !changes.borrow().ready {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(follower.credentials().is_err());
        let mut changed = next;
        changed.refresh_token = "foreign-same-generation".into();
        store.persist(&changed).unwrap();
        // A sealed follower cannot resume from a different pair under the generation it consumed.
        let mut status = changes.clone();
        tokio::time::timeout(Duration::from_secs(2), status.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(follower.credentials().is_err());
    }
    #[tokio::test]
    async fn owner_automatically_refreshes_from_actual_response_expiry() {
        let dir = Directory::new();
        let store = OAuthTokenStore::new(dir.config(OAuthRole::Owner)).unwrap();
        store
            .import_initial(
                "old-access".into(),
                "old-refresh".into(),
                now_secs().unwrap(),
                2,
            )
            .unwrap();
        let (endpoint, server) = refresh_peer(200, serde_json::json!({"accessToken":"new-access", "refreshToken":"new-refresh", "tokenType":"bearer", "expiresIn":3600, "errorCode":null})).await;
        let owner = start_at_endpoint(
            dir.config(OAuthRole::Owner),
            "synthetic-secret".into(),
            endpoint,
        )
        .unwrap();
        let mut changes = owner.subscribe();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                changes.changed().await.unwrap();
                if changes.borrow().generation == 2 && changes.borrow().ready {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(owner.credentials().unwrap().0, 2);
        let record = store.read(now_secs().unwrap()).unwrap();
        assert_eq!(record.expires_at_secs - record.issued_at_secs, 3600);
        server.await.unwrap();
    }
    #[rstest::rstest]
    fn symlink_and_incomplete_rotation_state_cannot_be_used_as_ready_tokens() {
        let dir = Directory::new();
        let store = import(&dir);
        let mut record = store.read(now_secs().unwrap()).unwrap();
        record.status = TokenStatus::Refreshing;
        store.persist(&record).unwrap();
        assert!(store.read(now_secs().unwrap()).is_err());
        record.status = TokenStatus::Ready;
        let target = dir.0.join("other.json");
        fs::rename(&store.config.state_path, &target).unwrap();
        std::os::unix::fs::symlink(&target, &store.config.state_path).unwrap();
        assert!(store.read(now_secs().unwrap()).is_err());
        assert!(store.persist(&record).is_err());
    }
    #[tokio::test]
    async fn successful_remote_rotation_with_failed_local_commit_never_republishes_old_pair() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            sync::oneshot,
        };
        let dir = Directory::new();
        let store = import(&dir);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/apps/token", listener.local_addr().unwrap());
        let (received_tx, received) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut byte = [0; 1];
            while !bytes.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).await.unwrap();
                bytes.push(byte[0]);
                assert!(bytes.len() < 16384);
            }
            received_tx.send(()).unwrap();
            released.await.unwrap();
            let body = serde_json::json!({"accessToken":"new-access", "refreshToken":"new-refresh", "tokenType":"bearer", "expiresIn":3600}).to_string();
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        let owner = start_at_endpoint(
            dir.config(OAuthRole::Owner),
            "synthetic-secret".into(),
            endpoint,
        )
        .unwrap();
        let inner = owner.inner.clone();
        let rotation = tokio::spawn(async move { rotate(&inner).await });
        // The request can only arrive after the durable refreshing fence has committed.
        received.await.unwrap();
        fs::set_permissions(&dir.0, fs::Permissions::from_mode(0o500)).unwrap();
        release.send(()).unwrap();
        assert!(rotation.await.unwrap().is_err());
        assert!(owner.credentials().is_err());
        fs::set_permissions(&dir.0, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(store.read(now_secs().unwrap()).is_err());
        let persisted: Value =
            serde_json::from_slice(&fs::read(&store.config.state_path).unwrap()).unwrap();
        assert_eq!(persisted["status"], "refreshing");
        assert_eq!(persisted["generation"], 1);
        assert_eq!(persisted["access_token"], "old-access");
        server.await.unwrap();
    }
    #[rstest::rstest]
    fn explicit_reauthorization_recovers_fenced_state_only_with_expected_generation_and_new_pair() {
        let dir = Directory::new();
        let store = import(&dir);
        let mut record = store.read(now_secs().unwrap()).unwrap();
        record.status = TokenStatus::Refreshing;
        store.persist(&record).unwrap();
        assert!(
            store
                .import_reauthorization("new-access", "new-refresh", now_secs().unwrap(), 3600, 2)
                .is_err()
        );
        assert!(
            store
                .import_reauthorization("old-access", "old-refresh", now_secs().unwrap(), 3600, 1)
                .is_err()
        );
        store
            .import_reauthorization("new-access", "new-refresh", now_secs().unwrap(), 3600, 1)
            .unwrap();
        assert_eq!(store.read(now_secs().unwrap()).unwrap().generation, 2);
        assert!(
            store
                .import_reauthorization(
                    "third-access",
                    "third-refresh",
                    now_secs().unwrap(),
                    3600,
                    1
                )
                .is_err()
        );
    }
    #[tokio::test]
    async fn synchronous_generation_guard_fences_before_any_async_epoch_watcher() {
        let dir = Directory::new();
        let store = import(&dir);
        let grant =
            OAuthGrant::start(dir.config(OAuthRole::Owner), "synthetic-secret".into()).unwrap();
        let old = grant.admission(1);
        assert!(old.check().is_ok());
        let record = store.read(now_secs().unwrap()).unwrap();
        invalidate(&grant.inner);
        assert!(old.check().is_err());
        let next = refreshed_record(&record, &serde_json::json!({"accessToken":"new-access", "refreshToken":"new-refresh", "tokenType":"bearer", "expiresIn":7200}), now_secs().unwrap()).unwrap();
        store.persist(&next).unwrap();
        publish(&grant.inner, next).unwrap();
        assert!(old.check().is_err());
        assert!(grant.admission(2).check().is_ok());
        tokio::task::yield_now().await;
    }
    #[tokio::test]
    async fn aborted_async_persistence_retains_owner_lock_until_blocking_commit_finishes() {
        use tokio::sync::oneshot;
        let dir = Directory::new();
        let store = import(&dir);
        let grant =
            OAuthGrant::start(dir.config(OAuthRole::Owner), "synthetic-secret".into()).unwrap();
        let manager = grant.task.lock().unwrap().take().unwrap();
        manager.abort();
        let _ = manager.await;
        let lock = grant.inner.owner_lock.clone();
        let record = store.read(now_secs().unwrap()).unwrap();
        let commit_store = store.clone();
        let (started_tx, started) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let (finished_tx, finished) = oneshot::channel();
        let task = tokio::spawn(owner_commit(lock, move || {
            started_tx.send(()).unwrap();
            released.recv().unwrap();
            let result = commit_store.persist(&record);
            finished_tx.send(()).unwrap();
            result
        }));
        started.await.unwrap();
        task.abort();
        let _ = task.await;
        drop(grant);
        let blocked =
            OAuthGrant::start(dir.config(OAuthRole::Owner), "synthetic-secret".into()).unwrap_err();
        assert!(blocked.to_string().contains("already has an owner"));
        release.send(()).unwrap();
        finished.await.unwrap();
        let replacement = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(owner) =
                    OAuthGrant::start(dir.config(OAuthRole::Owner), "synthetic-secret".into())
                {
                    break owner;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(replacement.credentials().unwrap().0, 1);
    }
    #[tokio::test]
    async fn expired_ready_owner_state_refreshes_after_restart_without_reusing_access_token() {
        let dir = Directory::new();
        let store = import(&dir);
        let mut expired = store.read(now_secs().unwrap()).unwrap();
        expired.issued_at_secs = now_secs().unwrap() - 100;
        expired.expires_at_secs = now_secs().unwrap() - 1;
        store.persist(&expired).unwrap();
        let expired_follower =
            OAuthGrant::start(dir.config(OAuthRole::Follower), "synthetic-secret".into()).unwrap();
        assert!(expired_follower.credentials().is_err());
        drop(expired_follower);
        let (endpoint, server) = refresh_peer(200, serde_json::json!({"accessToken":"new-access", "refreshToken":"new-refresh", "tokenType":"bearer", "expiresIn":3600})).await;
        let owner = start_at_endpoint(
            dir.config(OAuthRole::Owner),
            "synthetic-secret".into(),
            endpoint,
        )
        .unwrap();
        assert!(owner.admission(1).check().is_err());
        let mut changes = owner.subscribe();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                changes.changed().await.unwrap();
                if changes.borrow().generation == 2 && changes.borrow().ready {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(owner.credentials().unwrap().0, 2);
        server.await.unwrap();
    }
    #[tokio::test]
    async fn supplied_seed_has_no_invented_expiry_and_restart_preserves_rotated_state() {
        let dir = Directory::new();
        let config = dir.config(OAuthRole::Owner);
        let seed = OAuthTokenStore::new(config.clone()).unwrap();
        seed.initialize_seed("seed-access".into(), "seed-refresh".into()).unwrap();
        let record = seed.read_record().unwrap();
        assert!(record.status == TokenStatus::Bootstrap);
        assert_eq!(record.expires_at_secs, record.issued_at_secs);
        assert!(record.validate(&config, now_secs().unwrap()).is_err());
        record.validate_for_refresh(&config, now_secs().unwrap()).unwrap();
        // Existing managed owner/follower path resumes; seed never overwrites it.
        let next = refreshed_record(&record, &serde_json::json!({
            "accessToken":"rotated-access", "refreshToken":"rotated-refresh",
            "tokenType":"bearer", "expiresIn":3600}), now_secs().unwrap()).unwrap();
        seed.persist(&next).unwrap();
        seed.initialize_seed("stale-access".into(), "stale-refresh".into()).unwrap();
        assert_eq!(seed.read(now_secs().unwrap()).unwrap().access_token, "rotated-access");
        let follower = OAuthGrant::start(dir.config(OAuthRole::Follower), "synthetic-secret".into()).unwrap();
        assert_eq!(follower.credentials().unwrap().0, 2);
        let mut failed = next.clone(); failed.status = TokenStatus::Refreshing;
        seed.persist(&failed).unwrap();
        assert!(seed.initialize_seed("stale-access".into(), "stale-refresh".into()).is_err());
    }

    #[tokio::test]
    async fn bootstrap_refresh_uses_actual_http_expiry_and_persists_before_admission() {
        let dir = Directory::new();
        let config = dir.config(OAuthRole::Owner);
        let store = OAuthTokenStore::new(config.clone()).unwrap();
        store.initialize_seed("old-access".into(), "old-refresh".into()).unwrap();
        let (endpoint, server) = refresh_peer(200, serde_json::json!({
            "accessToken":"new-access", "refreshToken":"new-refresh",
            "tokenType":"bearer", "expiresIn":100})).await;
        let owner = start_at_endpoint(config.clone(), "synthetic-secret".into(), endpoint).unwrap();
        let (generation, credentials) = owner.ready_credentials().await.unwrap();
        assert_eq!(generation, 2);
        assert_eq!(credentials.access_token, "new-access");
        let record = store.read(now_secs().unwrap()).unwrap();
        assert_eq!(record.expires_at_secs - record.issued_at_secs, 100);
        assert_eq!(record.generation, generation);
        assert!(OAuthGrant::start(config, "synthetic-secret".into()).is_err());
        server.await.unwrap();
    }

}

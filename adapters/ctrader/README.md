# cTrader built-in adapter

cTrader is included in the `tl-store==0.2.1a3` alpha distribution and exposed as
`tl_store.ctrader`; it is not a separate cTrader distribution. Distribution metadata
and release builds belong to the collection root. The protocol crate is 0.2.1; the instance-credential constructor and metadata
requests require a newly compiled native extension. Earlier wheels are not relabelled. See the collection README for exact
installation commands and publication status.

## Dependencies and capabilities

Requires unmodified official `nautilus_trader==2.0.0rc7` and CPython 3.13. Standalone
NA factories depend on neither TL nor a monitoring service. Optional NautilusStore
integration requires a separately installed TL with **unified Store adapter API 2, Backtrader interface 1**;
published TL 0.2.9 is insufficient and is rejected when the binding is selected.
TL owns the one-way `tl-store` extra, so TL users install `trade-learn[nautilus,tl-store]` without naming an adapter distribution. The actual selected distribution is tl-store, containing cTrader. It has no reverse dependency on TL. The target command is not public-PyPI usable until compatible versions are published.
Stop/StopLimit, trailing, Limit and linked Bracket/OCO remain adapter implementation
gaps, rather than a claim that the broker protocol lacks those capabilities.
The TL bridge has separate gaps; a factory or enum name does not prove venue support.
No NA emulation is implicitly enabled.
Matching versions alone do not authenticate the provenance of an installed binary.

Capabilities: broker-resolved instruments, Open API quotes, plain Market IOC
orders, cancellation, typed position closes, execution reports and account
observations. The optional Store uses official NA internal SECOND/MINUTE/HOUR/DAY
BID/ASK/MID bars from quotes. LAST, external bars, tick-count and volume bars are
rejected. Bar timing, empty intervals and timezone policies belong to
LiveDataEngineConfig; the Store adds no resampling loop. Quoted size is unavailable
and is represented as zero.

The simple, unpublished candidate entry is:

```python
store = bt.stores.NautilusStore(
    adapter="ctrader", account=12345678,
    client_id="CLIENT_ID", client_secret="CLIENT_SECRET",
    access_token="ACCESS_TOKEN", refresh_token="REFRESH_TOKEN", practice=True,
)
data = store.getdata(dataname="BTCUSD", timeframe=bt.TimeFrame.Seconds,
                     compression=1, price="mid")
```

`account` is the positive broker login, never the internal cTrader account ID.
The existing authenticated account-list/trader checks resolve and verify that ID.
`practice` must be a boolean: True selects Demo/SANDBOX, False selects Real/LIVE.
Simple feed requests support Seconds, Minutes, Days, Weeks, Months and Years, positive integer compression,
and MicroSeconds only when compression is an exact whole number of milliseconds.
Calendar boundaries are delegated to the pinned NA aggregator; interval overflow is rejected.
Prices accept lowercase bid/ask/mid. Unsupported combinations, unknown parameters and mixing
native/simple forms fail explicitly. Bars use the official NA internal aggregator,
with `time_bars_build_with_no_updates=False`; there is no added tick callback,
empty-second synthesis or automatic historical backfill. Exact symbol-name lookup
and broker symbol-category/asset-class joins happen after authentication. Missing,
ambiguous or unsupported metadata is an error; symbol names are not classified by
heuristic. Current class-name mapping is explicit in provider.py.

Simple Store optionally accepts `trader_id` and `strategy_id` (official NA identity
fields, each defaults to `TL-001`), `economics` (`VerifiedDealEconomics`),
`require_flat_on_connect`, `queue_capacity`, `max_history_records`,
`history_lookback_ms` and `request_timeout_ms`. These map to the existing
`CTraderConfig` fields, retaining their native defaults and bounds; they do not
create another account/symbol/routing declaration. Admission flags require booleans
and transport bounds require integers. Missing fee proof remains a trading gate.

Advanced native connection options remain in CTraderConfig. CTraderStoreConfig combines these
with official LiveNodeConfig, optional StrategyConfig, local state root and an
optional authoritative account provider. Configure exactly one `CTRADER` data
client and execution client. Account ID derives from options.expected_login;
conflicting native declarations fail. Cash/equity reads require a qualified
account_provider rather than an inferred balance. There is one shared session per
Store, one existing TL Runtime/Broker/host and the NA loop, with no global singleton
or application profile.

The `tradelearn.store_adapters/ctrader` entry point loads build_adapter(config).
Select `bt.stores.NautilusStore(adapter="ctrader", config=...)`; getdata accepts an
internal BarType such as `SYMBOL.CTRADER-1-SECOND-MID-INTERNAL`. Constructor and
getdata/getbroker do not connect or read secrets. Cerebro.run owns startup and
cleanup. Unknown parameters fail. Each Store is single use; preserve account-scoped
durable state across restarts. Strategies and account configuration remain outside
this component.

Explicit `CTraderCredentials` is per instance and its repr is redacted. It is also
available to standalone NA users through `CTraderConfig(credentials=...)`, without
importing TL. Supplied values never populate or fall back to process environment.
Construction/getdata/getbroker do not load token files or create OAuth tasks.

On first run the toolkit creates a private 0700 directory/0600 atomic state file
under `~/.tradelearn/oauth/<grant-digest>/tokens.json`. The digest binds the original
client/refresh seed; the same seed cannot silently become separate refresh owners
for different logins/environments. Tokens have no inferred expiry: first use saves
an unready seed and obtains a real OAuth refresh response before session admission.
A persisted rotating/failed state stays blocked; a crash never retries a possibly
consumed token pair. Restart with the same seed resumes the committed generation
without overwriting refreshed credentials. Keep the toolkit state directory.
Changing the initial seed selects a different state identity; this is not a way to
recover an uncertain refresh.

Existing managed owner/follower grants use advanced `CTraderCredentials` with their
explicit `state_path`, `grant_id` and `role`, or simple Store `token_state`, `grant_id`,
`token_role` fields mapped directly to those settings. Existing-state users may omit
both seed tokens; explicit state and grant identity are then mandatory. Followers never initialize or rotate;
the original refresh owner/lock remains authoritative. Explicit
`import_oauth_token_state` retains operator-controlled initial import and generation
CAS recovery. The legacy native configuration without instance credentials keeps
its existing CTRADER_OPENAPI_* environment binding; independent grants should use
the explicit path. Do not run a new owner beside an existing owner to reuse a grant.

Monitoring is optional and uses TL's actual SQS credentials interface. The same
sender credentials may be reused by a separately installed identity client; this
adapter does not implement a signing service, fetch enrollment, add a platform
token or assume cloud verification has occurred. No endpoint+token API is promised.

数据说明：仅上传运行日志，不上传策略源码、券商密码、交易API密钥或OAuth凭据。

## Release builds and supported targets

| Target | Native build | Qualification |
|---|---|---|
| macOS 11+ arm64, CPython 3.13 | Optimized Release, opt-level 3, Rust 1.97.0 | Local wheel installation, official NA assembly and offline callback/cleanup tests |
| Linux x86_64, glibc 2.34+, CPython 3.13 | Optimized 0.2.1a3 Release, Rust 1.97.0, manylinux 2.34 audit | 2026-11-10 offline clean standalone/TL/base installation qualified; live deployment pending |

The 0.2.1a1 Linux wheel is historical evidence for its own source and cannot
validate the new credential ABI. Current source, Cargo.lock, toolchain, flags and
artifact correspondence must be recorded with the new release review. No other platform, live broker or GitHub CI run is certified.

For a new build, use Rust 1.97.0, CPython 3.13 and maturin 1.13.1. From the collection root:

```sh
python -m pip install "maturin==1.13.1"
python tools/build_release.py --interpreter python3.13 --out raw-dist
python tools/finalize_wheel.py raw-dist/*.whl dist/$(basename raw-dist/*.whl)
maturin sdist --out dist
```

The release wrapper requires the maturin CLI on PATH and checks both its version
and the imported package metadata. For a `--target` tooling installation, expose
the target scripts directory on PATH and make the target package importable; the
`python -m maturin` launcher does not resolve that scripts directory from PATH.

The release wrapper adds path remapping and, on qualified Linux builders,
`--compatibility manylinux_2_34 --auditwheel check`. The finalizer normalizes local
SBOM references and rebuilds RECORD without changing native bytes or wheel tags.
The audit must succeed; renaming a native Linux wheel is not a substitute. CI uses
a Debian 12 Python base and this same release profile. Cargo.lock is committed;
there are no NA Rust dependencies or official-package patches. Rebuilding requires
the locked crates (or an existing Cargo registry cache); end users should install
qualified wheels instead of compiling by default.

From the collection root run `cargo test --locked --manifest-path adapters/ctrader/native/Cargo.toml --lib` for synthetic
protocol tests and `python -m pytest adapters/ctrader/tests --ignore=adapters/ctrader/tests/test_store.py` for
standalone installed-adapter tests. Store tests additionally need the qualified TL
API 2 artifact. Tests use synthetic identities, local loopback and deterministic
replay; they are not live account, AWS or venue certification. All runtime/test
source, Cargo.lock, COPYING, COPYING.LESSER, NOTICE and locked dependency licenses
are included in the sdist. Existing LGPL copyright notices are retained.


The source candidate also provides an explicit macOS arm64 to Linux x86_64
cross-build entry. This does not certify a wheel until the Linux checks pass.
Run `python tools/export_target_python_abi.py > target-abi.json` on the pinned
Linux CPython 3.13 target. Preserve that JSON and decode its `sysconfigdata.base64`
to its original basename in a dedicated target-Python directory. The build helper
verifies both the raw file and digest. With Rust 1.97.0 host/target standard
libraries, maturin 1.13.1 and native-host Zig 0.15.2 explicitly prepared, use:

```bash
python tools/build_release.py --offline --out dist \
  --target x86_64-unknown-linux-gnu \
  --target-python-abi target-abi.json --target-python-dir target-python
```

The builder supplies its own isolated Cargo/sysroot/cache directories. The helper
uses `python3.13` as the target specification; it does not execute a Mac interpreter
to infer Linux ABI flags. Missing or altered sysconfigdata, debug/free-threaded ABI,
and external PyO3 configuration overrides are rejected. It keeps manylinux 2.34
audit checks enabled and does not rename tags to claim compatibility. A real Linux
load and clean installation remain mandatory after cross compilation. The 2026-11-10 Linux cross-build and clean installation qualified this path.
The installed target suite has 52 passes and two strict GTD boundary xfails;
this is offline package qualification, not live venue or public-release certification.

Offline builders using a Cargo vendor directory outside CARGO_HOME must pass
`--vendor-dir /path/to/locked/vendor` so Rust source locations map to `/vendor`.
The original locked sources are still compiled; the native wheel check rejects
local workspace paths. This option does not change dependency selection or ABI.
For cross preparation, expose an existing host CPython 3.13 as `python3.13` on the
task PATH; maturin executes it to parse the exported target sysconfigdata. Linux
ABI values still come exclusively from the verified target file.

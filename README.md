# TL-Store

`tl-store` is the Python distribution for the TL-Store adapter collection.
**cTrader is its first built-in adapter**, exposed as `tl_store.ctrader` and through
`NautilusStore(adapter="ctrader", config=...)`. It is not a separately installed
cTrader distribution. Adapter implementations remain separated under `adapters/`;
future adapters and dependencies will be selected explicitly, not all installed
by default.

**0.2.1a4 is published on public PyPI** from private source revision
`6939669c683afb6c3500efc8686609a1e6201f8f`. Eight real native wheels cover
CPython 3.13/3.14 on macOS arm64, Windows AMD64, Linux x86_64 and Linux aarch64
(glibc 2.34+). Mac/Linux x86_64 passed installed offline tests; their CP313 jobs
also rebuilt recipient source offline with an empty Cargo home. Windows and
Linux aarch64 are **build-only, not runtime-tested**. Alpha status and the fixed
official NA release-candidate dependency remain explicit.

The GitHub repository and history remain private. Each public wheel supplies
483 necessary source/license/build files and all 233 locked vendor crates under
`.dist-info/corresponding-source`; private Git access is unnecessary to rebuild.
There is no public standalone Store sdist or private-history attachment.

## Install through TradeLearn

The recommended target command is:

```sh
pip install "trade-learn[nautilus,tl-store]"
```

The TL-owned `tl-store` extra pins the qualified `tl-store` distribution internally.
Users do not specify an adapter package or its version separately. Dependencies
are one-way: TL's optional group selects TL-Store, which depends on official
`nautilus_trader==2.0.0rc7`. TL-Store does not depend on TL or commercial monitoring.

Published TradeLearn **0.3.0** selects immutable Store **0.2.1a3** and has a
qualified CP313 Mac arm64/Linux x86_64 combination. Its exact wheels and SDK
freeze remain valid for current IC consumers. New TradeLearn **0.3.1 is still an
unreleased candidate**: it selects a4 across the eight OS/ABI targets and must
complete its own formal release gates. Installing a4 alone does not replace
that TL dependency lock or certify new TL functionality.

## Install for standalone official NA

```sh
python -m pip install "tl-store==0.2.1a4"
python -m pip check
```

Import with `from tl_store import ctrader`; native lives at
`tl_store.ctrader._native`, with no old `lq_adapter_ctrader` alias. Standalone
Store installs official NA without TL, commercial monitoring, account tokens
or service enrollment. Only optional TL binding requires a qualified matching
unified Store API2/Backtrader interface1 framework.

See the [cTrader capability and configuration notes](adapters/ctrader/README.md).
General TL strategy examples use official NautilusStore adapters. TL owns Cerebro,
NautilusStore, Runtime and Broker; this package adds no account
singleton, second Runtime or Store event forwarding.

## Source and Release builds

The source repository `MuuYesen/tl-store` remains private under the final
2026-11-12 decision. Initial repository history contains only this generic
adapter collection and its license/build/test inputs, without LeafQuant
deployment history or credentials. Repository access, CI artifacts, public
binary publication and actual installation are separate qualifications.

The canonical distribution metadata is the root `pyproject.toml`. From this root,
with Rust 1.97.0 and CPython 3.13:

```sh
python -m pip install "maturin==1.13.1"
python tools/build_release.py --out raw-dist
cargo vendor --locked --versioned-dirs --manifest-path adapters/ctrader/native/Cargo.toml /private/tmp/tl-store-vendor
python tools/finalize_wheel.py raw-dist/*.whl dist/$(basename raw-dist/*.whl) --vendor-dir /private/tmp/tl-store-vendor
maturin sdist --out dist
python tools/check_release.py --wheel dist/*.whl
```

The release wrapper uses optimized compilation and remaps local compiler paths.
On Linux it requires a successful manylinux_2_34 audit; it does not rename an
unaudited wheel. The finalizer normalizes SBOM source references and rebuilds
RECORD without changing native bytes or platform tags. Source distributions contain
the complete Rust/Python implementation, locked dependencies, license notices and
build tooling; they can also build with the declared maturin backend directly.

The CI workflow targets macOS arm64 and Debian 12 Linux x86_64 CPython
3.13. Each final wheel carries the component's necessary corresponding source,
original notices, build inputs and the complete checksum-verified locked Cargo
vendor source under `tl_store-0.2.1a3.dist-info/corresponding-source/`.
Recipients need no access to the private Git repository. Python runtime files
and the attached Rust/build materials are visible to every public wheel recipient;
Git history, CI configuration and application/account data are not included.
No standalone public sdist or public GitHub source attachment is published.

CI verifies the source closure, then extracts the recipient material and performs
an actual offline optimized rebuild with an empty Cargo home/target on both
platforms. The rebuilt native must import from its new installed wheel. The
existing primary offline adapter suite runs against the qualified final wheel.
Source vendoring includes the locked dev/target dependency superset; it is not
a claim every dependency is linked on every supported OS.

`release.yml` defaults to build-only qualification and requires the successful
exact-main CI run ID. It downloads those existing artifacts instead of rebuilding
unchanged native code, rechecks recipient source and Twine, and uploads only the
two qualified wheels when explicitly published. GitHub OIDC uses owner
`MuuYesen`, repository `tl-store`, workflow `release.yml`, environment `pypi`.
The user confirmed the PyPI pending publisher is configured; the actual OIDC
exchange/upload is validated at release. No registry token belongs in source.
The source repository/history remain private; wheel publication is a separate action.

The actual prior Linux TL0.3/Storea3/official-NA combination passed eight offline
installed tests after correcting a noexec qualification mount; those old wheel
hashes do not substitute for the newly source-bearing final bytes. No other
platform or live broker is certified. Store integration tests additionally
require the qualified TL build.

Only generic source, tests, dependencies, licenses and build tooling belong here.
Strategies, deployment configuration, accounts, credentials and internal operating
records are excluded. See [LICENSE](LICENSE) and component notices. Existing
copyright headers are retained. Backtrader was an API reference only; no GPL
Backtrader implementation or singleton was copied.

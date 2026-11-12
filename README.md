> Current source candidate: **0.2.1a4, unpublished/unqualified**. Adds real Mac arm64, Windows AMD64, Linux x64/ARM64 build targets for CPython3.13/3.14. Windows/ARM results will be build-only unless explicitly recorded; do not infer runtime support from tags. Public **0.2.1a3** remains the immutable qualified CP313 Mac/Linuxx64 release. Current IC uses TL0.3.0+Storea3; a4 preparation does not replace its frozen artifacts.

# TL-Store

`tl-store` is the Python distribution for the TL-Store adapter collection.
**cTrader is its first built-in adapter**, exposed as `tl_store.ctrader` and through
`NautilusStore(adapter="ctrader", config=...)`. It is not a separately installed
cTrader distribution. Adapter implementations remain separated under `adapters/`;
future adapters and dependencies will be selected explicitly, not all installed
by default.

The current source is an **unpublished alpha candidate 0.2.1a3** for the
Backtrader interface. Its new native-credential ABI requires matching rebuilt wheels;
0.2.1a1 artifacts remain historical evidence for their own source. Optimized compilation does not turn the alpha or its official NA release
candidate dependency into stable software. GitHub source, Release and PyPI
publication are separate actions and are not asserted here.

## Install through TradeLearn

The recommended target command is:

```sh
pip install "trade-learn[nautilus,tl-store]"
```

The TL-owned `tl-store` extra pins the qualified `tl-store` distribution internally.
Users do not specify an adapter package or its version separately. Dependencies
are one-way: TL's optional group selects TL-Store, which depends on official
`nautilus_trader==2.0.0rc7`. TL-Store does not depend on TL or commercial monitoring.

This is a publication target. Public trade-learn 0.2.9 has neither unified Store adapter API 2 nor
the new extra. It is **not currently a verified public PyPI installation**. Before
publication, install the matching local TL candidate and dependencies from the
prepared wheelhouse with the exact same requirement:

```sh
pip install --no-index --find-links ./wheelhouse "trade-learn[nautilus,tl-store]"
python -m pip check
```

After compatible TL and TL-Store versions have actually been published and
verified, the target command becomes the public installation command. TL's next
release version is owned by its release process; changing source metadata does
not update the old PyPI release. This combination is qualified only for CPython
3.13 on macOS arm64 or Linux x86_64 with glibc 2.34+.

## Install for standalone official NA

Before publication, use the local wheelhouse:

```sh
pip install --no-index --find-links ./wheelhouse "tl-store==0.2.1a3"
```

After this exact alpha version is published, omit
`--no-index --find-links ./wheelhouse`. Import the cTrader API with
`from tl_store import ctrader`. This installs the official NA dependency without
TL, monitoring, platform tokens or commercial service enrollment. Only the optional
TL Store binding needs a separately qualified TL unified Store adapter API 2 / Backtrader interface 1 build; older builds
are rejected explicitly.

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

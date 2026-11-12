# TL-Store

`tl-store` is the Python distribution for the TL-Store adapter collection.
**cTrader is its first built-in adapter**, exposed as `tl_store.ctrader` and through
`NautilusStore(adapter="ctrader", config=...)`. It is not a separately installed
cTrader distribution. Adapter implementations remain separated under `adapters/`;
future adapters and dependencies will be selected explicitly, not all installed
by default.

The current source is an **unpublished alpha candidate 0.2.1a2** for the
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
pip install --no-index --find-links ./wheelhouse "tl-store==0.2.1a2"
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
python tools/finalize_wheel.py raw-dist/*.whl dist/$(basename raw-dist/*.whl)
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
3.13. `release.yml` reuses that same build/install qualification and defaults to
build-only. Its optional `publish` job uses the `pypi` environment and GitHub OIDC
trusted publishing; no registry token belongs in this repository. Configure the
PyPI publisher for owner `MuuYesen`, repository `tl-store`, workflow `release.yml`,
environment `pypi` before attempting publication. Private-source CI artifacts
are not automatically uploaded to PyPI: the publish job selects only the exact
two qualified wheels. Existing LGPL notices require a recipient-accessible
corresponding-source offer for the exact binary; the release input records its
approved URL. A private Git repository alone does not settle this distribution
boundary. Until that exact offer and publisher are available, build-only CI can
proceed but public installation remains unqualified.

The current Linux candidate requires fresh optimized native compilation and
installation evidence; the earlier 0.2.1a1 native bytes cannot validate its new ABI. No other platform or live broker is
certified. Store integration tests additionally require the qualified TL build.

Only generic source, tests, dependencies, licenses and build tooling belong here.
Strategies, deployment configuration, accounts, credentials and internal operating
records are excluded. See [LICENSE](LICENSE) and component notices. Existing
copyright headers are retained. Backtrader was an API reference only; no GPL
Backtrader implementation or singleton was copied.

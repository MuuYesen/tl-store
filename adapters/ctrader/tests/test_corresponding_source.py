"""Recipient source is complete, tamper checked, and independent of private Git."""

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "store_finalize", ROOT / "tools/finalize_wheel.py"
)
pack = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pack)


def vendor(tmp_path):
    directory = tmp_path / "vendor"
    crate = directory / "fixture-1.0.0"
    crate.mkdir(parents=True)
    body = b"pub fn available() {}\n"
    (crate / "lib.rs").write_bytes(body)
    (crate / ".cargo-checksum.json").write_text(
        json.dumps(
            {
                "package": "a" * 64,
                "files": {"lib.rs": hashlib.sha256(body).hexdigest()},
            }
        )
    )
    lock = (
        'version = 4\n[[package]]\nname = "fixture"\nversion = "1.0.0"\n'
        'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
        'checksum = "' + "a" * 64 + '"\n'
    )
    return directory, lock.encode()


def test_vendor_requires_exact_locked_packages_and_bytes(tmp_path):
    directory, lock = vendor(tmp_path)
    archive, packages = pack.vendor_source(directory, lock)
    assert archive.startswith(b"\x1f\x8b") and len(packages) == 1
    (directory / "fixture-1.0.0/lib.rs").write_text("modified")
    with pytest.raises(ValueError, match="checksum"):
        pack.vendor_source(directory, lock)


@pytest.mark.parametrize("fault", ["missing", "unexpected", "symlink"])
def test_vendor_rejects_incomplete_or_unreviewed_files(tmp_path, fault):
    directory, lock = vendor(tmp_path)
    target = directory / "fixture-1.0.0/lib.rs"
    if fault == "missing":
        target.unlink()
    elif fault == "unexpected":
        (directory / "fixture-1.0.0/extra").write_text("unreviewed")
    else:
        target.unlink()
        target.symlink_to(tmp_path / "outside")
    with pytest.raises(ValueError):
        pack.vendor_source(directory, lock)


def test_approved_component_source_scope_excludes_tests_history_and_old_alias():
    files = pack.component_source(ROOT)
    assert len(files) == 484
    assert "adapters/ctrader/native/src/lib.rs" in files
    assert all(".git/" not in n and not n.startswith(".github/") for n in files)
    assert not any("/tests/" in n or "lq_adapter_ctrader" in n for n in files)


def test_recipient_source_manifest_rejects_missing_or_modified_source(tmp_path):
    directory, lock = vendor(tmp_path)
    source = pack.component_source(ROOT)
    source["adapters/ctrader/native/Cargo.lock"] = lock
    members = pack.corresponding_source("tl_store-0.2.1a6.dist-info", source, directory)
    assert pack.verify_corresponding_source(members)["source_files"] == 484
    key = next(n for n in members if n.endswith("/native/src/lib.rs"))
    members[key] += b"\nmodified"
    with pytest.raises(ValueError, match="source digest"):
        pack.verify_corresponding_source(members)


def test_missing_recipient_source_cannot_qualify():
    with pytest.raises(ValueError, match="lacks recipient"):
        pack.verify_corresponding_source({})


@pytest.mark.parametrize(
    "field,value",
    [
        ("head_sha", "old"),
        ("conclusion", "failure"),
        ("head_branch", "unreviewed"),
        ("path", ".github/workflows/other.yml"),
        ("event", "pull_request"),
    ],
)
def test_release_rejects_wrong_or_failed_artifact_provenance(field, value):
    spec = importlib.util.spec_from_file_location(
        "store_check", ROOT / "tools/check_release.py"
    )
    checker = importlib.util.module_from_spec(spec)
    import sys

    sys.path.insert(0, str(ROOT / "tools"))
    try:
        spec.loader.exec_module(checker)
    finally:
        sys.path.pop(0)
    run = {
        "id": 1,
        "status": "completed",
        "conclusion": "success",
        "head_sha": "exact",
        "head_branch": "main",
        "path": ".github/workflows/ci.yml",
        "event": "push",
        "repository": {"full_name": "MuuYesen/tl-store"},
        "head_repository": {"full_name": "MuuYesen/tl-store"},
    }
    assert checker.check_build_run(run, "exact")["qualified"]
    run[field] = value
    with pytest.raises(ValueError):
        checker.check_build_run(run, "exact")


def test_recipient_extracts_approved_source_and_executable_vendor_without_git(tmp_path):
    from zipfile import ZipFile

    directory, lock = vendor(tmp_path)
    (directory / "fixture-1.0.0/lib.rs").chmod(0o755)
    source = pack.component_source(ROOT)
    source["adapters/ctrader/native/Cargo.lock"] = lock
    members = pack.corresponding_source("tl_store-0.2.1a6.dist-info", source, directory)
    wheel = tmp_path / "recipient.whl"
    with ZipFile(wheel, "w") as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    output = tmp_path / "extracted"
    assert pack.extract_source(wheel, output)["source_files"] == 484
    assert not (output / ".git").exists()
    assert (output / "adapters/ctrader/native/src/lib.rs").read_bytes() == source[
        "adapters/ctrader/native/src/lib.rs"
    ]
    assert (output / "vendor/fixture-1.0.0/lib.rs").stat().st_mode & 0o111
    members[pack.KIT + "extra.env"] = b"unreviewed"
    with pytest.raises(ValueError, match="member set"):
        pack.verify_corresponding_source(members)


@pytest.mark.parametrize('windows',[False,True])
def test_archive_paths_are_portable_and_cannot_escape_source_root(windows):
    from pathlib import PureWindowsPath,PurePosixPath
    path=PureWindowsPath if windows else PurePosixPath
    root=path('C:/source' if windows else '/source')
    assert pack.archive_name(root/'nested'/'source.rs',root)=='nested/source.rs'
    with pytest.raises(ValueError):pack.archive_name(root/'..'/'outside.rs',root)

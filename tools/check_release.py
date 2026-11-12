"""Offline collection boundary and wheel integrity checks; never connects an account."""

import argparse
import base64
import csv
import hashlib
import io
import json
from pathlib import Path
from zipfile import ZipFile

from finalize_wheel import KIT, archive_name, verify_corresponding_source

ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / "adapters/ctrader"
FORBIDDEN = (".local-backups/", "/Users/", "交易系统/", "monitor_core")


def check_source():
    count = 0
    for path in COMPONENT.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"symlink in public source: {path.relative_to(ROOT)}")
        if not path.is_file() or any(
            p in ("target", "__pycache__", ".pytest_cache") for p in path.parts
        ):
            continue
        if path.suffix in (".py", ".rs", ".md", ".toml", ".yml"):
            text = path.read_text(encoding="utf-8")
            if any(value in text for value in FORBIDDEN):
                raise ValueError(f"internal reference in {path.relative_to(ROOT)}")
        count += 1
    inventory = json.loads((COMPONENT / "LICENSES/inventory.json").read_text(encoding="utf-8"))
    for crate in inventory["packages"]:
        assert crate["license_files"], f"missing license text: {crate['package']}"
        for item in crate["license_files"]:
            data = (COMPONENT / "LICENSES" / item["file"]).read_bytes()
            assert hashlib.sha256(data).hexdigest() == item["sha256"], item["file"]
    return count, len(inventory["packages"])


def check_wheel(path):
    with ZipFile(path) as archive:
        names = archive.namelist()
        assert len(names) == len(set(names)), "duplicate member"
        assert all(not n.startswith("/") and ".." not in Path(n).parts for n in names)
        record = next(n for n in names if n.endswith(".dist-info/RECORD"))
        rows = list(csv.reader(io.StringIO(archive.read(record).decode())))
        assert {row[0] for row in rows} == set(names), "RECORD member coverage"
        for name, digest, size in rows:
            if name == record:
                assert digest == size == ""
                continue
            data = archive.read(name)
            actual = (
                "sha256="
                + base64.urlsafe_b64encode(hashlib.sha256(data).digest())
                .rstrip(b"=")
                .decode()
            )
            assert digest == actual and int(size) == len(data), name
        assert any(
            n.startswith("tl_store/ctrader/_native") and n.endswith((".so", ".pyd"))
            for n in names
        )
        metadata = archive.read(
            next(n for n in names if n.endswith(".dist-info/METADATA"))
        ).decode()
        assert "Name: tl-store" in metadata
        assert "License-Expression: LGPL-3.0-only" in metadata
        assert "nautilus" in metadata and "2.0.0rc7" in metadata
        verify_corresponding_source(
            {n: archive.read(n) for n in names if n.startswith(KIT)}, ROOT
        )
        assert not any("test_monitor_roundtrip" in n for n in names)
        for name in names:
            # Source tools contain explicit forbidden-pattern guards. They are
            # matched byte-for-byte against the qualified component above.
            if not name.startswith(KIT) and name.endswith(
                (".py", ".md", ".json", ".toml", ".txt", ".yml")
            ):
                assert not any(v in archive.read(name).decode() for v in FORBIDDEN), (
                    name
                )
        for name in names:
            if name.endswith((".so", ".pyd")):
                body = archive.read(name)
                assert all(marker not in body for marker in (b"/Users/", b"\\Users\\", b".local-backups")), name
        assert any(
            n.endswith("/licenses/adapters/ctrader/COPYING.LESSER") for n in names
        )
        assert not any(n.startswith("lq_adapter_ctrader/") for n in names)
        for source in (COMPONENT / "python/tl_store").rglob("*.py"):
            member = archive_name(source, COMPONENT / "python")
            assert archive.read(member) == source.read_bytes(), member
        for source in (COMPONENT / "LICENSES").iterdir():
            matches = [
                n
                for n in names
                if n.endswith("/licenses/adapters/ctrader/LICENSES/" + source.name)
            ]
            assert len(matches) == 1, source.name
            assert archive.read(matches[0]) == source.read_bytes(), source.name
    return len(names)


def check_build_run(run, sha):
    """Release may consume only the successful exact private-main CI artifact."""
    if (run.get("status"), run.get("conclusion"), run.get("head_sha")) != (
        "completed",
        "success",
        sha,
    ):
        raise ValueError("successful exact-SHA CI run required")
    if (
        run.get("path") != ".github/workflows/ci.yml"
        or run.get("head_branch") != "main"
    ):
        raise ValueError("qualified main CI workflow required")
    if run.get("event") not in ("push", "workflow_dispatch"):
        raise ValueError("release cannot consume pull-request artifacts")
    for field in ("repository", "head_repository"):
        if run.get(field, {}).get("full_name") != "MuuYesen/tl-store":
            raise ValueError("qualified repository differs")
    return {"run": run["id"], "sha": sha, "qualified": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--wheel", nargs="*", type=Path, default=[])
    parser.add_argument("--qualified-run", type=Path)
    parser.add_argument("--sha")
    args = parser.parse_args()
    if args.qualified_run:
        print(
            json.dumps(
                check_build_run(json.loads(args.qualified_run.read_text(encoding="utf-8")), args.sha)
            )
        )
    files, crates = check_source()
    wheels = {p.name: check_wheel(p) for p in args.wheel}
    print(
        json.dumps(
            {"source_files": files, "locked_crates": crates, "wheel_members": wheels},
            sort_keys=True,
        )
    )

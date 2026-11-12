"""Normalize local SBOM paths; keep all native bytes and wheel tags unchanged."""

import argparse
import base64
import csv
import gzip
import hashlib
import io
import json
import re
import tarfile
import tomllib
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


def normalize(value):
    if isinstance(value, dict):
        return {key: normalize(child) for key, child in value.items()}
    if isinstance(value, list):
        return [normalize(child) for child in value]
    if isinstance(value, str):
        return re.sub(r"path\+file://[^#]*#", "path+file:///source/native#", value)
    return value


ROOT = Path(__file__).resolve().parents[1]
KIT = "tl_store-0.2.1a5.dist-info/corresponding-source/"
CONFIG = b'[source.crates-io]\nreplace-with = "vendored-sources"\n[source.vendored-sources]\ndirectory = "vendor"\n'
REBUILD = b"""Recipient source for tl-store 0.2.1a5; private Git access is unnecessary.
Use matching CPython3.13 or3.14, Rust1.97.0, maturin1.13.1 and the ordinary platform compiler.
From an unpacked wheel, run this supplied script:
  python tools/finalize_wheel.py --extract-corresponding-source /path/to/wheel --destination /new/source
Then cd /new/source and build without any registry/network access:
  python tools/build_release.py --offline --vendor-dir vendor --out raw-dist
All locked crate sources, original notices and checksums accompany this copy.
Vendor contains the locked dev/target superset, not a claim all crates link on every OS.
This recipient material does not include private Git history, CI or business configuration.
"""


def digest(data):
    return hashlib.sha256(data).hexdigest()


def archive_name(path, root):
    relative = path.relative_to(root)
    if relative.is_absolute() or '..' in relative.parts:
        raise ValueError('source archive path escapes root')
    return relative.as_posix()


def component_source(root):
    root = Path(root)
    names = [
        ".gitattributes",
        "LICENSE",
        "README.md",
        "pyproject.toml",
        "adapters/ctrader/README.md",
        "adapters/ctrader/COPYING",
        "adapters/ctrader/COPYING.LESSER",
        "adapters/ctrader/NOTICE",
        "adapters/ctrader/native/Cargo.toml",
        "adapters/ctrader/native/Cargo.lock",
    ]
    for folder, pattern in [
        ("tools", "*.py"),
        ("adapters/ctrader/native/src", "*.rs"),
        ("adapters/ctrader/python/tl_store", "*.py"),
        ("adapters/ctrader/LICENSES", "*"),
    ]:
        names.extend(
            archive_name(p, root)
            for p in (root / folder).rglob(pattern)
            if p.is_file()
        )
    result = {}
    for name in sorted(set(names)):
        path = root / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("missing or redirected component source: " + name)
        result[name] = path.read_bytes()
    return result


def locked_packages(lock):
    result = {}
    for p in tomllib.loads(lock.decode())["package"]:
        if "source" not in p:
            continue
        if p["source"] != "registry+https://github.com/rust-lang/crates.io-index":
            raise ValueError("private or unreviewed dependency source")
        if not re.fullmatch("[0-9a-f]{64}", p.get("checksum", "")):
            raise ValueError("missing registry package checksum")
        result[p["name"] + "-" + p["version"]] = p["checksum"]
    return result


def vendor_source(directory, lock):
    directory = Path(directory)
    packages = locked_packages(lock)
    if directory.is_symlink() or {p.name for p in directory.iterdir()} != set(packages):
        raise ValueError("vendor package set differs from locked dependencies")
    output = io.BytesIO()
    with (
        gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as compressed,
        tarfile.open(fileobj=compressed, mode="w") as archive,
    ):
        for name, checksum in sorted(packages.items()):
            crate = directory / name
            if crate.is_symlink() or not crate.is_dir():
                raise ValueError("redirected vendor package")
            check = json.loads((crate / ".cargo-checksum.json").read_text(encoding="utf-8"))
            if check["package"] != checksum:
                raise ValueError("vendor package checksum differs")
            files = {
                archive_name(p, crate): p
                for p in crate.rglob("*")
                if p.is_file() or p.is_symlink()
            }
            if set(files) != set(check["files"]) | {".cargo-checksum.json"}:
                raise ValueError("missing or unexpected vendor file")
            for relative, path in sorted(files.items()):
                if path.is_symlink() or not path.is_file():
                    raise ValueError("redirected vendor file")
                body = path.read_bytes()
                if (
                    relative != ".cargo-checksum.json"
                    and digest(body) != check["files"][relative]
                ):
                    raise ValueError("vendor file checksum differs")
                info = tarfile.TarInfo("vendor/" + name + "/" + relative)
                info.size = len(body)
                info.mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
                archive.addfile(info, io.BytesIO(body))
    return output.getvalue(), packages


def corresponding_source(dist_info, source, vendor):
    prefix = dist_info + "/corresponding-source/"
    body, packages = vendor_source(vendor, source["adapters/ctrader/native/Cargo.lock"])
    members = {prefix + name: data for name, data in source.items()}
    members[prefix + "vendor.tar.gz"] = body
    members[prefix + ".cargo/config.toml"] = CONFIG
    members[prefix + "REBUILD.md"] = REBUILD
    manifest = {
        "schema": 1,
        "project": "tl-store",
        "version": "0.2.1a5",
        "files": {n: digest(b) for n, b in source.items()},
        "vendor_sha256": digest(body),
        "locked_packages": packages,
    }
    members[prefix + "SOURCE-MANIFEST.json"] = (
        json.dumps(manifest, indent=2) + "\n"
    ).encode()
    return members


def verify_corresponding_source(members, root=None):
    if KIT + "SOURCE-MANIFEST.json" not in members:
        raise ValueError("wheel lacks recipient corresponding source")
    manifest = json.loads(members[KIT + "SOURCE-MANIFEST.json"])
    if (manifest.get("schema"), manifest.get("project"), manifest.get("version")) != (
        1,
        "tl-store",
        "0.2.1a5",
    ):
        raise ValueError("recipient source identity differs")
    sources = manifest["files"]
    if len(sources) != 483:
        raise ValueError("recipient component source closure differs")
    expected = {KIT + n for n in sources} | {
        KIT + n
        for n in [
            "vendor.tar.gz",
            ".cargo/config.toml",
            "REBUILD.md",
            "SOURCE-MANIFEST.json",
        ]
    }
    if {n for n in members if n.startswith(KIT)} != expected:
        raise ValueError("recipient source member set differs")
    for name, sha in sources.items():
        if Path(name).is_absolute() or ".." in Path(name).parts or "\\" in name:
            raise ValueError("unsafe recipient source name")
        if digest(members[KIT + name]) != sha:
            raise ValueError("recipient source digest differs: " + name)
    if root is not None:
        actual = component_source(root)
        if {n: digest(b) for n, b in actual.items()} != sources:
            raise ValueError("recipient source differs from current qualified source")
    if (
        members[KIT + ".cargo/config.toml"] != CONFIG
        or members[KIT + "REBUILD.md"] != REBUILD
    ):
        raise ValueError("recipient rebuild instructions/config differ")
    packages = locked_packages(members[KIT + "adapters/ctrader/native/Cargo.lock"])
    vendor = members[KIT + "vendor.tar.gz"]
    if (
        digest(vendor) != manifest["vendor_sha256"]
        or manifest["locked_packages"] != packages
    ):
        raise ValueError("recipient vendor digest or lock differs")
    seen = {}
    checks = {}
    with tarfile.open(fileobj=io.BytesIO(vendor), mode="r|gz") as archive:
        for item in archive:
            parts = Path(item.name).parts
            if (
                not item.isfile()
                or len(parts) < 3
                or parts[0] != "vendor"
                or ".." in parts
                or item.name.startswith("/")
                or item.size > 64 * 1024**2
                or item.mode not in (0o644, 0o755)
            ):
                raise ValueError("unsafe vendor source archive")
            package = parts[1]
            relative = "/".join(parts[2:])
            if package not in packages or relative in seen.setdefault(package, {}):
                raise ValueError("unexpected or duplicate vendor source")
            body = archive.extractfile(item).read()
            seen[package][relative] = digest(body)
            if relative == ".cargo-checksum.json":
                checks[package] = json.loads(body)
    if set(seen) != set(packages) or set(checks) != set(packages):
        raise ValueError("recipient vendor is incomplete")
    for name, checksum in packages.items():
        check = checks[name]
        actual = {n: h for n, h in seen[name].items() if n != ".cargo-checksum.json"}
        if check["package"] != checksum or actual != check["files"]:
            raise ValueError("recipient vendor file checksum differs")
    return {
        "source_files": len(sources),
        "vendor_packages": len(packages),
        "vendor_sha256": digest(vendor),
        "source_manifest_sha256": digest(members[KIT + "SOURCE-MANIFEST.json"]),
    }


def extract_source(wheel, destination):
    destination = Path(destination)
    if destination.exists():
        raise ValueError("recipient extraction requires a new destination")
    with ZipFile(wheel) as archive:
        members = {n: archive.read(n) for n in archive.namelist() if n.startswith(KIT)}
    receipt = verify_corresponding_source(members)
    destination.mkdir(parents=True)
    for name, body in members.items():
        target = destination / name[len(KIT) :]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    with tarfile.open(
        fileobj=io.BytesIO(members[KIT + "vendor.tar.gz"]), mode="r|gz"
    ) as archive:
        for item in archive:
            target = destination / item.name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.extractfile(item).read())
            target.chmod(item.mode)
    print(json.dumps(receipt))
    return receipt


def finalize(source, destination, vendor_dir):
    with ZipFile(source) as archive:
        names = archive.namelist()
        assert len(names) == len(set(names))
        assert not any(n.endswith(("RECORD.jws", "RECORD.p7s")) for n in names)
        record = next(n for n in names if n.endswith(".dist-info/RECORD"))
        members = {
            n: archive.read(n) for n in names if n != record and not n.endswith("/")
        }
    native_before = {
        n: hashlib.sha256(b).hexdigest()
        for n, b in members.items()
        if n.endswith((".so", ".pyd"))
    }
    for name, body in members.items():
        if name.endswith(".cyclonedx.json"):
            members[name] = (
                json.dumps(normalize(json.loads(body)), indent=2) + "\n"
            ).encode()
    members.update(
        corresponding_source(
            record.rsplit("/", 1)[0], component_source(ROOT), vendor_dir
        )
    )
    receipt = verify_corresponding_source(members, ROOT)
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    for name, body in sorted(members.items()):
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(body).digest())
            .rstrip(b"=")
            .decode()
        )
        writer.writerow([name, "sha256=" + digest, str(len(body))])
    writer.writerow([record, "", ""])
    members[record] = stream.getvalue().encode()
    temporary = destination.with_name(destination.name + ".tmp")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(temporary, "w", ZIP_DEFLATED) as archive:
        for name, body in sorted(members.items()):
            archive.writestr(name, body)
    temporary.replace(destination)
    with ZipFile(destination) as archive:
        assert {
            n: hashlib.sha256(archive.read(n)).hexdigest() for n in native_before
        } == native_before
    print(
        json.dumps(
            {
                "file": destination.name,
                "native_unchanged": native_before,
                "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
                "recipient_source": receipt,
            }
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path, nargs="?")
    parser.add_argument("destination", type=Path, nargs="?")
    parser.add_argument("--vendor-dir", type=Path)
    parser.add_argument("--extract-corresponding-source", type=Path)
    parser.add_argument("--destination", dest="extract_destination", type=Path)
    args = parser.parse_args()
    if args.extract_corresponding_source:
        if not args.extract_destination:
            parser.error("--destination required")
        extract_source(args.extract_corresponding_source, args.extract_destination)
    else:
        if not args.source or not args.destination or not args.vendor_dir:
            parser.error("source, destination and --vendor-dir are required")
        finalize(args.source, args.destination, args.vendor_dir)

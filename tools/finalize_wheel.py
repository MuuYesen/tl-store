"""Normalize local SBOM paths; keep all native bytes and wheel tags unchanged."""
import argparse
import base64
import csv
import hashlib
import io
import json
from pathlib import Path
import re
from zipfile import ZipFile, ZIP_DEFLATED


def normalize(value):
    if isinstance(value, dict):
        return {key: normalize(child) for key, child in value.items()}
    if isinstance(value, list):
        return [normalize(child) for child in value]
    if isinstance(value, str):
        return re.sub(r'path\+file://[^#]*#', 'path+file:///source/native#', value)
    return value


def finalize(source, destination):
    with ZipFile(source) as archive:
        names = archive.namelist()
        assert len(names) == len(set(names))
        assert not any(n.endswith(('RECORD.jws', 'RECORD.p7s')) for n in names)
        record = next(n for n in names if n.endswith('.dist-info/RECORD'))
        members = {n: archive.read(n) for n in names if n != record}
    native_before = {n: hashlib.sha256(b).hexdigest() for n, b in members.items() if n.endswith('.so')}
    for name in members:
        if name.endswith('.cyclonedx.json'):
            members[name] = (json.dumps(normalize(json.loads(members[name])), indent=2) + '\n').encode()
    stream = io.StringIO(newline='')
    writer = csv.writer(stream, lineterminator='\n')
    for name, body in sorted(members.items()):
        digest = base64.urlsafe_b64encode(hashlib.sha256(body).digest()).rstrip(b'=').decode()
        writer.writerow([name, 'sha256=' + digest, str(len(body))])
    writer.writerow([record, '', ''])
    members[record] = stream.getvalue().encode()
    temporary = destination.with_name(destination.name + '.tmp')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(temporary, 'w', ZIP_DEFLATED) as archive:
        for name, body in sorted(members.items()):
            archive.writestr(name, body)
    temporary.replace(destination)
    with ZipFile(destination) as archive:
        assert {n: hashlib.sha256(archive.read(n)).hexdigest() for n in native_before} == native_before
    print(json.dumps({'file': destination.name, 'native_unchanged': native_before,
                      'sha256': hashlib.sha256(destination.read_bytes()).hexdigest()}))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    finalize(args.source, args.destination)

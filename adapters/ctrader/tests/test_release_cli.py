"""Hermetic build-entry regression; no real Rust build, account or network."""
import importlib.util
import json
import os
from pathlib import Path
import sys

import pytest


@pytest.fixture
def build_entry(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[3]
    spec = importlib.util.spec_from_file_location('release_entry_under_test', root/'tools/build_release.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    binary = tmp_path/'target-scripts'
    binary.mkdir()
    modules = tmp_path/'target-modules'
    modules.mkdir()
    (modules/'maturin.py').write_text('raise SystemExit("Unable to find `maturin` script")\n')
    distribution = modules/'maturin-1.13.1.dist-info'
    distribution.mkdir()
    (distribution/'METADATA').write_text('Metadata-Version: 2.1\nName: maturin\nVersion: 1.13.1\n')
    monkeypatch.syspath_prepend(str(modules))
    monkeypatch.setenv('PYTHONPATH', str(modules))
    monkeypatch.setenv('PATH', str(binary))
    monkeypatch.delenv('RUSTFLAGS', raising=False)
    monkeypatch.delenv('CARGO_ENCODED_RUSTFLAGS', raising=False)
    rust = binary/'rustc'
    rust.write_text(f'#!{sys.executable}\nprint("rustc 1.97.0 (test fixture)")\n')
    rust.chmod(0o755)
    monkeypatch.setattr(module.platform, 'system', lambda:'Linux')
    monkeypatch.setattr(module.platform, 'machine', lambda:'x86_64')
    monkeypatch.setattr(sys, 'argv', ['build_release.py','--component',str(tmp_path),
        '--out',str(tmp_path/'out'),'--offline'])
    return module, binary, tmp_path/'calls.jsonl'


def install_fixture_cli(binary, calls, version='1.13.1'):
    cli = binary/'maturin'
    cli.write_text(f'''#!{sys.executable}
import json, os, sys
with open({str(calls)!r}, 'a') as stream:
    stream.write(json.dumps(dict(argv=sys.argv[1:], remap=os.environ.get('CARGO_ENCODED_RUSTFLAGS'))) + '\\n')
if sys.argv[1:] == ['--version']:
    print('maturin {version}')
''')
    cli.chmod(0o755)


def test_target_install_cli_outside_python_scripts_runs_pinned_build(build_entry):
    module, binary, calls = build_entry
    install_fixture_cli(binary, calls)
    module.main()
    invocations = [json.loads(line) for line in calls.read_text().splitlines()]
    build = next(item for item in invocations if item['argv'][0]=='build')
    assert all(flag in build['argv'] for flag in ['--release','--locked','--offline'])
    assert build['argv'][-4:] == ['--compatibility','manylinux_2_34','--auditwheel','check']
    assert '--remap-path-prefix=' in build['remap']


def test_wrong_cli_version_fails_before_build(build_entry):
    module, binary, calls = build_entry
    install_fixture_cli(binary, calls, version='1.12.6')
    with pytest.raises(SystemExit, match='maturin 1.13.1'):
        module.main()
    assert all(json.loads(line)['argv']==['--version'] for line in calls.read_text().splitlines())


def test_missing_cli_rejects_before_build(build_entry):
    module, _, calls = build_entry
    with pytest.raises(SystemExit, match='maturin.*PATH'):
        module.main()
    assert not calls.exists()


@pytest.fixture
def cross_entry(build_entry, monkeypatch, tmp_path):
    import base64
    import hashlib
    module, binary, calls = build_entry
    install_fixture_cli(binary, calls)
    monkeypatch.setattr(module.platform, 'system', lambda:'Darwin')
    monkeypatch.setattr(module.platform, 'machine', lambda:'arm64')
    target = tmp_path/'target-python'
    target.mkdir()
    raw = b'build_time_vars = {"SOABI": "cpython-313-x86_64-linux-gnu"}\n'
    name = '_sysconfigdata__linux_x86_64-linux-gnu.py'
    (target/name).write_bytes(raw)
    evidence = tmp_path/'abi.json'
    data = dict(implementation='cpython',python_version='3.13.17',machine='x86_64',
        config=dict(SOABI='cpython-313-x86_64-linux-gnu',SIZEOF_VOID_P=8,Py_DEBUG=0,Py_GIL_DISABLED=0),
        sysconfigdata=dict(original_path='/usr/local/lib/python3.13/'+name,size=len(raw),
            sha256=hashlib.sha256(raw).hexdigest(),base64=base64.b64encode(raw).decode()))
    evidence.write_text(json.dumps(data))
    sys.argv += ['--target','x86_64-unknown-linux-gnu','--target-python-abi',str(evidence),
                 '--target-python-dir',str(target)]
    return module,binary,calls,evidence,target


def test_cross_uses_real_target_config_without_host_interpreter(cross_entry,monkeypatch):
    module,_,calls,_,target=cross_entry
    seen={}
    original_run=module.subprocess.run
    def capture(command,**kwargs):
        if 'build' in command:
            seen.update(command=command,**kwargs)
            return None
        return original_run(command,**kwargs)
    monkeypatch.setattr(module.subprocess,'run',capture)
    module.main()
    assert '--zig' in seen['command']
    assert seen['command'][seen['command'].index('--target')+1]=='x86_64-unknown-linux-gnu'
    assert seen['command'][seen['command'].index('--interpreter')+1]=='python3.13'
    assert seen['command'][-4:]==['--compatibility','manylinux_2_34','--auditwheel','check']
    assert seen['env']['PYO3_CROSS_LIB_DIR']==str(target)
    assert seen['env']['PYO3_CROSS']=='1'


def test_cross_requires_actual_target_evidence(build_entry):
    module,_,calls=build_entry
    sys.argv+=['--target','x86_64-unknown-linux-gnu']
    with pytest.raises(SystemExit,match='target.*ABI|cross.*Darwin'):
        module.main()
    assert not calls.exists()


@pytest.mark.parametrize('field,value',[('SOABI','cpython-313t-x86_64-linux-gnu'),
    ('Py_GIL_DISABLED',1),('Py_DEBUG',1),('SIZEOF_VOID_P',4)])
def test_cross_rejects_abi_drift(cross_entry,field,value):
    module,_,calls,evidence,_=cross_entry
    data=json.loads(evidence.read_text());data['config'][field]=value
    evidence.write_text(json.dumps(data))
    with pytest.raises(SystemExit,match='target.*ABI'):
        module.main()
    assert not calls.exists()


def test_cross_rejects_tampered_sysconfigdata(cross_entry):
    module,_,calls,_,target=cross_entry
    next(target.iterdir()).write_text('changed')
    with pytest.raises(SystemExit,match='sysconfigdata'):
        module.main()
    assert not calls.exists()


def test_cross_rejects_conflicting_pyo3_override(cross_entry,monkeypatch):
    module,_,calls,_,_=cross_entry
    monkeypatch.setenv('PYO3_CONFIG_FILE','/unreviewed/config')
    with pytest.raises(SystemExit,match='PYO3'):
        module.main()
    assert not calls.exists()


def test_cross_arguments_without_target_are_rejected(build_entry,tmp_path):
    module,_,calls=build_entry
    sys.argv+=['--target-python-dir',str(tmp_path)]
    with pytest.raises(SystemExit,match='target'):
        module.main()
    assert not calls.exists()


def test_offline_external_vendor_path_is_fully_remapped(build_entry, tmp_path):
    module, binary, calls = build_entry
    vendor = tmp_path / '.local-backups' / 'vendor'
    vendor.mkdir(parents=True)
    install_fixture_cli(binary, calls)
    sys.argv += ['--vendor-dir', str(vendor)]
    module.main()
    build = next(json.loads(line) for line in calls.read_text().splitlines() if json.loads(line)['argv'][0] == 'build')
    assert f'--remap-path-prefix={vendor.resolve()}=/vendor' in build['remap'].split('\x1f')

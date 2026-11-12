"""Build qualified Release wheels with local compiler paths remapped."""
import argparse
import base64
import hashlib
import json
from importlib.metadata import version
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]

def validate_native_target(system, machine, python_version):
    """Qualified build-only targets; runtime acceptance is recorded separately."""
    allowed = {
        ('Darwin', 'arm64'), ('Windows', 'amd64'), ('Windows', 'x86_64'),
        ('Linux', 'x86_64'), ('Linux', 'aarch64'), ('Linux', 'arm64'),
    }
    if (system, machine.lower()) not in allowed:
        raise SystemExit('Unqualified native OS/architecture target')
    if tuple(python_version) not in ((3, 13), (3, 14)):
        raise SystemExit('CPython 3.13 or 3.14 target required')


def cross_environment(args):
    if not args.target:
        if args.target_python_abi or args.target_python_dir:
            raise SystemExit('target Python evidence requires an explicit target')
        return {}
    if (platform.system(), platform.machine()) != ('Darwin', 'arm64'):
        raise SystemExit('cross target requires a Darwin arm64 builder')
    if not args.target_python_abi or not args.target_python_dir:
        raise SystemExit('cross target requires actual target Python ABI evidence and directory')
    if args.interpreter not in (None, 'python3.13'):
        raise SystemExit('cross target interpreter must be python3.13, never the host executable')
    if any(os.environ.get(key) for key in ('PYO3_CONFIG_FILE', 'PYO3_NO_PYTHON',
            'PYO3_CROSS', 'PYO3_CROSS_LIB_DIR', 'PYO3_CROSS_PYTHON_VERSION',
            'PYO3_CROSS_PYTHON_IMPLEMENTATION', '_PYTHON_SYSCONFIGDATA_NAME')):
        raise SystemExit('Clear external PYO3 cross/config overrides')
    try:
        evidence = json.loads(args.target_python_abi.read_text(encoding="utf-8"))
        config = evidence['config']
        if (evidence['implementation'] != 'cpython' or
                not evidence['python_version'].startswith('3.13.') or
                evidence['machine'] != 'x86_64' or
                config['SOABI'] != 'cpython-313-x86_64-linux-gnu' or
                config['SIZEOF_VOID_P'] != 8 or config['Py_DEBUG'] or config['Py_GIL_DISABLED']):
            raise SystemExit('Unsupported target Python ABI')
        metadata = evidence['sysconfigdata']
        name = Path(metadata['original_path']).name
        if not name.startswith('_sysconfigdata_') or not name.endswith('.py'):
            raise SystemExit('Invalid target sysconfigdata name')
        directory = args.target_python_dir.resolve()
        paths = list(directory.glob('_sysconfigdata*.py'))
        if len(paths) != 1 or paths[0].name != name or paths[0].is_symlink():
            raise SystemExit('Ambiguous or redirected target sysconfigdata')
        data = paths[0].read_bytes()
        if (len(data) != metadata['size'] or
                hashlib.sha256(data).hexdigest() != metadata['sha256'] or
                data != base64.b64decode(metadata['base64'], validate=True)):
            raise SystemExit('Target sysconfigdata digest mismatch')
    except (KeyError, ValueError, OSError, TypeError) as exc:
        raise SystemExit('Invalid target Python ABI/sysconfigdata evidence') from exc
    return {'PYO3_CROSS': '1', 'PYO3_CROSS_LIB_DIR': str(directory),
            'PYO3_CROSS_PYTHON_VERSION': '3.13',
            'PYO3_CROSS_PYTHON_IMPLEMENTATION': 'CPython'}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--component', type=Path, default=ROOT)
    parser.add_argument('--interpreter')
    parser.add_argument('--target', choices=['x86_64-unknown-linux-gnu'])
    parser.add_argument('--target-python-abi', type=Path)
    parser.add_argument('--target-python-dir', type=Path)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--vendor-dir', type=Path, help='External locked Cargo vendor source root for path remapping')
    args = parser.parse_args()
    cross_env = cross_environment(args)
    interpreter = ('python3.13' if args.target else (args.interpreter or sys.executable))
    if version('maturin') != '1.13.1':
        raise SystemExit('maturin 1.13.1 required')
    maturin = shutil.which('maturin')
    if maturin is None:
        raise SystemExit('maturin 1.13.1 CLI on PATH required')
    if subprocess.check_output([maturin, '--version'], text=True).strip() != 'maturin 1.13.1':
        raise SystemExit('maturin 1.13.1 CLI required')
    compiler = subprocess.check_output(['rustc', '--version'], text=True).strip()
    if not compiler.startswith('rustc 1.97.0 '):
        raise SystemExit('Rust 1.97.0 required')
    if not args.target:
        python_version = json.loads(subprocess.check_output(
            [interpreter, '-c', 'import json,sys; print(json.dumps(list(sys.version_info[:2])))'], text=True))
        validate_native_target(platform.system(), platform.machine(), python_version)
    if os.environ.get('RUSTFLAGS') or os.environ.get('CARGO_ENCODED_RUSTFLAGS'):
        raise SystemExit('Clear external Rust flags before a qualified release build')
    component = args.component.resolve()
    home = Path.home()
    mappings = [(home, '/build'),
                (Path(os.environ.get('CARGO_HOME', home / '.cargo')).resolve(), '/cargo'),
                (Path(os.environ.get('RUSTUP_HOME', home / '.rustup')).resolve(), '/rust'),
                (component, '/source/ctrader')]
    if args.vendor_dir:
        vendor = args.vendor_dir.resolve()
        if not vendor.is_dir():
            raise SystemExit('vendor source directory does not exist')
        mappings.append((vendor, '/vendor'))
    env = dict(os.environ)
    env.update(cross_env)
    env['CARGO_ENCODED_RUSTFLAGS'] = '\x1f'.join(f'--remap-path-prefix={a}={b}' for a, b in mappings)
    command = [maturin, 'build', '--release', '--locked',
               '--interpreter', interpreter, '--out', str(args.out.resolve())]
    if args.offline:
        command.append('--offline')
    if args.target:
        command.extend(['--target', args.target, '--zig'])
    if platform.system() == 'Linux' or args.target:
        command.extend(['--compatibility', 'manylinux_2_34', '--auditwheel', 'check'])
    subprocess.run(command, env=env, check=True, cwd=component)

if __name__ == '__main__':
    main()

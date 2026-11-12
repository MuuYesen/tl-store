"""Read-only target interpreter evidence; run only in the reviewed pinned Linux image."""
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import sys
import sysconfig


def collect():
    if sys.implementation.name != 'cpython' or sys.version_info[:2] != (3, 13):
        raise RuntimeError('Expected target CPython 3.13')
    if platform.system() != 'Linux' or platform.machine() != 'x86_64':
        raise RuntimeError('Expected target Linux x86_64')
    name = sysconfig._get_sysconfigdata_name()
    spec = importlib.util.find_spec(name)
    if spec is None or spec.origin is None:
        raise RuntimeError('Missing authoritative target sysconfigdata')
    path = Path(spec.origin).resolve()
    prefix = Path(sys.base_prefix).resolve()
    if not path.is_relative_to(prefix) or not path.name.startswith('_sysconfigdata_') or path.suffix != '.py':
        raise RuntimeError('Unexpected target sysconfigdata source')
    data = path.read_bytes()
    if len(data) > 524288:
        raise RuntimeError('sysconfigdata exceeds reviewed limit')
    keys = ('SOABI', 'EXT_SUFFIX', 'VERSION', 'LDVERSION', 'Py_ENABLE_SHARED',
            'Py_GIL_DISABLED', 'Py_DEBUG', 'Py_REF_DEBUG', 'Py_TRACE_REFS',
            'COUNT_ALLOCS', 'SIZEOF_VOID_P', 'LIBDIR', 'LDLIBRARY',
            'PYTHONFRAMEWORK', 'PYTHONFRAMEWORKPREFIX', 'MULTIARCH')
    values = {k: sysconfig.get_config_var(k) for k in keys}
    if values['SOABI'] != 'cpython-313-x86_64-linux-gnu' or values['SIZEOF_VOID_P'] != 8:
        raise RuntimeError('Target ABI drift')
    if values['Py_GIL_DISABLED'] or values['Py_DEBUG']:
        raise RuntimeError('Unsupported free-threaded/debug target')
    return dict(schema=1, implementation=sys.implementation.name,
                python_version=platform.python_version(), platform=platform.platform(),
                machine=platform.machine(), libc=platform.libc_ver(), config=values,
                sysconfigdata=dict(name=name, original_path=str(path), size=len(data),
                                   sha256=hashlib.sha256(data).hexdigest(),
                                   base64=base64.b64encode(data).decode('ascii')),
                environment_exported=False, official_packages_imported=False)


if __name__ == '__main__':
    print(json.dumps(collect(), sort_keys=True))

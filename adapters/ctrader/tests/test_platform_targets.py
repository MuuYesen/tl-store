"""Build target policy; it does not pretend another platform has run tests."""
import importlib.util
from pathlib import Path
import pytest

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('store_platform_build',ROOT/'tools/build_release.py')
build=importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


@pytest.mark.parametrize('system,machine',[
    ('Darwin','arm64'),('Windows','AMD64'),('Windows','x86_64'),
    ('Linux','x86_64'),('Linux','aarch64'),('Linux','arm64'),
])
@pytest.mark.parametrize('version',[(3,13),(3,14)])
def test_actual_native_target_matrix(system,machine,version):
    assert build.validate_native_target(system,machine,version) is None


@pytest.mark.parametrize('system,machine,version',[
    ('Darwin','x86_64',(3,13)),('Windows','ARM64',(3,13)),
    ('Linux','ppc64le',(3,13)),('Linux','x86_64',(3,12)),
    ('Linux','aarch64',(3,15)),('FreeBSD','x86_64',(3,13)),
])
def test_unqualified_platform_or_abi_is_rejected(system,machine,version):
    with pytest.raises(SystemExit):build.validate_native_target(system,machine,version)


@pytest.mark.parametrize('minor,accepted',[(12,False),(13,True),(14,True),(15,False)])
def test_runtime_python_guard_matches_official_na_abi_range(minor,accepted,monkeypatch):
    spec=importlib.util.spec_from_file_location('store_compat_policy',ROOT/'adapters/ctrader/python/tl_store/ctrader/compat.py')
    compat=importlib.util.module_from_spec(spec);spec.loader.exec_module(compat)
    with monkeypatch.context() as patch:
        patch.setattr(compat.sys,'version_info',(3,minor))
        patch.setattr(compat,'version',lambda name:'2.0.0rc7')
        if accepted:compat.require_official_nautilus()
        else:
            with pytest.raises(RuntimeError,match='CPython'):compat.require_official_nautilus()

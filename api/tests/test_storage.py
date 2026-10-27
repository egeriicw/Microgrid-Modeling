"""FR-A14 — Storage protocol. One parametrized class over LocalStorage and
S3Storage (moto). RED placeholder (docs/test-plan.md §4 Phase 2).
"""
from __future__ import annotations

import hashlib

import pytest

pytest.importorskip("microgrid_api", reason="Phase 2: storage not implemented")

pytestmark = pytest.mark.xfail(reason="FR-A14 not implemented", strict=False)


@pytest.fixture(params=["local", "s3"])
def storage(request, tmp_path):
    if request.param == "local":
        from microgrid_api.storage import LocalStorage

        return LocalStorage(root=tmp_path)
    from moto import mock_aws

    with mock_aws():
        import boto3

        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-artifacts")
        from microgrid_api.storage import S3Storage

        yield S3Storage(endpoint=None, bucket="test-artifacts", access_key="x", secret_key="y")


def test_put_get_roundtrip(storage):
    data = b"hello,world\n1,2\n"
    ref = storage.put("runs/abc/csv/x.csv", data, content_type="text/csv")
    assert ref.bytes == len(data)
    assert ref.sha256 == hashlib.sha256(data).hexdigest()
    assert storage.get("runs/abc/csv/x.csv") == data


def test_presign_get_returns_fetchable_url(storage):
    storage.put("runs/abc/o.bin", b"\x00\x01\x02", content_type="application/octet-stream")
    url = storage.presign_get("runs/abc/o.bin", ttl=60)
    assert isinstance(url, str) and url


def test_list_prefix(storage):
    storage.put("runs/abc/a.txt", b"a", content_type="text/plain")
    storage.put("runs/abc/b.txt", b"b", content_type="text/plain")
    storage.put("runs/xyz/c.txt", b"c", content_type="text/plain")
    keys = {o.key for o in storage.list("runs/abc/")}
    assert keys == {"runs/abc/a.txt", "runs/abc/b.txt"}


def test_delete(storage):
    storage.put("runs/abc/d.txt", b"d", content_type="text/plain")
    storage.delete("runs/abc/d.txt")
    with pytest.raises(KeyError):
        storage.get("runs/abc/d.txt")

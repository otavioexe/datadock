from datadock.api import _normalize_path


def test_dbfs_single_slash_normalized():
    assert _normalize_path("dbfs:/mnt/dados") == "/dbfs/mnt/dados"


def test_dbfs_double_slash_normalized():
    assert _normalize_path("dbfs://mnt/dados") == "/dbfs/mnt/dados"


def test_local_path_unchanged():
    assert _normalize_path("/local/path/data") == "/local/path/data"


def test_already_dbfs_mount_unchanged():
    assert _normalize_path("/dbfs/mnt/dados") == "/dbfs/mnt/dados"


def test_relative_path_unchanged():
    assert _normalize_path("data/files") == "data/files"


def test_dbfs_nested_path():
    assert _normalize_path("dbfs:/mnt/bronze/sales/2024") == "/dbfs/mnt/bronze/sales/2024"


def test_empty_string_unchanged():
    assert _normalize_path("") == ""

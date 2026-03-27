"""
Integration tests for Databricks DBFS path support.

Since /dbfs/ is only available inside a Databricks cluster, these tests use
monkeypatch to redirect dbfs:/ paths to a local temp directory — simulating
the normalization that would happen in a real Databricks environment.
"""
import os
import pytest
import datadock.api as api_module
from datadock.api import read_data, get_schema_info, scan_schema


FAKE_DBFS_PATH = "dbfs:/mnt/test/sales"
FAKE_DBFS_PATH_DOUBLE_SLASH = "dbfs://mnt/test/sales"


def _write_csv(path: str, header: str, row: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"{header}\n{row}\n")


@pytest.fixture
def dbfs_patch(temp_dir, monkeypatch):
    """Redirects any dbfs:/ path to temp_dir, simulating the FUSE mount."""
    monkeypatch.setattr(api_module, "_normalize_path", lambda p: temp_dir if p.startswith("dbfs:") else p)
    return temp_dir


# ---------------------------------------------------------------------------
# read_data
# ---------------------------------------------------------------------------

def test_read_data_dbfs_single_slash(spark, dbfs_patch):
    _write_csv(os.path.join(dbfs_patch, "a.csv"), "id,name", "1,Alice")

    df = read_data(FAKE_DBFS_PATH, spark=spark)
    assert df is not None
    assert df.count() == 1
    assert set(df.columns) == {"id", "name"}


def test_read_data_dbfs_double_slash(spark, dbfs_patch):
    _write_csv(os.path.join(dbfs_patch, "a.csv"), "id,name", "1,Alice")

    df = read_data(FAKE_DBFS_PATH_DOUBLE_SLASH, spark=spark)
    assert df is not None
    assert df.count() == 1


def test_read_data_dbfs_merges_multiple_files(spark, dbfs_patch):
    _write_csv(os.path.join(dbfs_patch, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(dbfs_patch, "b.csv"), "id,name", "2,Bob")

    df = read_data(FAKE_DBFS_PATH, spark=spark)
    assert df is not None
    assert df.count() == 2


def test_read_data_dbfs_missing_columns_filled_with_null(spark, dbfs_patch):
    _write_csv(os.path.join(dbfs_patch, "a.csv"), "id,name,age,city", "1,Alice,30,SP")
    _write_csv(os.path.join(dbfs_patch, "b.csv"), "id,name,age,city,country", "2,Bob,25,SP,BR")

    df = read_data(FAKE_DBFS_PATH, spark=spark)
    assert df is not None
    assert df.count() == 2
    assert "country" in df.columns
    assert df.filter(df["country"].isNull()).count() == 1


def test_read_data_dbfs_recursive(spark, dbfs_patch):
    subdir = os.path.join(dbfs_patch, "subdir")
    os.makedirs(subdir)
    _write_csv(os.path.join(dbfs_patch, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(subdir, "b.csv"), "id,name", "2,Bob")

    df = read_data(FAKE_DBFS_PATH, spark=spark, recursive=True)
    assert df is not None
    assert df.count() == 2


def test_read_data_dbfs_empty_directory(spark, dbfs_patch):
    result = read_data(FAKE_DBFS_PATH, spark=spark)
    assert result is None


# ---------------------------------------------------------------------------
# get_schema_info
# ---------------------------------------------------------------------------

def test_get_schema_info_dbfs_path(dbfs_patch):
    _write_csv(os.path.join(dbfs_patch, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(dbfs_patch, "b.csv"), "id,name", "2,Bob")

    info = get_schema_info(FAKE_DBFS_PATH)
    assert len(info) == 1
    assert info[0]["file_count"] == 2
    assert info[0]["column_count"] == 2


def test_get_schema_info_dbfs_multiple_groups(dbfs_patch):
    _write_csv(os.path.join(dbfs_patch, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(dbfs_patch, "b.csv"), "city,country,zip", "SP,BR,01310")

    info = get_schema_info(FAKE_DBFS_PATH)
    assert len(info) == 2


def test_get_schema_info_dbfs_empty_directory(dbfs_patch):
    result = get_schema_info(FAKE_DBFS_PATH)
    assert result == []


# ---------------------------------------------------------------------------
# scan_schema
# ---------------------------------------------------------------------------

def test_scan_schema_dbfs_returns_info(dbfs_patch):
    _write_csv(os.path.join(dbfs_patch, "a.csv"), "id,name", "1,Alice")

    result = scan_schema(FAKE_DBFS_PATH)
    assert len(result) == 1
    assert result[0]["file_count"] == 1


def test_scan_schema_dbfs_matches_get_schema_info(dbfs_patch):
    _write_csv(os.path.join(dbfs_patch, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(dbfs_patch, "b.csv"), "city,country", "SP,BR")

    scan_result = scan_schema(FAKE_DBFS_PATH)
    info_result = get_schema_info(FAKE_DBFS_PATH)
    assert scan_result == info_result


def test_scan_schema_dbfs_empty_directory(dbfs_patch):
    result = scan_schema(FAKE_DBFS_PATH)
    assert result == []

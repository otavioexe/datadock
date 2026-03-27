import os
import pytest
from datadock.api import read_data, get_schema_info, scan_schema


def _write_csv(path: str, header: str, row: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"{header}\n{row}\n")


# ---------------------------------------------------------------------------
# read_data
# ---------------------------------------------------------------------------

def test_read_data_single_file(spark, temp_dir):
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")

    df = read_data(temp_dir, spark=spark)
    assert df is not None
    assert df.count() == 1
    assert set(df.columns) == {"id", "name"}


def test_read_data_merges_multiple_files_same_schema(spark, temp_dir):
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(temp_dir, "b.csv"), "id,name", "2,Bob")

    df = read_data(temp_dir, spark=spark)
    assert df is not None
    assert df.count() == 2


def test_read_data_missing_columns_filled_with_null(spark, temp_dir):
    # Core use case: files with different columns in same schema group
    # intersection={id,name,age,city}=4, union={id,name,age,city,country}=5 → Jaccard=0.8
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name,age,city", "1,Alice,30,SP")
    _write_csv(os.path.join(temp_dir, "b.csv"), "id,name,age,city,country", "2,Bob,25,SP,BR")

    df = read_data(temp_dir, spark=spark)
    assert df is not None
    assert df.count() == 2
    assert "country" in df.columns
    # file a.csv has no "country" column — must be null for that row
    null_count = df.filter(df["country"].isNull()).count()
    assert null_count == 1


def test_read_data_uses_injected_spark_session(spark, temp_dir):
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")

    df = read_data(temp_dir, spark=spark)
    assert df is not None
    # verify the session used is the one we injected
    from pyspark.sql import SparkSession
    assert SparkSession.getActiveSession() is spark


def test_read_data_invalid_schema_id_returns_none(spark, temp_dir):
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")

    result = read_data(temp_dir, schema_id=99, spark=spark)
    assert result is None


def test_read_data_empty_directory_returns_none(spark, temp_dir):
    result = read_data(temp_dir, spark=spark)
    assert result is None


def test_read_data_with_min_similarity_separates_into_groups(spark, temp_dir):
    # Jaccard=0.8 between these two files; raising threshold to 0.9 splits them
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name,age,city", "1,Alice,30,SP")
    _write_csv(os.path.join(temp_dir, "b.csv"), "id,name,age,city,country", "2,Bob,25,SP,BR")

    df_group1 = read_data(temp_dir, schema_id=1, spark=spark, min_similarity=0.9)
    df_group2 = read_data(temp_dir, schema_id=2, spark=spark, min_similarity=0.9)

    assert df_group1 is not None
    assert df_group2 is not None
    assert df_group1.count() == 1
    assert df_group2.count() == 1


def test_read_data_directory_with_unsupported_files_only(spark, temp_dir):
    # .txt files are loadable but not included in schema scanning
    path = os.path.join(temp_dir, "notes.txt")
    with open(path, "w") as f:
        f.write("hello\n")

    result = read_data(temp_dir, spark=spark)
    assert result is None


# ---------------------------------------------------------------------------
# get_schema_info
# ---------------------------------------------------------------------------

def test_get_schema_info_returns_expected_keys(temp_dir):
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")

    info = get_schema_info(temp_dir)
    assert len(info) == 1
    entry = info[0]
    assert set(entry.keys()) == {"schema_id", "file_count", "column_count", "files"}


def test_get_schema_info_correct_values(temp_dir):
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(temp_dir, "b.csv"), "id,name", "2,Bob")

    info = get_schema_info(temp_dir)
    assert len(info) == 1
    assert info[0]["file_count"] == 2
    assert info[0]["column_count"] == 2
    assert set(info[0]["files"]) == {"a.csv", "b.csv"}


def test_get_schema_info_empty_directory_returns_empty_list(temp_dir):
    result = get_schema_info(temp_dir)
    assert result == []


def test_get_schema_info_multiple_groups(temp_dir):
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(temp_dir, "b.csv"), "city,country,zip", "SP,BR,01310")

    info = get_schema_info(temp_dir)
    assert len(info) == 2
    total_files = sum(entry["file_count"] for entry in info)
    assert total_files == 2


def test_get_schema_info_with_min_similarity(temp_dir):
    # Jaccard=0.8; with threshold 0.9 they become 2 groups
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name,age,city", "1,Alice,30,SP")
    _write_csv(os.path.join(temp_dir, "b.csv"), "id,name,age,city,country", "2,Bob,25,SP,BR")

    info_default = get_schema_info(temp_dir, min_similarity=0.8)
    info_strict = get_schema_info(temp_dir, min_similarity=0.9)

    assert len(info_default) == 1
    assert len(info_strict) == 2


# ---------------------------------------------------------------------------
# scan_schema
# ---------------------------------------------------------------------------

def test_scan_schema_does_not_raise(temp_dir):
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")
    scan_schema(temp_dir)  # should not raise


def test_scan_schema_empty_directory_does_not_raise(temp_dir):
    scan_schema(temp_dir)  # should not raise


def test_scan_schema_with_min_similarity_does_not_raise(temp_dir):
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(temp_dir, "b.csv"), "id,name,age", "1,Alice,30")
    scan_schema(temp_dir, min_similarity=0.9)  # should not raise


def test_scan_schema_returns_same_structure_as_get_schema_info(temp_dir):
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(temp_dir, "b.csv"), "city,country", "SP,BR")

    scan_result = scan_schema(temp_dir)
    info_result = get_schema_info(temp_dir)

    assert scan_result == info_result


def test_scan_schema_nonexistent_path_returns_empty_list():
    result = scan_schema("/nonexistent/path/xyz")
    assert result == []


# ---------------------------------------------------------------------------
# nonexistent path
# ---------------------------------------------------------------------------

def test_read_data_nonexistent_path_returns_none(spark):
    result = read_data("/nonexistent/path/xyz", spark=spark)
    assert result is None


def test_get_schema_info_nonexistent_path_returns_empty_list():
    result = get_schema_info("/nonexistent/path/xyz")
    assert result == []


# ---------------------------------------------------------------------------
# recursive scanning
# ---------------------------------------------------------------------------

def test_read_data_recursive_finds_nested_files(spark, temp_dir):
    subdir = os.path.join(temp_dir, "subdir")
    os.makedirs(subdir)
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(subdir, "b.csv"), "id,name", "2,Bob")

    df_non_recursive = read_data(temp_dir, spark=spark, recursive=False)
    df_recursive = read_data(temp_dir, spark=spark, recursive=True)

    assert df_non_recursive.count() == 1
    assert df_recursive.count() == 2


def test_get_schema_info_recursive_finds_nested_files(temp_dir):
    subdir = os.path.join(temp_dir, "subdir")
    os.makedirs(subdir)
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(subdir, "b.csv"), "id,name", "2,Bob")

    info_flat = get_schema_info(temp_dir, recursive=False)
    info_recursive = get_schema_info(temp_dir, recursive=True)

    assert info_flat[0]["file_count"] == 1
    assert info_recursive[0]["file_count"] == 2


def test_scan_schema_recursive_finds_nested_files(temp_dir):
    subdir = os.path.join(temp_dir, "subdir")
    os.makedirs(subdir)
    _write_csv(os.path.join(temp_dir, "a.csv"), "id,name", "1,Alice")
    _write_csv(os.path.join(subdir, "b.csv"), "id,name", "2,Bob")

    result_flat = scan_schema(temp_dir, recursive=False)
    result_recursive = scan_schema(temp_dir, recursive=True)

    assert result_flat[0]["file_count"] == 1
    assert result_recursive[0]["file_count"] == 2

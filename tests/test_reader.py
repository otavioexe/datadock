import os
import json
import pytest
from datadock._reader import _load_file, _read_schema_only


def test_load_csv(spark, temp_dir):
    df = spark.createDataFrame([("Alice", 1), ("Bob", 2)], ["name", "id"])
    path = os.path.join(temp_dir, "sample.csv")
    df.write.option("header", True).csv(path)

    read_df = _load_file(spark, path)
    assert read_df.count() == 2
    assert set(read_df.columns) == {"name", "id"}


def test_load_csv_semicolon(spark, temp_dir):
    path = os.path.join(temp_dir, "sample_semicolon.csv")
    with open(path, "w", encoding="utf-8") as f:
        f.write("name;id\nAlice;1\nBob;2\n")

    read_df = _load_file(spark, path)
    assert read_df.count() == 2
    assert set(read_df.columns) == {"name", "id"}

    schema = _read_schema_only(path)
    assert schema == [("name", "string"), ("id", "string")]


def test_load_json(spark, temp_dir):
    df = spark.createDataFrame([{"name": "Alice", "id": 1}, {"name": "Bob", "id": 2}])
    path = os.path.join(temp_dir, "sample.json")
    df.write.json(path)

    read_df = _load_file(spark, path)
    assert read_df.count() == 2
    assert "name" in read_df.columns


def test_load_parquet(spark, temp_dir):
    df = spark.createDataFrame([("Alice", 1)], ["name", "id"])
    path = os.path.join(temp_dir, "sample.parquet")
    df.write.parquet(path)

    read_df = _load_file(spark, path)
    assert read_df.count() == 1
    assert "id" in read_df.columns


def test_load_unsupported_extension(spark):
    path = "/tmp/file.unsupported"
    result = _load_file(spark, path)
    assert result is None


# ---------------------------------------------------------------------------
# _read_schema_only — JSON new scenarios
# ---------------------------------------------------------------------------

def test_read_schema_only_json_multiline(temp_dir):
    path = os.path.join(temp_dir, "multiline.json")
    with open(path, "w", encoding="utf-8") as f:
        f.write('{\n  "id": 1,\n  "name": "Alice",\n  "city": "SP"\n}')

    schema = _read_schema_only(path)
    assert schema is not None
    assert set(col for col, _ in schema) == {"id", "name", "city"}


def test_read_schema_only_json_array(temp_dir):
    path = os.path.join(temp_dir, "array.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], f)

    schema = _read_schema_only(path)
    assert schema is not None
    assert set(col for col, _ in schema) == {"id", "name"}


def test_read_schema_only_json_empty_file(temp_dir):
    path = os.path.join(temp_dir, "empty.json")
    open(path, "w").close()

    result = _read_schema_only(path)
    assert result is None


def test_read_schema_only_json_array_empty(temp_dir):
    path = os.path.join(temp_dir, "empty_array.json")
    with open(path, "w", encoding="utf-8") as f:
        f.write("[]")

    result = _read_schema_only(path)
    assert result is None


# ---------------------------------------------------------------------------
# _read_schema_only — CSV edge cases
# ---------------------------------------------------------------------------

def test_read_schema_only_csv_header_only(temp_dir):
    path = os.path.join(temp_dir, "header_only.csv")
    with open(path, "w", encoding="utf-8") as f:
        f.write("id,name,age\n")

    schema = _read_schema_only(path)
    assert schema == [("id", "string"), ("name", "string"), ("age", "string")]


def test_read_schema_only_csv_pipe_delimiter(temp_dir):
    path = os.path.join(temp_dir, "pipe.csv")
    with open(path, "w", encoding="utf-8") as f:
        f.write("id|name|city\n1|Alice|SP\n2|Bob|RJ\n")

    schema = _read_schema_only(path)
    assert schema is not None
    assert set(col for col, _ in schema) == {"id", "name", "city"}


def test_read_schema_only_csv_tab_delimiter(temp_dir):
    path = os.path.join(temp_dir, "tab.csv")
    with open(path, "w", encoding="utf-8") as f:
        f.write("id\tname\tcountry\n1\tAlice\tBR\n")

    schema = _read_schema_only(path)
    assert schema is not None
    assert set(col for col, _ in schema) == {"id", "name", "country"}


def test_read_schema_only_nonexistent_file(temp_dir):
    path = os.path.join(temp_dir, "does_not_exist.csv")
    result = _read_schema_only(path)
    assert result is None


# ---------------------------------------------------------------------------
# _load_file — edge cases
# ---------------------------------------------------------------------------

def test_load_csv_header_only_returns_empty_dataframe(spark, temp_dir):
    path = os.path.join(temp_dir, "header_only.csv")
    with open(path, "w", encoding="utf-8") as f:
        f.write("id,name,age\n")

    df = _load_file(spark, path)
    assert df is not None
    assert df.count() == 0
    assert set(df.columns) == {"id", "name", "age"}


def test_load_txt_file_returns_none(spark, temp_dir):
    path = os.path.join(temp_dir, "notes.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("hello world\nfoo bar\n")

    result = _load_file(spark, path)
    assert result is None

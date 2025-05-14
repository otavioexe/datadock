import os
import pytest
from pyspark.sql import SparkSession
from datadock._reader import _load_file


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("DatadockTest")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir(tmp_path):
    return str(tmp_path)


def test_load_csv(spark, temp_dir):
    df = spark.createDataFrame([("Alice", 1), ("Bob", 2)], ["name", "id"])
    path = os.path.join(temp_dir, "sample.csv")
    df.write.option("header", True).csv(path)

    read_df = _load_file(spark, path)
    assert read_df.count() == 2
    assert set(read_df.columns) == {"name", "id"}


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


def test_load_txt(spark, temp_dir):
    df = spark.createDataFrame([("linha1",), ("linha2",)], ["value"])
    path = os.path.join(temp_dir, "sample.txt")
    df.write.text(path)

    read_df = _load_file(spark, path)
    assert read_df.count() == 2
    assert "value" in read_df.columns


def test_load_unsupported_extension(spark):
    path = "/tmp/file.unsupported"
    result = _load_file(spark, path)
    assert result is None
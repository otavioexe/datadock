import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("DatadockTest")
        .master("local[*]")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def temp_dir(tmp_path):
    return str(tmp_path)

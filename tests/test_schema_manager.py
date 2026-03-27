import os
import pytest
from datadock._schema_manager import (
    _schema_similarity,
    _group_by_schema,
    _read_schema_group,
)


# ---------------------------------------------------------------------------
# _schema_similarity
# ---------------------------------------------------------------------------

def test_similarity_identical_schemas():
    a = [("id", "string"), ("name", "string")]
    assert _schema_similarity(a, a) == 1.0


def test_similarity_completely_different_schemas():
    a = [("id", "string"), ("name", "string")]
    b = [("city", "string"), ("country", "string")]
    assert _schema_similarity(a, b) == 0.0


def test_similarity_partial_overlap():
    a = [("id", "string"), ("name", "string"), ("age", "string")]
    b = [("id", "string"), ("name", "string"), ("city", "string")]
    # intersection: {id, name} = 2, union: {id, name, age, city} = 4
    assert _schema_similarity(a, b) == 0.5


def test_similarity_ignores_data_types():
    a = [("id", "string"), ("value", "string")]
    b = [("id", "int"), ("value", "double")]
    assert _schema_similarity(a, b) == 1.0


def test_similarity_both_empty():
    assert _schema_similarity([], []) == 0


def test_similarity_one_empty():
    a = [("id", "string")]
    assert _schema_similarity(a, []) == 0.0


# ---------------------------------------------------------------------------
# _group_by_schema (uses real CSV files)
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_dir(tmp_path):
    return str(tmp_path)


def _write_csv(path: str, header: str, row: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"{header}\n{row}\n")


def test_group_identical_schemas(temp_dir):
    p1 = os.path.join(temp_dir, "a.csv")
    p2 = os.path.join(temp_dir, "b.csv")
    _write_csv(p1, "id,name", "1,Alice")
    _write_csv(p2, "id,name", "2,Bob")

    grouped = _group_by_schema([p1, p2])

    assert len(grouped) == 1
    assert len(grouped[1]) == 2


def test_group_different_schemas(temp_dir):
    p1 = os.path.join(temp_dir, "a.csv")
    p2 = os.path.join(temp_dir, "b.csv")
    _write_csv(p1, "id,name", "1,Alice")
    _write_csv(p2, "city,country,zip", "SP,BR,01310")

    grouped = _group_by_schema([p1, p2])

    assert len(grouped) == 2


def test_group_similar_but_not_identical_schemas(temp_dir):
    p1 = os.path.join(temp_dir, "a.csv")
    p2 = os.path.join(temp_dir, "b.csv")
    # intersection={id,name,age,city}=4, union={id,name,age,city,country}=5 → Jaccard=0.8
    _write_csv(p1, "id,name,age,city", "1,Alice,30,SP")
    _write_csv(p2, "id,name,age,city,country", "2,Bob,25,SP,BR")

    grouped = _group_by_schema([p1, p2], min_similarity=0.8)

    assert len(grouped) == 1


def test_group_min_similarity_splits_groups(temp_dir):
    p1 = os.path.join(temp_dir, "a.csv")
    p2 = os.path.join(temp_dir, "b.csv")
    # intersection={id,name,age,city}=4, union={id,name,age,city,country}=5 → Jaccard=0.8
    _write_csv(p1, "id,name,age,city", "1,Alice,30,SP")
    _write_csv(p2, "id,name,age,city,country", "2,Bob,25,SP,BR")

    # threshold above 0.8 forces them into separate groups
    grouped = _group_by_schema([p1, p2], min_similarity=0.9)

    assert len(grouped) == 2


def test_group_skips_unreadable_file(temp_dir):
    p1 = os.path.join(temp_dir, "a.csv")
    _write_csv(p1, "id,name", "1,Alice")
    missing = os.path.join(temp_dir, "missing.csv")

    grouped = _group_by_schema([p1, missing])

    assert len(grouped) == 1


def test_group_empty_input():
    grouped = _group_by_schema([])
    assert grouped == {}


# ---------------------------------------------------------------------------
# _read_schema_group
# ---------------------------------------------------------------------------

def test_read_schema_group_returns_correct_paths():
    grouped = {
        1: [("/data/a.csv", [("id", "string")]), ("/data/b.csv", [("id", "string")])],
        2: [("/data/c.csv", [("city", "string")])],
    }

    result = _read_schema_group(grouped, schema_id=1)

    assert result == ["/data/a.csv", "/data/b.csv"]


def test_read_schema_group_defaults_to_first_id():
    grouped = {
        1: [("/data/a.csv", [("id", "string")])],
        2: [("/data/b.csv", [("city", "string")])],
    }

    result = _read_schema_group(grouped)

    assert result == ["/data/a.csv"]


def test_read_schema_group_empty_dict():
    result = _read_schema_group({})
    assert result is None

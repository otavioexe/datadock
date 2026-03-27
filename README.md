# Datadock

**Datadock** is a Python library built on top of PySpark, designed to simplify **data interoperability** between files of different formats and schemas in modern data engineering pipelines.

It automatically detects schemas from CSV, JSON and Parquet files, groups structurally similar files, and allows standardized reading of all grouped files into a single Spark DataFrame — even in highly heterogeneous datasets.


## ✨ Key Features

- 🚀 **Automatic parsing** of multiple file formats: `.csv`, `.json`, `.parquet`
- 🧠 **Schema-based file grouping** by structural similarity
- 📊 **Auto-selection of dominant schemas**
- 🛠️ **Unified read** across similar files into a single PySpark DataFrame
- 🔍 **Schema insight** for diagnostics and inspection


## 🔧 Installation

```bash
pip install datadock
```


## 🗂️ Expected Input Structure

Place your data files (CSV, JSON or Parquet) inside a folder. The library will automatically detect supported files and organize them by schema similarity.

```bash
/data/input/
├── sales_2020.csv
├── sales_2021.csv
├── products.json
├── archive.parquet
├── log.parquet
```


## 🧪 Usage Example

```python
from datadock import scan_schema, get_schema_info, read_data

path = "/path/to/your/data"

# Logs schema groups detected and returns their metadata
scan_schema(path)

# Retrieves schema metadata programmatically
info = get_schema_info(path)
print(info)

# Loads all files from schema group 1 into a single DataFrame
df = read_data(path, schema_id=1, logs=True)
df.show()
```


## 📌 Public API

### `scan_schema(path, min_similarity=0.8, recursive=False)`

Logs the identified schema groups found in the specified folder and returns their metadata (same structure as `get_schema_info`).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` | — | Folder containing data files |
| `min_similarity` | `float` | `0.8` | Minimum Jaccard similarity (0–1) to group files together |
| `recursive` | `bool` | `False` | Whether to scan subdirectories recursively |


### `get_schema_info(path, min_similarity=0.8, recursive=False)`

Returns a list of dictionaries containing:
- `schema_id`: ID of the schema group
- `file_count`: number of files in the group
- `column_count`: number of columns in the schema
- `files`: list of file names in the group

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` | — | Folder containing data files |
| `min_similarity` | `float` | `0.8` | Minimum Jaccard similarity (0–1) to group files together |
| `recursive` | `bool` | `False` | Whether to scan subdirectories recursively |


### `read_data(path, schema_id=None, logs=False, spark=None, min_similarity=0.8, recursive=False)`

Reads and merges all files that share the same schema into a single Spark DataFrame.
If `schema_id` is not specified, defaults to schema group 1 (first detected).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` | — | Folder containing data files |
| `schema_id` | `int` | `None` | ID of the schema group to read. Defaults to `1` |
| `logs` | `bool` | `False` | Whether to print detailed logs during loading |
| `spark` | `SparkSession` | `None` | Active SparkSession to use. Creates one if not provided |
| `min_similarity` | `float` | `0.8` | Minimum Jaccard similarity (0–1) to group files together |
| `recursive` | `bool` | `False` | Whether to scan subdirectories recursively |


## ✅ Requirements

- Python 3.10+
- PySpark


## 📚 Motivation

In real-world data engineering workflows, it's common to deal with files that represent the same data domain but have slight structural variations — such as missing columns, different orders, or evolving schemas.
**Datadock** automates the process of grouping, inspecting, and reading these files reliably, allowing you to build pipelines that are schema-aware, scalable, and format-agnostic.


## 📄 License

This project is licensed under the **MIT License**.

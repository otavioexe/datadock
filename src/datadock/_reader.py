from pyspark.sql import SparkSession, DataFrame
import csv
import json
from pathlib import Path
from typing import Optional, List, Tuple
import pyarrow.parquet as pq
from datadock._utils import logger

CSV_CANDIDATE_DELIMITERS = [",", ";", "\t", "|", ":"]


def _resolve_csv_source(path: str) -> Optional[Path]:
    """
    Resolve the real file used to inspect CSV content.
    Supports plain files and Spark output directories.
    """
    source = Path(path)
    if source.is_file():
        return source

    if source.is_dir():
        for file in sorted(source.iterdir()):
            if file.is_file() and not file.name.startswith((".", "_")):
                return file

    return None


def _detect_csv_delimiter(path: str) -> str:
    """
    Detect delimiter from a sample CSV file. Falls back to comma.
    """
    source = _resolve_csv_source(path)
    if source is None:
        return ","

    try:
        with open(source, encoding="utf-8", newline="") as f:
            sample = f.read(8192)
            if not sample.strip():
                return ","

            dialect = csv.Sniffer().sniff(sample, delimiters="".join(CSV_CANDIDATE_DELIMITERS))
            return dialect.delimiter
    except Exception:
        return ","


def _read_schema_only(path: str) -> Optional[List[Tuple[str, str]]]:
    """
    Reads only the schema (field names and types) from a file without loading data.
    """
    ext = Path(path).suffix.lower()

    try:
        if ext == ".csv":
            source = _resolve_csv_source(path)
            if source is None:
                return None

            delimiter = _detect_csv_delimiter(path)
            with open(source, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                columns = reader.fieldnames
                if columns:
                    return [(col, "string") for col in columns]

        elif ext == ".json":
            with open(path, encoding='utf-8') as f:
                first_line = f.readline().strip()
                if not first_line:
                    return None

                # Fast path: JSONL (one object per line)
                try:
                    parsed = json.loads(first_line)
                    if isinstance(parsed, dict):
                        return [(key, "string") for key in parsed.keys()]
                except json.JSONDecodeError:
                    pass

                # Slow path: multi-line JSON object or array
                f.seek(0)
                parsed = json.loads(f.read())
                if isinstance(parsed, dict):
                    return [(key, "string") for key in parsed.keys()]
                if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                    return [(key, "string") for key in parsed[0].keys()]

        elif ext == ".parquet":
            schema = pq.read_schema(path)
            return [(col.name, str(col.type)) for col in schema]

        else:
            logger.warning(f"[WARNING] Unsupported file extension: {ext}")
            return None

    except Exception as e:
        logger.error(f"Failed to read schema for {path}: {e}")
        return None
    

def _load_file(spark: SparkSession, file: str) -> Optional[DataFrame]:
    """
    Loads a file in the appropriate format (CSV, JSON, Parquet, Avro, TXT).
    
    Args:
        spark (SparkSession): The active Spark session.
        file (str): The path to the file.

    Returns:
        Optional[DataFrame]: A Spark DataFrame if successful, None otherwise.
    """
    ext = Path(file).suffix.lower()

    try:
        if ext == ".csv":
            delimiter = _detect_csv_delimiter(file)
            return spark.read.option("header", True).option("sep", delimiter).csv(file)
        elif ext == ".json":
            return spark.read.option("multiline", True).json(file)
        elif ext == ".parquet":
            return spark.read.parquet(file)
        elif ext == ".txt":
            return spark.read.text(file)
        else:
            logger.warning(f"Unsupported file format: {ext}")
            return None
    except Exception as e:
        logger.error(f"Error loading file {file}: {e}")
        return None

from pyspark.sql import SparkSession, DataFrame
import csv
import json
from pathlib import Path
from typing import Optional, List, Tuple
import pyarrow.parquet as pq
from datadock._utils import logger

def _read_schema_only(path: str) -> Optional[List[Tuple[str, str]]]:
    """
    Reads only the schema (field names and types) from a file without loading data.
    """
    ext = Path(path).suffix.lower()

    try:
        if ext == ".csv":
            with open(path, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                columns = reader.fieldnames
                if columns:
                    return [(col, "string") for col in columns]

        elif ext == ".json":
            with open(path, encoding='utf-8') as f:
                line = json.loads(f.readline())
                if isinstance(line, dict):
                    return [(key, "string") for key in line.keys()]

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
            return spark.read.option("header", True).csv(file)
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
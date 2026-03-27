from pyspark.sql import SparkSession, DataFrame
from datadock._schema_manager import _group_by_schema, _read_schema_group
from datadock._reader import _load_file
from datadock._utils import logger
from pathlib import Path
from typing import Optional, List, Dict, Any

SUPPORTED_EXTENSIONS = {".csv", ".json", ".parquet"}


def _normalize_path(path: str) -> str:
    """
    Normalizes Databricks DBFS paths to the local FUSE mount format.
    Converts 'dbfs:/path' and 'dbfs://path' to '/dbfs/path'.
    All other paths are returned unchanged.
    """
    if path.startswith("dbfs://"):
        return "/dbfs/" + path[len("dbfs://"):]
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/"):]
    return path


def _collect_paths(data_dir: Path, recursive: bool) -> List[str]:
    pattern = "**/*" if recursive else "*"
    return [
        str(p) for p in data_dir.glob(pattern)
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
    ]


def scan_schema(path: str, min_similarity: float = 0.8, recursive: bool = False) -> List[Dict[str, Any]]:
    """
    Scans and logs schema groupings for all supported files in the specified path.
    Returns the same structure as get_schema_info.

    :param path: Folder containing data files
    :param min_similarity: Minimum Jaccard similarity (0–1) to group files together. Defaults to 0.8.
    :param recursive: Whether to scan subdirectories recursively. Defaults to False.
    :return: List of dicts with schema_id, file_count, column_count, and files.
    """
    path = _normalize_path(path)
    data_dir = Path(path)
    if not data_dir.exists():
        logger.error(f"Path does not exist: {path}")
        return []

    info = get_schema_info(path, min_similarity=min_similarity, recursive=recursive)

    if not info:
        logger.warning("No supported data files found.")
        return []

    total_files = sum(e["file_count"] for e in info)
    logger.info(f"Found {total_files} file(s) in {len(info)} schema group(s).")

    for entry in info:
        logger.info(f"Schema {entry['schema_id']}: {entry['column_count']} columns – {entry['file_count']} file(s):")
        for filename in entry["files"]:
            logger.info(f"  • {filename}")

    return info


def read_data(
    path: str,
    schema_id: Optional[int] = None,
    logs: bool = False,
    spark: Optional[SparkSession] = None,
    min_similarity: float = 0.8,
    recursive: bool = False,
) -> Optional[DataFrame]:
    """
    Reads and merges all files that belong to a schema group.

    :param path: Folder containing data files
    :param schema_id: ID of the schema group to read. Defaults to schema 1 (first detected group).
    :param logs: Whether to print detailed logs during loading
    :param spark: Active SparkSession to use. If not provided, gets or creates one.
    :param min_similarity: Minimum Jaccard similarity (0–1) to group files together. Defaults to 0.8.
    :param recursive: Whether to scan subdirectories recursively. Defaults to False.
    :return: Spark DataFrame or None if load failed
    """
    if spark is None:
        spark = SparkSession.builder.appName("Datadock").getOrCreate()

    path = _normalize_path(path)
    data_dir = Path(path)
    if not data_dir.exists():
        logger.error(f"Path does not exist: {path}")
        return None

    paths = _collect_paths(data_dir, recursive)
    if not paths:
        if logs:
            logger.warning("No supported data files found.")
        return None

    if logs:
        logger.info(f"Found {len(paths)} files to process.")
    grouped = _group_by_schema(paths, min_similarity=min_similarity)

    if len(grouped) > 1 and logs:
        logger.warning(f"Multiple schemas found in path '{path}'. Total: {len(grouped)}")

    if schema_id is None:
        if logs:
            logger.warning("Schema ID not specified. Defaulting to schema 1.")
        schema_id = 1

    if schema_id not in grouped:
        logger.error(f"Schema ID {schema_id} not found among detected schema groups. Available IDs: {list(grouped.keys())}")
        return None

    selected_files = _read_schema_group(grouped, schema_id=schema_id)
    if not selected_files:
        if logs:
            logger.warning(f"No dataset found for schema {schema_id}.")
        return None

    if logs:
        logger.info(f"Reading data from schema group {schema_id}")

    dfs = []
    for file in selected_files:
        df = _load_file(spark, file)
        if df:
            dfs.append(df)
            if logs:
                logger.info(f"Loaded file: {file}")

    if not dfs:
        if logs:
            logger.warning("No DataFrames were loaded.")
        return None

    final_df = dfs[0]
    for df in dfs[1:]:
        try:
            final_df = final_df.unionByName(df, allowMissingColumns=True)
        except Exception as e:
            logger.error(f"Error merging DataFrames: {e}")
            return None

    if logs:
        logger.info("Dataset successfully loaded.")

    return final_df


def get_schema_info(path: str, min_similarity: float = 0.8, recursive: bool = False) -> List[Dict[str, Any]]:
    """
    Returns detailed information about schema groups detected in the given directory.

    :param path: Path to the folder containing raw data files.
    :param min_similarity: Minimum Jaccard similarity (0–1) to group files together. Defaults to 0.8.
    :param recursive: Whether to scan subdirectories recursively. Defaults to False.
    :return: A list of dictionaries with schema_id, file count, column count, and list of files.
    """
    path = _normalize_path(path)
    data_dir = Path(path)
    if not data_dir.exists():
        logger.error(f"Path does not exist: {path}")
        return []

    paths = _collect_paths(data_dir, recursive)
    if not paths:
        logger.warning("No supported data files found in the provided directory.")
        return []

    grouped = _group_by_schema(paths, min_similarity=min_similarity)
    schema_info = []

    for schema_id, group in grouped.items():
        file_list = [Path(file_path).name for file_path, _ in group]
        column_count = len(group[0][1]) if group else 0

        schema_info.append({
            "schema_id": schema_id,
            "file_count": len(file_list),
            "column_count": column_count,
            "files": file_list
        })

    return schema_info

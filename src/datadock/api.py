from pyspark.sql import SparkSession, DataFrame
from datadock._schema_manager import _group_by_schema, _read_schema_group
from datadock._reader import _load_file
from datadock._utils import logger
from pathlib import Path
from typing import Optional, List, Dict, Any

SUPPORTED_EXTENSIONS = {".csv", ".json", ".parquet"}

def scan_schema(path: str):
    """
    Scans and prints schema groupings for all supported files in the specified path.
    This is the public entry point for schema inspection.
    """
    data_dir = Path(path)
    paths = [str(p) for p in data_dir.glob("*") if p.suffix.lower() in SUPPORTED_EXTENSIONS]

    if not paths:
        logger.warning("No supported data files found.")
        return

    logger.info(f"Found {len(paths)} files to process.")
    grouped = _group_by_schema(paths)

    if len(grouped) > 1:
        logger.info(f"Found {len(grouped)} different schemas.")
    else:
        logger.info("Found 1 schema.")

    for sid, group in grouped.items():
        col_count = len(group[0][1])
        logger.info(f"Schema {sid}: {col_count} columns – {len(group)} file(s):")
        for file_path, _ in group:
            logger.info(f"  • {file_path}")

def read_data(path: str, schema_id: Optional[int] = None, logs: bool = False) -> Optional[DataFrame]:
    """
    Reads and merges all files that belong to a schema group.

    :param path: Folder containing data files
    :param schema_id: ID of the schema group to read (defaults to most complex)
    :param logs: Whether to print detailed logs during loading
    :return: Spark DataFrame or None if load failed
    """
    spark = SparkSession.builder.appName("Databridge").getOrCreate()

    data_dir = Path(path)
    paths = [str(p) for p in data_dir.glob("*") if p.suffix.lower() in SUPPORTED_EXTENSIONS]
    if not paths:
        if logs:
            logger.warning("No supported data files found.")
        return None

    if logs:
        logger.info(f"Found {len(paths)} files to process.")
    elif logs == False:
        logger.info(f"For more details about the reading process, set `logs=True`.")
    grouped = _group_by_schema(paths)

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
            final_df = final_df.unionByName(df)
        except Exception as e:
            if logs:
                logger.error(f"Error merging DataFrames: {e}")

    if logs:
        logger.info("Dataset successfully loaded.")

    return final_df


def get_schema_info(path: str) -> List[Dict[str, Any]]:
    """
    Returns detailed information about schema groups detected in the given directory.

    :param path: Path to the folder containing raw data files.
    :return: A list of dictionaries with schema_id, file count, column count, and list of files.
    """
    data_dir = Path(path)
    paths = [str(p) for p in data_dir.glob("*") if p.suffix.lower() in SUPPORTED_EXTENSIONS]

    if not paths:
        logger.warning("No supported data files found in the provided directory.")
        return []

    grouped = _group_by_schema(paths)
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
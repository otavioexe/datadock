from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from datadock._reader import _read_schema_only
from datadock._utils import logger
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from pathlib import Path


def _extract_schema_signature(schema: List[Tuple[str, str]]) -> Tuple[str, ...]:
    """
    Create a normalized schema signature: tuple of (column_name, type), sorted by column name.
    """
    return tuple(sorted((col, dtype) for col, dtype in schema))

def _structtype_to_list(schema: StructType) -> List[Tuple[str, str]]:
    """
    Convert a Spark StructType schema to a list of (column_name, data_type) tuples.
    """
    return [(field.name, field.dataType.simpleString()) for field in schema]


def _group_by_schema(
    paths: List[str]
) -> Dict[int, List[Tuple[str, List[Tuple[str, str]]]]]:
    """
    Group files by identical schema, independent of file format.
    Rank schemas by number of columns.

    Args:
        paths (List[str]): list of file paths

    Returns:
        Dict[schema_id, List[(path, schema)]] - Groups of schemas with file paths.
    """
    schema_groups = defaultdict(list)

    for path in paths:
        schema = _read_schema_only(path)
        if schema is None:
            logger.warning(f"Could not read dataset schema at path: {path}")
            continue

        schema_sig = _extract_schema_signature(schema)
        schema_groups[schema_sig].append((path, schema))

    ranked_schemas = sorted(schema_groups.items(), key=lambda x: len(x[0]), reverse=True)

    grouped_by_id = {}
    for schema_id, (schema_sig, items) in enumerate(ranked_schemas, start=1):
        grouped_by_id[schema_id] = items

    return grouped_by_id


def _read_schema_group(
    grouped_by_id: Dict[int, List[Tuple[str, List[Tuple[str, str]]]]],
    schema_id: Optional[int] = None
) -> Optional[List[str]]:
    """
    Returns the file paths that share the same schema.
    If schema_id is not provided, returns the group with the most columns (rank 1).
    
    Args:
        grouped_by_id (Dict[int, List[Tuple[str, List[Tuple[str, str]]]]]): 
            A dictionary where the key is the schema ID and the value is a list of tuples 
            containing file paths and their associated schema.
        schema_id (Optional[int]): 
            The schema ID to retrieve. If not specified, defaults to the group with the most columns.

    Returns:
        Optional[List[str]]: 
            A list of file paths that share the specified schema. 
            Returns None if the group is not found or if there are no datasets.
    """
    if not grouped_by_id:
        logger.error("No datasets available to read.")
        return None

    selected_id = schema_id or sorted(grouped_by_id.keys())[0]
    logger.info(f"Selected dataset from schema group {selected_id}.")

    return [path for path, _ in grouped_by_id[selected_id]]
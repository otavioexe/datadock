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


def _group_by_schema(paths: List[str], min_similarity: float = 0.8) -> Dict[int, List[Tuple[str, List[Tuple[str, str]]]]]:
    """
    Groups files based on schema similarity using only column names.
    Files are assigned to the first group where the similarity exceeds the given threshold.
    If no group is similar enough, a new group is created.

    Args:
        paths (List[str]): 
            A list of file paths to be analyzed.
        min_similarity (float, optional): 
            Minimum similarity ratio (0–1) required to group files together. 
            Defaults to 0.8.

    Returns:
        Dict[int, List[Tuple[str, List[Tuple[str, str]]]]]: 
            A dictionary mapping each schema ID to a list of tuples, 
            each containing a file path and its inferred schema.
    """
    schema_groups = {}
    schema_signatures = {}
    next_id = 1

    for path in paths:
        schema = _read_schema_only(path)
        if schema is None:
            logger.warning(f"Could not read schema for file: {path}")
            continue

        matched = False
        for schema_id, ref_schema in schema_signatures.items():
            sim = _schema_similarity(schema, ref_schema)
            if sim >= min_similarity:
                schema_groups[schema_id].append((path, schema))
                matched = True
                break

        if not matched:
            schema_signatures[next_id] = schema
            schema_groups[next_id] = [(path, schema)]
            next_id += 1

    return schema_groups


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

def _schema_similarity(a: List[Tuple[str, str]], b: List[Tuple[str, str]]) -> float:
    """
    Calculates the similarity between two schemas based on column names only,
    ignoring data types. Uses Jaccard similarity.

    Args:
        a (List[Tuple[str, str]]): 
            Schema A as a list of (column name, data type) tuples.
        b (List[Tuple[str, str]]): 
            Schema B as a list of (column name, data type) tuples.

    Returns:
        float: 
            A value between 0.0 and 1.0 representing the schema similarity.
    """
    set_a = set(col_name for col_name, _ in a)
    set_b = set(col_name for col_name, _ in b)
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union) if union else 0
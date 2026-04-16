"""Shared utility functions for parquet dataset handling."""

from urllib.parse import urlparse
from urllib.request import url2pathname
from typing import Any, Mapping

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs


def _get_dataset(dataset_url: str) -> ds.Dataset:
    """
    Build a PyArrow dataset from a direct URL or local path.

    Parameters
    ----------
    dataset_url : str
        A direct parquet URL, a data-explorer wrapper URL, or a local path.

    Returns
    -------
    pyarrow.dataset.Dataset
        The dataset object used for reading.
    """
    parsed_url = urlparse(dataset_url)

    if parsed_url.scheme in {"http", "https"} and parsed_url.hostname:
        path_parts = parsed_url.path.strip("/").split("/", 1)
        if len(path_parts) != 2:
            raise ValueError(
                "Expected S3 URL with bucket and key, got: "
                f"{dataset_url}"
            )

        bucket_name, key = path_parts
        s3 = fs.S3FileSystem(endpoint_override=parsed_url.hostname, anonymous=True)
        dataset_path = f"{bucket_name}/{key}"
        return ds.dataset(dataset_path, filesystem=s3, format="parquet")

    if parsed_url.scheme == "file":
        local_path = url2pathname(parsed_url.path)
        if parsed_url.netloc:
            local_path = f"{parsed_url.netloc}{local_path}"
        return ds.dataset(local_path, format="parquet")
    return ds.dataset(dataset_url, format="parquet")


def _resolve_dataset(dataset_input: ds.Dataset | str) -> ds.Dataset:
    """Resolve dataset input to a Dataset object."""
    if isinstance(dataset_input, ds.Dataset):
        return dataset_input
    if isinstance(dataset_input, str):
        return _get_dataset(dataset_input)
    raise TypeError(
        "dataset_input must be a pyarrow.dataset.Dataset or a parquet URL/path string"
    )


def _resolve_columns(dataset: ds.Dataset, columns: list[str] | None) -> list[str]:
    """Resolve and validate requested columns."""
    if not columns:
        return [field.name for field in dataset.schema]

    missing_columns = [name for name in columns if name not in dataset.schema.names]
    if missing_columns:
        raise ValueError(f"Unknown columns: {missing_columns}")

    return columns


def _filter_items(filters: Mapping[str, Any] | list[tuple[str, Any]] | None) -> list[tuple[str, Any]]:
    """Convert filter input to list of tuples."""
    if filters is None:
        return []
    if isinstance(filters, Mapping):
        return list(filters.items())
    if isinstance(filters, list):
        return filters
    raise TypeError("filters must be a mapping, a list of (column, value), or None")


def _build_filter_expression(
    dataset: ds.Dataset,
    filters: Mapping[str, Any] | list[tuple[str, Any]] | None,
) -> ds.Expression | None:
    """Build PyArrow filter expression from filter input."""
    expression = None
    for column_name, filter_value in _filter_items(filters):
        if column_name not in dataset.schema.names:
            raise ValueError(f"Unknown filter column: {column_name}")

        field_type = dataset.schema.field(column_name).type
        is_string_type = pa.types.is_string(field_type) or pa.types.is_large_string(field_type)

        if isinstance(filter_value, (list, tuple, set, frozenset)):
            values = list(filter_value)
            if not values:
                raise ValueError(
                    f"Filter column '{column_name}' received an empty value list"
                )
            condition = pc.field(column_name).isin(values)
        elif filter_value is None:
            condition = pc.field(column_name).is_null()
        elif is_string_type and isinstance(filter_value, str) and filter_value.strip():
            condition = pc.match_substring(pc.field(column_name), filter_value)
        else:
            condition = pc.field(column_name) == filter_value

        expression = condition if expression is None else expression & condition

    return expression


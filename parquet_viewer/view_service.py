from __future__ import annotations

from collections import defaultdict
import csv
import json
from typing import Any, Mapping

import pyarrow as pa
import pyarrow.dataset as ds

from ._utils import _build_filter_expression, _resolve_columns, _resolve_dataset

DatasetInput = ds.Dataset | str
FilterInput = Mapping[str, Any] | list[tuple[str, Any]] | None

__all__ = [
    "DatasetInput",
    "FilterInput",
    "ParquetViewService",
    "get_view",
    "inspect_parquet",
    "get_schema",
]


def _value_sort_key(value: Any) -> tuple[int, str]:
    if value is None:
        return (0, "")
    return (1, str(value).casefold())


def _row_from_counts(
    dataset: ds.Dataset,
    column_name: str,
    value_counts: defaultdict[Any, int],
) -> list[str]:
    sorted_values = sorted(value_counts.items(), key=lambda item: _value_sort_key(item[0]))
    values_payload = [{"value": value, "count": count} for value, count in sorted_values]

    return [
        column_name,
        str(dataset.schema.field(column_name).type),
        json.dumps(values_payload, ensure_ascii=True),
    ]


class ParquetViewService:
    """Reusable parquet view service that caches dataset/file-system setup."""

    def __init__(self, parquet: DatasetInput):
        self._dataset = _resolve_dataset(parquet)

    @property
    def dataset(self) -> ds.Dataset:
        return self._dataset

    def get_schema(self, output_file: str | None = None) -> pa.Schema:
        schema = self._dataset.schema

        if output_file is not None:
            with open(output_file, "w", newline="", encoding="utf-8") as file_handle:
                writer = csv.writer(file_handle)
                writer.writerow(["name", "dtype"])
                for field in schema:
                    writer.writerow([field.name, str(field.type)])

        return schema

    def get_view(
        self,
        columns: list[str] | None = None,
        filters: FilterInput = None,
        max_rows: int = 25,
    ) -> pa.Table:
        selected_columns = _resolve_columns(self._dataset, columns)
        filter_expression = _build_filter_expression(self._dataset, filters)
        if max_rows < 0:
            raise ValueError("max_rows must be >= 0")
        selected_schema = pa.schema([self._dataset.schema.field(name) for name in selected_columns])

        if max_rows == 0:
            return pa.Table.from_arrays(
                [pa.array([], type=field.type) for field in selected_schema],
                names=selected_schema.names,
            )

        batches: list[pa.RecordBatch] = []
        rows_remaining = max_rows
        scanner = self._dataset.scanner(columns=selected_columns, filter=filter_expression)

        for batch in scanner.to_batches():
            if rows_remaining <= 0:
                break
            limited_batch = batch if batch.num_rows <= rows_remaining else batch.slice(0, rows_remaining)
            batches.append(limited_batch)
            rows_remaining -= limited_batch.num_rows

        if not batches:
            return pa.Table.from_arrays(
                [pa.array([], type=field.type) for field in selected_schema],
                names=selected_schema.names,
            )

        return pa.Table.from_batches(batches)

    def inspect(
        self,
        output_file: str,
        columns: list[str] | None = None,
        filters: FilterInput = None,
    ) -> str:
        selected_columns = _resolve_columns(self._dataset, columns)
        filter_expression = _build_filter_expression(self._dataset, filters)

        counts_by_column: dict[str, defaultdict[Any, int]] = {
            column_name: defaultdict(int) for column_name in selected_columns
        }

        table = self._dataset.to_table(columns=selected_columns, filter=filter_expression)
        for batch in table.to_batches():
            for column_name in selected_columns:
                counts_struct = batch[column_name].value_counts().to_pylist()
                for item in counts_struct:
                    counts_by_column[column_name][item["values"]] += int(item["counts"])

        with open(output_file, "w", newline="", encoding="utf-8") as file_handle:
            writer = csv.writer(file_handle)
            writer.writerow(["column_name", "column_type", "unique_values"])
            for column_name in selected_columns:
                writer.writerow(_row_from_counts(self._dataset, column_name, counts_by_column[column_name]))

        return output_file


def get_view(
    parquet: DatasetInput,
    columns: list[str] | None = None,
    filters: FilterInput = None,
    max_rows: int = 25,
) -> pa.Table:
    """Convenience entrypoint mirroring ``ParquetViewService.get_view``."""
    return ParquetViewService(parquet).get_view(
        columns=columns,
        filters=filters,
        max_rows=max_rows,
    )


def inspect_parquet(
    parquet: DatasetInput,
    output_file: str,
    columns: list[str] | None = None,
    filters: FilterInput = None,
) -> str:
    """Convenience entrypoint mirroring ``ParquetViewService.inspect``."""
    return ParquetViewService(parquet).inspect(
        output_file=output_file,
        columns=columns,
        filters=filters,
    )


def get_schema(parquet: DatasetInput, output_file: str | None = None) -> pa.Schema:
    """Convenience entrypoint mirroring ``ParquetViewService.get_schema``."""
    return ParquetViewService(parquet).get_schema(output_file=output_file)



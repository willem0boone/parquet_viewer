from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import csv
import json
from typing import Any, Mapping

import pyarrow as pa
import pyarrow.compute as pc
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

        for field in schema:
            print(f"{field.name}: {field.type}")

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
        output_file: str | None = None,
        logs: bool = True,
    ) -> dict[str, Any]:
        selected_columns = _resolve_columns(self._dataset, columns)
        filter_expression = _build_filter_expression(self._dataset, filters)

        if logs:
            print(
                f"{datetime.now()} | start get_view | "
                f"columns={len(selected_columns)}"
            )

        if filter_expression is None:
            scanner = self._dataset.scanner(columns=selected_columns, batch_size=max_rows * 10)
        else:
            scanner = self._dataset.scanner(columns=selected_columns, filter=filter_expression)

        display_data: list[dict[str, Any]] = []
        rows_collected = 0

        if logs:
            print(f"{datetime.now()} | reading first {max_rows} rows from parquet")

        for batch in scanner.to_batches():
            if rows_collected >= max_rows:
                break

            batch_list = batch.to_pylist()
            remaining_needed = max_rows - rows_collected
            rows_to_take = min(len(batch_list), remaining_needed)

            display_data.extend(batch_list[:rows_to_take])
            rows_collected += rows_to_take

        total_rows = len(display_data)

        if logs:
            print(f"{datetime.now()} | read {total_rows} rows total")

        csv_path = None
        if output_file:
            with open(output_file, "w", newline="", encoding="utf-8") as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=selected_columns)
                csv_writer.writeheader()
                csv_writer.writerows(display_data)
            csv_path = output_file
            if logs:
                print(f"{datetime.now()} | saved {total_rows} rows to {output_file}")

        print("\n" + "=" * 80)
        print(f"Parquet Data Summary ({total_rows}/{total_rows} rows displayed)")
        print("=" * 80)

        if display_data:
            print(" | ".join(f"{col:20s}" for col in selected_columns))
            print("-" * (len(selected_columns) * 22))
            for row in display_data:
                values = [str(row.get(col, ""))[:20] for col in selected_columns]
                print(" | ".join(f"{val:20s}" for val in values))
        else:
            print("(No rows match the filter criteria)")

        print("=" * 80 + "\n")

        if logs:
            print(f"{datetime.now()} | done get_view")

        return {
            "total_rows": total_rows,
            "displayed_rows": total_rows,
            "columns": selected_columns,
            "data": display_data,
            "output_file": csv_path,
        }

    def inspect(
        self,
        output_file: str,
        columns: list[str] | None = None,
        filters: FilterInput = None,
        logs: bool = True,
    ) -> str:
        selected_columns = _resolve_columns(self._dataset, columns)
        filter_expression = _build_filter_expression(self._dataset, filters)

        if logs:
            print(
                f"{datetime.now()} | start inspect_parquet | "
                f"columns={len(selected_columns)}"
            )

        counts_by_column: dict[str, defaultdict[Any, int]] = {
            column_name: defaultdict(int) for column_name in selected_columns
        }

        scanner = self._dataset.scanner(columns=selected_columns, filter=filter_expression)
        for batch_index, batch in enumerate(scanner.to_batches(), start=1):
            if logs:
                print(f"{datetime.now()} | inspect_parquet | batch {batch_index}")
            for column_name in selected_columns:
                counts_struct = pc.value_counts(batch[column_name]).to_pylist()
                for item in counts_struct:
                    counts_by_column[column_name][item["values"]] += int(item["counts"])

        with open(output_file, "w", newline="", encoding="utf-8") as file_handle:
            writer = csv.writer(file_handle)
            writer.writerow(["column_name", "column_type", "unique_values"])
            for column_name in selected_columns:
                writer.writerow(_row_from_counts(self._dataset, column_name, counts_by_column[column_name]))

        if logs:
            print(f"{datetime.now()} | done inspect_parquet")

        return output_file


def get_view(
    parquet: DatasetInput,
    columns: list[str] | None = None,
    filters: FilterInput = None,
    max_rows: int = 25,
    output_file: str | None = None,
    logs: bool = True,
) -> dict[str, Any]:
    """Convenience entrypoint mirroring ``ParquetViewService.get_view``."""
    return ParquetViewService(parquet).get_view(
        columns=columns,
        filters=filters,
        max_rows=max_rows,
        output_file=output_file,
        logs=logs,
    )


def inspect_parquet(
    parquet: DatasetInput,
    output_file: str,
    columns: list[str] | None = None,
    filters: FilterInput = None,
    logs: bool = True,
) -> str:
    """Convenience entrypoint mirroring ``ParquetViewService.inspect``."""
    return ParquetViewService(parquet).inspect(
        output_file=output_file,
        columns=columns,
        filters=filters,
        logs=logs,
    )


def get_schema(parquet: DatasetInput, output_file: str | None = None) -> pa.Schema:
    """Convenience entrypoint mirroring ``ParquetViewService.get_schema``."""
    return ParquetViewService(parquet).get_schema(output_file=output_file)



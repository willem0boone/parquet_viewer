from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pyarrow as pa
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .view_service import ParquetViewService


class ViewRequest(BaseModel):
    parquet_url: str = Field(..., min_length=1)
    max_rows: int = Field(default=25, ge=1, le=200)
    columns: list[str] | None = None
    filters: dict[str, Any] | None = None
    format: str = Field(default="json_columns", pattern="^(json_columns|arrow)$")


class SchemaRequest(BaseModel):
    parquet_url: str = Field(..., min_length=1)


class SchemaColumn(BaseModel):
    name: str
    dtype: str


class SchemaResponse(BaseModel):
    columns: list[SchemaColumn]


app = FastAPI(title="Parquet Viewer API")

# Minimal local dev CORS for the React app.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1)(:\d+)?",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _to_json_safe(value: Any) -> Any:
    """Convert nested values to JSON-safe primitives for API responses."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, Mapping):
        return {str(key): _to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_to_json_safe(item) for item in value]
    return str(value)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/view")
def get_view_endpoint(request: ViewRequest) -> Any:
    try:
        service = ParquetViewService(request.parquet_url)
        table = service.get_view(
            columns=request.columns,
            filters=request.filters,
            max_rows=request.max_rows,
        )

        if request.format == "arrow":
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            return Response(
                content=sink.getvalue().to_pybytes(),
                media_type="application/vnd.apache.arrow.stream",
            )

        return {"data": _to_json_safe(table.to_pydict())}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/schema", response_model=SchemaResponse)
def get_schema_endpoint(request: SchemaRequest) -> dict[str, list[dict[str, str]]]:
    try:
        service = ParquetViewService(request.parquet_url)
        schema = service.dataset.schema
        columns = [{"name": field.name, "dtype": str(field.type)} for field in schema]
        return {"columns": columns}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("parquet_viewer.api:app", host="127.0.0.1", port=8000, reload=True)


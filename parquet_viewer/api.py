from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .view_service_beta import DuckDBViewService


class ViewRequest(BaseModel):
    parquet_url: str = Field(..., min_length=1)
    max_rows: int = Field(default=25, ge=1, le=200)
    row_offset: int = Field(default=0, ge=0)
    columns: list[str] | None = None
    filters: dict[str, str] | None = None


class SchemaRequest(BaseModel):
    parquet_url: str = Field(..., min_length=1)


class SchemaColumn(BaseModel):
    name: str
    dtype: str


class SchemaResponse(BaseModel):
    columns: list[SchemaColumn]


class ViewResponse(BaseModel):
    total_rows: int
    displayed_rows: int
    columns: list[str]
    data: dict[str, list[Any]]
    output_file: str | None = None


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


@app.post("/view", response_model=ViewResponse)
def get_view_endpoint(request: ViewRequest) -> dict[str, Any]:
    try:
        service = DuckDBViewService(request.parquet_url)
        table = service.get_view(
            columns=request.columns,
            filters=request.filters,
            max_rows=request.max_rows,
            row_offset=request.row_offset,
        )
        columnar_data = {
            column_name: table.column(column_name).to_pylist()
            for column_name in table.column_names
        }
        payload = {
            "total_rows": table.num_rows,
            "displayed_rows": table.num_rows,
            "columns": table.column_names,
            "data": columnar_data,
            "output_file": None,
        }
        return _to_json_safe(payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/schema", response_model=SchemaResponse)
def get_schema_endpoint(request: SchemaRequest) -> dict[str, list[dict[str, str]]]:
    try:
        service = DuckDBViewService(request.parquet_url)
        schema = service.get_schema()
        columns = [{"name": name, "dtype": dtype} for name, dtype in schema.items()]
        return {"columns": columns}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("parquet_viewer.api:app", host="127.0.0.1", port=8000, reload=True)


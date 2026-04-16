from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dtotools")
except PackageNotFoundError:
    __version__ = "0.0.0"

from .view_service import (
    DatasetInput,
    FilterInput,
    ParquetViewService,
    get_schema,
    get_view,
    inspect_parquet,
)

__all__ = [
    "__version__",
    "DatasetInput",
    "FilterInput",
    "ParquetViewService",
    "get_schema",
    "get_view",
    "inspect_parquet",
]

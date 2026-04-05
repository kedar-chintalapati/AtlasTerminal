"""
Query router — direct SQL access to the DuckDB store.

Exposes an /api/v1/query endpoint that accepts SQL and returns results as JSON.
Powers the in-app research notebook.

Security note: this endpoint runs arbitrary SQL in the local DuckDB process.
It must NEVER be exposed to untrusted callers without authentication and
SQL allow-listing.  In the default config it is available only on localhost.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from atlas_app.backend.deps import get_store
from atlas_core.exceptions import StoreError, StoreTableNotFoundError
from atlas_core.store.duckdb_store import DuckDBStore

router = APIRouter(prefix="/query", tags=["query"])


class QueryRequest(BaseModel):
    sql: str = Field(..., min_length=1, description="SQL to execute against the DuckDB store")
    params: list[Any] = Field(default_factory=list, description="Positional parameters")
    limit: int = Field(default=10_000, ge=1, le=100_000)
    format: str = Field(default="records", pattern="^(records|columns)$")


class QueryResponse(BaseModel):
    columns: list[str]
    data: list[Any]
    row_count: int
    truncated: bool


@router.post("/sql", response_model=QueryResponse)
async def run_sql(
    req: QueryRequest,
    store: DuckDBStore = Depends(get_store),
) -> QueryResponse:
    """Execute SQL against the Atlas DuckDB store."""
    # Basic safety: disallow writes unless explicitly enabled
    sql_upper = req.sql.strip().upper()
    if any(sql_upper.startswith(kw) for kw in ("DROP", "DELETE", "TRUNCATE", "ALTER")):
        raise HTTPException(
            status_code=403,
            detail="DDL/DML write operations are not permitted via the query API",
        )
    try:
        df = store.query(req.sql, req.params or [])
    except StoreTableNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except StoreError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    truncated = len(df) > req.limit
    df = df.head(req.limit)

    if req.format == "columns":
        data: Any = {col: df[col].tolist() for col in df.columns}
    else:
        data = df.to_dict(orient="records")

    return QueryResponse(
        columns=list(df.columns),
        data=data,
        row_count=len(df),
        truncated=truncated,
    )


@router.get("/tables")
async def list_tables(store: DuckDBStore = Depends(get_store)) -> dict:
    """List all tables available in the store."""
    tables = store.list_tables()
    return {"tables": tables}


@router.get("/tables/{table_name}/schema")
async def table_schema(
    table_name: str,
    store: DuckDBStore = Depends(get_store),
) -> dict:
    """Describe the schema of a table."""
    try:
        df = store.table_schema(table_name)
        return {"table": table_name, "schema": df.to_dict(orient="records")}
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/tables/{table_name}/count")
async def table_count(
    table_name: str,
    store: DuckDBStore = Depends(get_store),
) -> dict:
    try:
        count = store.row_count(table_name)
        return {"table": table_name, "count": count}
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

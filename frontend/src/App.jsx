import React, { useEffect, useMemo, useRef, useState } from "react";

const API_BASE = "http://127.0.0.1:8000";
const DEFAULT_URL = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology/12639/marine_biodiversity_observations_2026-02-26.parquet";

export default function App() {
  const [parquetUrl, setParquetUrl] = useState(DEFAULT_URL);
  const [schemaColumns, setSchemaColumns] = useState([]);
  const [draftSelectedColumns, setDraftSelectedColumns] = useState([]);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [rendering, setRendering] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);

  const dropdownRef = useRef(null);
  const loadedSchemaUrlRef = useRef("");

  const allColumnNames = useMemo(() => schemaColumns.map((item) => item.name), [schemaColumns]);
  const selectedCount = draftSelectedColumns.length;
  const totalCount = allColumnNames.length;

  useEffect(() => {
    function onClickOutside(event) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setDropdownOpen(false);
      }
    }

    document.addEventListener("mousedown", onClickOutside);
    return () => document.removeEventListener("mousedown", onClickOutside);
  }, []);

  async function fetchSchema(url) {
    const response = await fetch(`${API_BASE}/schema`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ parquet_url: url })
    });

    const body = await response.json();

    if (!response.ok) {
      throw new Error(body.detail || "Failed to load schema");
    }

    const columns = Array.isArray(body.columns) ? body.columns : [];
    const names = columns.map((item) => item.name);

    return { columns, names };
  }

  async function renderData(url, columnsToRender) {
    const trimmedUrl = url.trim();
    if (!trimmedUrl || columnsToRender.length === 0) {
      return null;
    }

    const response = await fetch(`${API_BASE}/view`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        parquet_url: trimmedUrl,
        max_rows: 50,
        columns: columnsToRender,
        format: "json_columns"
      })
    });

    const body = await response.json();
    if (!response.ok) {
      throw new Error(body.detail || "Failed to load view");
    }

    return body.data || {};
  }

  function toRows(columnarData, columns) {
    const firstColumn = columns[0];
    const rowCount = firstColumn ? (columnarData[firstColumn] || []).length : 0;
    const rows = [];

    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      const row = {};
      columns.forEach((column) => {
        const values = columnarData[column] || [];
        row[column] = values[rowIndex];
      });
      rows.push(row);
    }

    return rows;
  }

  function toggleColumn(name) {
    setDraftSelectedColumns((previous) =>
      previous.includes(name)
        ? previous.filter((item) => item !== name)
        : [...previous, name]
    );
  }

  async function handleRenderClick() {
    const trimmedUrl = parquetUrl.trim();
    if (!trimmedUrl) {
      setError("Paste a parquet URL first.");
      return;
    }

    if (draftSelectedColumns.length === 0) {
      setError("Select at least one column.");
      return;
    }

    setRendering(true);
    setError("");

    try {
      let columnsToRender = draftSelectedColumns;
      if (loadedSchemaUrlRef.current !== trimmedUrl || schemaColumns.length === 0) {
        const schemaPayload = await fetchSchema(trimmedUrl);
        setSchemaColumns(schemaPayload.columns);
        setDraftSelectedColumns(schemaPayload.names);
        columnsToRender = schemaPayload.names;
        loadedSchemaUrlRef.current = trimmedUrl;
      }

      if (columnsToRender.length === 0) {
        setError("No columns found in parquet schema.");
        setResult(null);
        return;
      }

      const data = await renderData(trimmedUrl, columnsToRender);
      if (data) {
        const columns = columnsToRender;
        const rows = toRows(data, columns);
        setResult({ columns, rows });
      }
    } catch (err) {
      setError(err.message || "Failed to render");
    } finally {
      setRendering(false);
    }
  }

  const renderDisabled = rendering || !parquetUrl.trim() || (totalCount > 0 && selectedCount === 0);

  return (
    <div className="app-shell">
      <header className="top-panel">
        <h1>Parquet URL Viewer</h1>

        <div className="form-row">
          <input
            type="text"
            placeholder="Paste parquet URL"
            value={parquetUrl}
            onChange={(e) => {
              setParquetUrl(e.target.value);
              setSchemaColumns([]);
              setDraftSelectedColumns([]);
              setResult(null);
              setError("");
              loadedSchemaUrlRef.current = "";
            }}
          />
          <button type="button" className="render-button" onClick={handleRenderClick} disabled={renderDisabled}>
            Render
          </button>
          {rendering && (
            <span className="loading-indicator">Rendering...</span>
          )}
        </div>

        <div className="columns-row" ref={dropdownRef}>
          <button
            type="button"
            className="columns-trigger"
            onClick={() => setDropdownOpen((open) => !open)}
            disabled={totalCount === 0 || rendering}
          >
            Columns ({selectedCount}/{totalCount})
          </button>

          {dropdownOpen && (
            <div className="columns-dropdown">
              <div className="columns-list">
                {schemaColumns.map((column) => {
                  const selected = draftSelectedColumns.includes(column.name);

                  return (
                    <div key={column.name} className={`columns-item${selected ? " is-selected" : ""}`}>
                      <input
                        type="checkbox"
                        checked={selected}
                        onChange={() => toggleColumn(column.name)}
                        aria-label={`Select column ${column.name}`}
                      />
                      <span className="columns-name">{column.name}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>

        {error && <div className="error">{error}</div>}
        {result && <div className="meta">Showing {result.rows.length} rows, {result.columns.length} columns</div>}
      </header>

      <section className="table-panel">
        {result ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  {result.columns.map((col) => (
                    <th key={col} className="column-header-cell">{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.rows.map((row, idx) => (
                  <tr key={idx}>
                    {result.columns.map((col) => (
                      <td key={`${idx}-${col}`}>{String(row[col] ?? "")}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="empty-state">
            {parquetUrl.trim()
              ? totalCount > 0
                ? "Choose columns, then click Render."
                : "Click Render to load schema and view data."
              : "Paste a parquet URL to view the data."}
          </div>
        )}
      </section>
    </div>
  );
}

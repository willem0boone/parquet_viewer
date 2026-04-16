import React, { useEffect, useMemo, useRef, useState } from "react";

const API_BASE = "http://127.0.0.1:8000";
const DEFAULT_URL = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology/12639/marine_biodiversity_observations_2026-02-26.parquet";

export default function App() {
  const [parquetUrl, setParquetUrl] = useState(DEFAULT_URL);
  const [loading, setLoading] = useState(false);
  const [schemaLoading, setSchemaLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);
  const [schemaColumns, setSchemaColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [schemaUrl, setSchemaUrl] = useState("");
  const [dropdownOpen, setDropdownOpen] = useState(false);

  const dropdownRef = useRef(null);

  const allColumnNames = useMemo(
    () => schemaColumns.map((item) => item.name),
    [schemaColumns]
  );

  useEffect(() => {
    function onClickOutside(event) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setDropdownOpen(false);
      }
    }

    document.addEventListener("mousedown", onClickOutside);
    return () => document.removeEventListener("mousedown", onClickOutside);
  }, []);

  async function loadSchema(url, forceReload = false) {
    const trimmedUrl = url.trim();
    if (!trimmedUrl) {
      throw new Error("Please provide a parquet URL");
    }

    if (!forceReload && schemaUrl === trimmedUrl && schemaColumns.length > 0) {
      return {
        columns: schemaColumns,
        selected: selectedColumns,
      };
    }

    setSchemaLoading(true);

    try {
      const response = await fetch(`${API_BASE}/schema`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ parquet_url: trimmedUrl })
      });

      const body = await response.json();
      if (!response.ok) {
        throw new Error(body.detail || "Failed to load schema");
      }

      const columns = Array.isArray(body.columns) ? body.columns : [];
      const names = columns.map((item) => item.name);

      setSchemaColumns(columns);
      setSchemaUrl(trimmedUrl);
      setSelectedColumns(names);

      return {
        columns,
        selected: names,
      };
    } finally {
      setSchemaLoading(false);
    }
  }

  async function handleSubmit(event) {
    event.preventDefault();
    setLoading(true);
    setError("");

    try {
      const schema = await loadSchema(parquetUrl);
      const selected =
        schemaUrl === parquetUrl.trim() && schemaColumns.length > 0
          ? selectedColumns
          : schema.selected;

      if (selected.length === 0) {
        throw new Error("Select at least one column");
      }

      const response = await fetch(`${API_BASE}/view`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          parquet_url: parquetUrl,
          max_rows: 50,
          columns: selected
        })
      });

      const body = await response.json();
      if (!response.ok) {
        setResult(null);
        setError(body.detail || "Failed to load view");
        return;
      }

      setResult(body);
    } catch (err) {
      setResult(null);
      setError(err.message || "Unexpected error");
    } finally {
      setLoading(false);
    }
  }

  function toggleColumn(name) {
    setSelectedColumns((prev) =>
      prev.includes(name) ? prev.filter((item) => item !== name) : [...prev, name]
    );
  }

  function selectAllColumns() {
    setSelectedColumns(allColumnNames);
  }

  function clearColumns() {
    setSelectedColumns([]);
  }

  const selectedCount = selectedColumns.length;
  const totalCount = allColumnNames.length;

  return (
    <div className="app-shell">
      <header className="top-panel">
        <h1>Parquet URL Viewer</h1>
        <form onSubmit={handleSubmit}>
          <div className="form-row">
            <input
              type="text"
              placeholder="Paste parquet URL"
              value={parquetUrl}
              onChange={(e) => setParquetUrl(e.target.value)}
            />
            <button
              type="button"
              onClick={() => loadSchema(parquetUrl, true).catch((err) => setError(err.message || "Failed to load schema"))}
              disabled={schemaLoading || loading || !parquetUrl.trim()}
            >
              {schemaLoading ? "Loading schema..." : "Load columns"}
            </button>
            <button
              disabled={loading || schemaLoading || !parquetUrl.trim()}
              type="submit"
            >
              {loading ? "Loading..." : "Render"}
            </button>
          </div>

          <div className="columns-row" ref={dropdownRef}>
            <button
              type="button"
              className="columns-trigger"
              onClick={() => setDropdownOpen((open) => !open)}
              disabled={totalCount === 0}
            >
              Columns ({selectedCount}/{totalCount})
            </button>

            {dropdownOpen && (
              <div className="columns-dropdown">
                <div className="columns-actions">
                  <button type="button" onClick={selectAllColumns}>All</button>
                  <button type="button" onClick={clearColumns}>None</button>
                </div>
                <div className="columns-list">
                  {schemaColumns.map((column) => (
                    <label key={column.name} className="columns-item">
                      <input
                        type="checkbox"
                        checked={selectedColumns.includes(column.name)}
                        onChange={() => toggleColumn(column.name)}
                      />
                      <span className="columns-name">{column.name}</span>
                      <span className="columns-type">{column.dtype}</span>
                    </label>
                  ))}
                </div>
              </div>
            )}
          </div>
        </form>

        {error && <div className="error">{error}</div>}
        {result && (
          <div className="meta">
            Showing {result.displayed_rows} rows, {result.columns.length} columns
          </div>
        )}
      </header>

      <section className="table-panel">
        {result ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  {result.columns.map((col) => (
                    <th key={col}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.data.map((row, idx) => (
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
          <div className="empty-state">Load columns and click Render to see the data view.</div>
        )}
      </section>
    </div>
  );
}

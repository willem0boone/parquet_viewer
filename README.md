# Minimal Parquet URL Viewer

This project includes a tiny web UI:
- Paste a parquet URL
- Click **Render**
- See the first rows in a table

## Fastest start (Windows, Linux, PyCharm terminal)

Run both backend + frontend from one command:

```powershell
python dev.py
```

Press `Ctrl+C` to stop both processes.

## Bash start (Git Bash / Linux)

```bash
bash ./dev.sh
```

## Manual start

### 1) Start the API

From the project root:

```powershell
pip install -r requirements.txt
python -m uvicorn parquet_viewer.api:app --host 127.0.0.1 --port 8000 --reload
```

### 2) Start the frontend

In a second terminal:

```powershell
Set-Location frontend
npm install
npm run dev
```

Open the printed local URL (usually `http://localhost:5173`).

## API endpoint

- `POST /view`
- Body:

```json
{
  "parquet_url": "https://.../file.parquet",
  "max_rows": 50,
  "columns": ["datasetid", "eventdate"]
}
```

The endpoint returns `columns` and `data` from `ParquetViewService.get_view`.

- `POST /schema`
- Body:

```json
{
  "parquet_url": "https://.../file.parquet"
}
```

Returns available parquet columns (`name`, `dtype`) for the column picker.


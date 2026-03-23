# Watlow Knowledge Ingestion

A Databricks App that ingests documents (PDFs, images, audio) and extracts their content using AI. Built with FastAPI + React.

## What It Does

- Upload PDFs, images (PNG/JPG/TIFF/BMP), and MP3 audio files through a web portal
- PDFs and images are parsed via `ai_parse_document` on a SQL warehouse
- Audio files are transcribed via the Whisper Large V3 model on a serving endpoint
- All files are stored in a Unity Catalog Volume with parsed content in a Delta table

## Architecture

```
React Frontend
  → FastAPI Backend (Databricks App)
      → UC Volume (file storage)
      → SQL Warehouse (ai_parse_document for PDFs/images)
      → Model Serving Endpoint (Whisper V3 for audio transcription)
      → Delta Table (parsed content + metadata)
```

## Project Structure

```
├── app.yaml                 # Databricks App config (env vars, resources)
├── requirements.txt         # Python dependencies
├── backend/
│   └── main.py              # FastAPI app — upload, parse, transcribe, CRUD
├── frontend/
│   └── build/               # React frontend (pre-built static files)
├── deploy_whisper.py         # Notebook: deploy Whisper V3 from system.ai
└── keepalive.py              # Notebook: ping whisper endpoint to prevent scale-to-zero
```

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Warehouse
- `system.ai.whisper_large_v3` model available in Unity Catalog

## Setup

### 1. Deploy the Whisper endpoint

Run `deploy_whisper.py` as a notebook or job. This creates a `whisper-transcriber` model serving endpoint using the Whisper Large V3 model from `system.ai`.

### 2. Configure app.yaml

Update the environment variables in `app.yaml`:

| Variable | Description |
|----------|-------------|
| `CATALOG` | Unity Catalog catalog name |
| `SCHEMA` | Schema for tables and volumes |
| `VOLUME` | Volume name for raw document storage |
| `PARSED_TABLE` | Delta table name for parsed content |
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse ID (via `sql-warehouse` resource) |

### 3. Deploy the app

```bash
databricks apps create --name watlow-knowledge-ingestion
databricks apps deploy watlow-knowledge-ingestion --source-code-path /Workspace/path/to/this/project
```

### 4. (Optional) Schedule keepalive

Run `keepalive.py` on a schedule (e.g., every 30 minutes during business hours) to keep the Whisper endpoint warm and avoid cold-start latency.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/health` | Health check |
| `POST` | `/api/upload` | Upload and parse a file |
| `GET` | `/api/documents` | List all documents |
| `GET` | `/api/documents/{id}` | Get document details + parsed content |
| `DELETE` | `/api/documents/{id}` | Delete a document |

# CLAUDE.md

## Project Overview

Watlow Knowledge Ingestion — a Databricks App (FastAPI + React) that ingests PDFs, images, and MP3 audio files, parses/transcribes them with AI, and stores results in a Delta table.

## Architecture

- **Frontend:** Pre-built React app served as static files from `frontend/build/`
- **Backend:** FastAPI app in `backend/main.py` — handles uploads, parsing, transcription, CRUD
- **Parsing:** PDFs/images use `ai_parse_document` via SQL warehouse; MP3s use Whisper V3 endpoint directly via Databricks SDK
- **Storage:** Files in UC Volume, metadata + parsed content in Delta table
- **Whisper endpoint:** `whisper-transcriber` serving `system.ai.whisper_large_v3` (installed from [Databricks Marketplace](https://marketplace.databricks.com/details/1eceaa77-6b60-42f0-9809-ceccf1b237f5/Databricks_Whisper-V3-Model))

## Key Files

| File | Purpose |
|------|---------|
| `app.yaml` | Databricks App config — env vars, command, resources |
| `backend/main.py` | FastAPI app — all API endpoints |
| `deploy_whisper.py` | Notebook: deploys Whisper V3 from system.ai to a serving endpoint |
| `batch_transcribe.py` | Notebook: offline batch transcription of unprocessed MP3s via ai_query |
| `keepalive.py` | Notebook: pings whisper endpoint to prevent scale-to-zero |
| `requirements.txt` | Python dependencies for the app |

## Deployment

- **Workspace:** `fevm-uplight-demo-gen.cloud.databricks.com`
- **App name:** `watlow-knowledge-ingestion`
- **Deployed from:** `/Workspace/Shared/apps/watlow-knowledge-ingestion/`
- **Source also at:** `/Workspace/Users/megan.fang@databricks.com/watlow-knowledge-gemini/`
- **GitHub:** `mfang73/multimodal-transcription-app` (private, personal account — requires `gh auth login` as `mfang73` to push)

## Environment Variables (app.yaml)

| Variable | Default | Description |
|----------|---------|-------------|
| `CATALOG` | `uplight_demo_gen_catalog` | Unity Catalog catalog |
| `SCHEMA` | `watlow_ingestion` | Schema for tables and volumes |
| `VOLUME` | `raw_documents` | Volume for file storage |
| `PARSED_TABLE` | `parsed_documents_gemini` | Delta table for parsed content |
| `DATABRICKS_WAREHOUSE_ID` | via `sql-warehouse` resource | SQL warehouse for ai_parse_document |
| `WHISPER_ENDPOINT` | `whisper-transcriber` | Model serving endpoint for audio transcription |

## Development Notes

- `deploy_whisper.py` uses `# MAGIC` prefixes for `%pip` and `dbutils` lines so it works both interactively and as a job
- The app routes MP3 transcription directly to the serving endpoint (SDK call with base64 audio bytes), not through ai_query — this avoids SQL warehouse overhead for single-file uploads
- `batch_transcribe.py` uses `ai_query` with `failOnError => FALSE` for bulk offline processing — individual failures don't stop the batch
- There are two parsed tables: `parsed_documents` (legacy) and `parsed_documents_gemini` (current) — the app uses `parsed_documents_gemini`

## Syncing Code

When making changes in the Databricks workspace, pull them down and push to GitHub:

```bash
# Export from workspace
databricks workspace export /Workspace/Users/megan.fang@databricks.com/watlow-knowledge-gemini/backend/main.py --format AUTO --file backend/main.py

# Push to GitHub (must be authed as mfang73)
gh auth status  # verify mfang73
git add -A && git commit -m "description" && git push
```

When making changes locally, upload to workspace:

```bash
databricks workspace import /Workspace/Shared/apps/watlow-knowledge-ingestion/backend/main.py --file backend/main.py --format AUTO --overwrite
databricks apps deploy watlow-knowledge-ingestion --source-code-path /Workspace/Shared/apps/watlow-knowledge-ingestion
```

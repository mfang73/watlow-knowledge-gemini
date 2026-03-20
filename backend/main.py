import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Read config once at module level
CATALOG = os.getenv("CATALOG", "watlow_knowledge")
SCHEMA = os.getenv("SCHEMA", "ingestion")
VOLUME = os.getenv("VOLUME", "raw_documents")
PARSED_TABLE = os.getenv("PARSED_TABLE", "parsed_documents")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

ALLOWED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".mp3"}
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200MB

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.{PARSED_TABLE}"

w = WorkspaceClient()


def _exec(statement: str, wait_timeout: str = "30s"):
    """Execute a SQL statement on the configured warehouse."""
    return w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout=wait_timeout,
    )


def _transcribe_audio_async(volume_path: str, document_id: str):
    """Submit async ai_query transcription job via whisper-transcriber and poll for result."""
    statement = f"""
        SELECT ai_query(
            'whisper-transcriber',
            content
        ) AS transcript
        FROM read_files('{volume_path}', format => 'binaryFile')
    """
    try:
        result = _exec(statement, wait_timeout="0s")
        statement_id = result.statement_id
        _exec(f"""
            UPDATE {TABLE_NAME}
            SET parse_metadata = '{statement_id}'
            WHERE document_id = '{document_id}'
        """)
        threading.Thread(
            target=_poll_and_update,
            args=(statement_id, document_id),
            daemon=True,
        ).start()
    except Exception as e:
        _update_parse_status(document_id, "error", "", str(e))


@asynccontextmanager
async def lifespan(app):
    """Ensure catalog, schema, volume, and table exist on startup."""
    try:
        _exec(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
        _exec(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
        _exec(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
        _exec(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                document_id STRING,
                filename STRING,
                file_type STRING,
                upload_timestamp TIMESTAMP,
                volume_path STRING,
                parsed_content STRING,
                parse_status STRING,
                parse_metadata STRING
            )
        """)
    except Exception as e:
        print(f"Warning: Could not initialize resources: {e}")
    yield


app = FastAPI(title="Watlow Knowledge Ingestion (Gemini)", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload a file, store in UC Volume, parse with AI, and save results."""
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File type {ext} not supported. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        )

    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File exceeds 200MB limit")

    document_id = str(uuid.uuid4())
    safe_filename = f"{document_id}{ext}"
    volume_path = f"{VOLUME_PATH}/{safe_filename}"

    # Upload to UC Volume
    try:
        w.files.upload(volume_path, content, overwrite=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {e}")

    # Insert record as "processing" immediately
    try:
        escaped_filename = file.filename.replace("'", "''")
        _exec(f"""
            INSERT INTO {TABLE_NAME} VALUES (
                '{document_id}',
                '{escaped_filename}',
                '{ext}',
                current_timestamp(),
                '{volume_path}',
                '',
                'processing',
                ''
            )
        """)
    except Exception as e:
        print(f"Warning: Failed to insert record: {e}")

    if ext == ".mp3":
        # MP3: transcribe via whisper-transcriber using async ai_query SQL
        _transcribe_audio_async(volume_path, document_id)
    else:
        # PDF/images: parse via ai_parse_document SQL
        statement = f"""
            SELECT concat_ws('\\n\\n',
                transform(
                    try_cast(ai_parse_document(content):document:elements AS ARRAY<VARIANT>),
                    element -> try_cast(element:content AS STRING)
                )
            ) AS parsed
            FROM read_files('{volume_path}', format => 'binaryFile')
        """
        try:
            result = _exec(statement, wait_timeout="0s")
            statement_id = result.statement_id

            _exec(f"""
                UPDATE {TABLE_NAME}
                SET parse_metadata = '{statement_id}'
                WHERE document_id = '{document_id}'
            """)

            threading.Thread(
                target=_poll_and_update,
                args=(statement_id, document_id),
                daemon=True,
            ).start()

        except Exception as e:
            _update_parse_status(document_id, "error", "", str(e))

    return {
        "document_id": document_id,
        "filename": file.filename,
        "file_type": ext,
        "volume_path": volume_path,
        "parse_status": "processing",
        "content_preview": None,
    }



def _poll_and_update(statement_id: str, document_id: str):
    """Background thread to poll for parse completion and update the record."""
    for _ in range(60):
        time.sleep(5)
        try:
            status = w.statement_execution.get_statement(statement_id)
            state = status.status.state.value if status.status.state else ""
            if state == "SUCCEEDED":
                content = ""
                if status.result and status.result.data_array:
                    content = status.result.data_array[0][0] or ""
                _update_parse_status(document_id, "completed", content, "")
                return
            elif state in ("FAILED", "CANCELED", "CLOSED"):
                err = status.status.error.message if status.status.error else "Query failed"
                _update_parse_status(document_id, "error", "", err)
                return
        except Exception as e:
            print(f"Poll error for {document_id}: {e}")
    _update_parse_status(document_id, "error", "", "Parsing timed out after 5 minutes")


def _update_parse_status(document_id: str, status: str, content: str, metadata: str):
    """Update a document's parse status in the table."""
    try:
        escaped_content = content.replace("'", "''").replace("\\", "\\\\")
        escaped_metadata = metadata.replace("'", "''").replace("\\", "\\\\")
        _exec(f"""
            UPDATE {TABLE_NAME}
            SET parsed_content = '{escaped_content}',
                parse_status = '{status}',
                parse_metadata = '{escaped_metadata}'
            WHERE document_id = '{document_id}'
        """)
    except Exception as e:
        print(f"Failed to update status for {document_id}: {e}")


@app.get("/api/documents")
async def list_documents(limit: int = 50):
    """List all uploaded and parsed documents."""
    try:
        result = _exec(f"""
            SELECT document_id, filename, file_type, upload_timestamp,
                   parse_status, LENGTH(parsed_content) as content_length
            FROM {TABLE_NAME}
            ORDER BY upload_timestamp DESC
            LIMIT {limit}
        """)
        documents = []
        if result.result and result.result.data_array:
            for row in result.result.data_array:
                documents.append({
                    "document_id": row[0],
                    "filename": row[1],
                    "file_type": row[2],
                    "upload_timestamp": row[3],
                    "parse_status": row[4],
                    "content_length": row[5],
                })
        return {"documents": documents, "total": len(documents)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list documents: {e}")


@app.get("/api/documents/{document_id}")
async def get_document(document_id: str):
    """Get parsed content for a specific document."""
    try:
        uuid.UUID(document_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid document ID")

    try:
        result = _exec(f"""
            SELECT document_id, filename, file_type, upload_timestamp,
                   volume_path, parsed_content, parse_status, parse_metadata
            FROM {TABLE_NAME}
            WHERE document_id = '{document_id}'
        """)
        if result.result and result.result.data_array:
            row = result.result.data_array[0]
            return {
                "document_id": row[0],
                "filename": row[1],
                "file_type": row[2],
                "upload_timestamp": row[3],
                "volume_path": row[4],
                "parsed_content": row[5],
                "parse_status": row[6],
                "parse_metadata": row[7],
            }
        raise HTTPException(status_code=404, detail="Document not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get document: {e}")


@app.delete("/api/documents/{document_id}")
async def delete_document(document_id: str):
    """Delete a document and its parsed content."""
    try:
        uuid.UUID(document_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid document ID")

    try:
        result = _exec(
            f"SELECT volume_path FROM {TABLE_NAME} WHERE document_id = '{document_id}'"
        )
        if result.result and result.result.data_array:
            vpath = result.result.data_array[0][0]
            try:
                w.files.delete(vpath)
            except Exception:
                pass

        _exec(f"DELETE FROM {TABLE_NAME} WHERE document_id = '{document_id}'")
        return {"status": "deleted", "document_id": document_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete document: {e}")


# Serve React frontend
frontend_dir = Path(__file__).parent.parent / "frontend" / "build"
if frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")

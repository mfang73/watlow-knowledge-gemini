# Databricks notebook source
# Batch transcribe MP3 files in the UC Volume that haven't been processed yet.
# Schedule this as a job to catch bulk uploads or reprocess failed transcriptions.

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Configuration — override via job parameters or widgets
CATALOG = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.getAll()] else "uplight_demo_gen_catalog"
SCHEMA = dbutils.widgets.get("schema") if "schema" in [w.name for w in dbutils.widgets.getAll()] else "watlow_ingestion"
VOLUME = dbutils.widgets.get("volume") if "volume" in [w.name for w in dbutils.widgets.getAll()] else "raw_documents"
PARSED_TABLE = dbutils.widgets.get("parsed_table") if "parsed_table" in [w.name for w in dbutils.widgets.getAll()] else "parsed_documents_gemini"
WHISPER_ENDPOINT = dbutils.widgets.get("whisper_endpoint") if "whisper_endpoint" in [w.name for w in dbutils.widgets.getAll()] else "whisper-transcriber"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.{PARSED_TABLE}"

print(f"Volume: {VOLUME_PATH}")
print(f"Table: {TABLE_NAME}")
print(f"Endpoint: {WHISPER_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find unprocessed MP3 files
# MAGIC Compare files in the volume against records in the parsed table to find
# MAGIC MP3s that are new, failed, or still stuck in processing.

# COMMAND ----------

# Get all MP3 files in the volume
all_files_df = spark.sql(f"""
  SELECT path AS volume_path
  FROM list_files('{VOLUME_PATH}')
  WHERE lower(path) LIKE '%.mp3'
""")

# Get already-completed MP3s from the table
completed_df = spark.sql(f"""
  SELECT volume_path
  FROM {TABLE_NAME}
  WHERE file_type = '.mp3'
    AND parse_status = 'completed'
    AND parsed_content IS NOT NULL
    AND parsed_content != ''
""")

# Find MP3s that need transcription (new, failed, or stuck)
unprocessed_df = all_files_df.join(completed_df, on="volume_path", how="left_anti")
unprocessed_count = unprocessed_df.count()
print(f"Found {unprocessed_count} MP3 files to transcribe")

if unprocessed_count == 0:
    dbutils.notebook.exit("No files to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch transcribe using ai_query

# COMMAND ----------

# Transcribe all unprocessed MP3s via ai_query on the SQL warehouse
unprocessed_df.createOrReplaceTempView("unprocessed_mp3s")

transcribed_df = spark.sql(f"""
  SELECT
    u.volume_path,
    ai_query(
      '{WHISPER_ENDPOINT}',
      f.content,
      'returnType', 'STRING'
    ) AS transcript
  FROM unprocessed_mp3s u
  JOIN read_files('{VOLUME_PATH}/*.mp3', format => 'binaryFile') f
    ON f.path = u.volume_path
""")

transcribed_count = transcribed_df.count()
print(f"Transcribed {transcribed_count} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert results into the parsed table

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, regexp_extract
import uuid

# Prepare records for upsert
results_df = transcribed_df.select(
    regexp_extract(col("volume_path"), r"/([^/]+)\.mp3$", 1).alias("document_id"),
    regexp_extract(col("volume_path"), r"/([^/]+\.mp3)$", 1).alias("filename"),
    lit(".mp3").alias("file_type"),
    current_timestamp().alias("upload_timestamp"),
    col("volume_path"),
    col("transcript").alias("parsed_content"),
    lit("completed").alias("parse_status"),
    lit("batch_transcribe").alias("parse_metadata"),
)

results_df.createOrReplaceTempView("batch_results")

# Merge — update existing failed/processing records, insert new ones
spark.sql(f"""
  MERGE INTO {TABLE_NAME} t
  USING batch_results s
  ON t.volume_path = s.volume_path
  WHEN MATCHED AND t.parse_status != 'completed' THEN
    UPDATE SET
      t.parsed_content = s.parsed_content,
      t.parse_status = s.parse_status,
      t.parse_metadata = s.parse_metadata
  WHEN NOT MATCHED THEN
    INSERT (document_id, filename, file_type, upload_timestamp, volume_path, parsed_content, parse_status, parse_metadata)
    VALUES (s.document_id, s.filename, s.file_type, s.upload_timestamp, s.volume_path, s.parsed_content, s.parse_status, s.parse_metadata)
""")

print(f"Batch transcription complete. Processed {transcribed_count} files.")

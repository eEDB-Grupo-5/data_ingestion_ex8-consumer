# ex8-consumer

This repository contains a PySpark-based streaming consumer that reads Avro-encoded messages from Kafka, enriches and aggregates the data using reference tables from PostgreSQL, writes cleaned data back to PostgreSQL, and exports aggregated results to S3 as Parquet files.

The project is structured for local development and can be run inside Docker via the provided `docker-compose.yml`.

## Contents

- `src/ex8_consumer/app.py` — main application. Sets up Spark session, reads from Kafka, deserializes Avro, performs transformations and aggregations, writes streaming results to PostgreSQL and S3.
- `src/ex8_consumer/settings.py` — configuration module that reads environment variables used by the app.
- `src/ex8_consumer/schemas/reclamacoes.avsc` — Avro schema used to deserialize Kafka messages.
- `docker-compose.yml` — docker-compose file for running the consumer container. Uses `env_file: .env` to load environment variables.
- `.env` — environment variables (AWS, Postgres). Not tracked by Git in many setups — used here for local development.

## Design overview

The consumer implements a Spark Structured Streaming pipeline:

- A Kafka streaming source reads Avro-encoded messages from a configured topic.
- Messages are deserialized using an Avro schema loaded from `src/ex8_consumer/schemas/reclamacoes.avsc`.
- The pipeline writes cleaned records to a Postgres table `trusted.reclamacoes_cleaned` using `foreachBatch` on a micro-batch basis.
- A subsequent SQL aggregation joins the cleaned stream with static JDBC tables (`trusted.empregados` and `trusted.bancos`) and produces `result_df`.
- `result_df` is written using `foreachBatch` to both Postgres and S3 (as Parquet) with checkpointing for fault tolerance.

## Modules and key functions

The repository currently contains the following Python modules; the documentation below documents functions, classes and behavior inferred from the source files.

### src/ex8_consumer/settings.py

This module centralizes configuration by reading environment variables at import time. Variables available:

- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`, `DB_NAME` — used to construct JDBC connection URLs and credentials.
- `AWS_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_BUCKET_NAME`, `AWS_ENDPOINT_URL` — used to configure S3 access (including LocalStack compatibility).
- `SCHEMA_PATH` — path to the Avro schema file used by the application.

Notes:
- The module reads values directly from `os.getenv`. For local development, the project's `docker-compose.yml` sets `env_file: .env` so these come from `.env`.
- If any required env var is missing, behavior is to propagate `None` which may later raise errors in the app — consider validating required settings at startup.

### src/ex8_consumer/app.py

High-level responsibilities:

- Start a SparkSession configured with Kafka, Avro, JDBC and Hadoop AWS dependencies via `spark.jars.packages`.
- Load the Avro schema used to deserialize Kafka message payloads. The helper `_get_schema_str()` reads `settings.SCHEMA_PATH` and returns the schema as a string; it logs and raises if not found.
- Construct JDBC connection URL and properties from `settings`.
- Ensure the Postgres table `trusted.reclamacoes_cleaned` exists by writing an empty DataFrame with the expected schema and `mode='ignore'` (this leverages Spark JDBC's behavior to create the table when possible).
- Define a streaming Kafka source using `spark.readStream.format('kafka')` and the configured bootstrap servers and topic.
- Deserialize Avro payloads to a DataFrame using `from_avro(col('value'), AVRO_SCHEMA_STR)`.
- Register the cleaned stream as a temporary view `reclamacoes_cleaned_stream` and write it to Postgres using `foreachBatch` with `.outputMode('update')`.
- Build a SQL query that aggregates cleaned records and joins them with static reference tables (`empregados` and `bancos`) loaded over JDBC.
- For `result_df`, configure Hadoop's S3A settings from `settings` and use a `foreachBatch` writer that:
  - Writes micro-batches to Postgres (`trusted.reclamacoes_cleaned`) using JDBC. If append fails (commonly because the table does not exist), it attempts to create the table by writing a zero-row DataFrame with `mode='ignore'` and then retries the append.
  - Writes micro-batches as Parquet files to S3 using the `s3a://` scheme. The code writes to `s3a://{S3_BUCKET_NAME}/reclamacoes/epoch={epoch_id}` by default.
  - Uses `checkpointLocation` to enable stateful recovery.

Important implementation notes and gotchas:

- The application uses `outputMode('complete')` for `result_df`. This mode emits the full result on each trigger and is appropriate for aggregations, but may be expensive for large results.
- The S3A configuration in the code sets `fs.s3a.path.style.access=true` and disables SSL (`fs.s3a.connection.ssl.enabled=false`) which is convenient for LocalStack but should be adjusted for real AWS S3.
- Creating tables via Spark JDBC depends on the permissions of the provided DB user. If the user lacks CREATE privileges, explicit DDL via a database client (psycopg2) would be necessary.
- The Kafka bootstrap server is currently hard-coded to `broker:29092` in the code; change this to `KAFKA_BOOTSTRAP_SERVERS` from environment if you'd like runtime configurability.

## Configuration

Add a `.env` file (example provided in repository) with the following variables:

- AWS_ENDPOINT_URL (e.g., http://localhost:4566 for LocalStack)
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_DEFAULT_REGION
- S3_BUCKET_NAME
- POSTGRES_USER
- POSTGRES_PASSWORD
- POSTGRES_HOST
- POSTGRES_PORT
- DB_NAME

The project's `docker-compose.yml` already references `.env` via `env_file:` so these variables are loaded into the container at runtime.

## Running locally with Docker

1. Ensure your dependent services are available (Kafka, Postgres, S3/LocalStack). For development you can run them using docker-compose or separate containers.
2. Set environment variables in `.env` (or export them in your shell).
3. Start the service:

```bash
docker compose up --build
```

Notes:
- If Kafka runs on a different host or under a different service name, update `KAFKA_BOOTSTRAP_SERVERS` in `docker-compose.yml` or update the hard-coded value in `app.py`.
- For real S3, remove or adapt the S3A endpoint and SSL settings.
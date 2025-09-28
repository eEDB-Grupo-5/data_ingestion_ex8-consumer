# Project Documentation

## Project Overview

This project is a consumer application that processes messages from an AWS SQS queue, saves the data to a PostgreSQL database, and periodically exports data from the database to AWS S3 in Parquet format.

## Architecture

The application is composed of the following components:

- **SQS Consumer**: Polls an SQS queue for messages.
- **Avro Deserializer**: Deserializes the message body from Avro format.
- **Database Writer**: Saves the deserialized data to a PostgreSQL database.
- **Data Exporter**: Periodically queries the database and exports the data to S3 in Parquet format.

The application is asynchronous and uses `asyncio` for concurrency.

## Modules

### `app.py`

This is the main application module. It initializes the database, gets the SQS client, and starts the main loop. The main loop polls SQS for messages, processes them concurrently, and runs the S3 export process as a background task.

### `settings.py`

This module manages the application's configuration. It loads environment variables from a `.env` file and defines settings for the database, AWS, SQS, and S3.

### `database.py`

This module handles all database interactions. It uses `tortoise-orm` to connect to the PostgreSQL database, initialize the schema, and save events. It also provides a function to get a database connection.

### `sqs.py`

This module provides functions to interact with AWS SQS. It includes functions to get an SQS client, receive messages from a queue, and delete messages from a queue.

### `export.py`

This module contains the logic for exporting data from PostgreSQL to S3. It runs as a background task and exports data every minute. It queries the database, converts the data to a Pandas DataFrame, and then to a Parquet file. The Parquet file is then uploaded to S3.

### `s3.py`

This module provides functions to interact with AWS S3. It includes functions to get an S3 client and upload files to an S3 bucket.

### `models.py`

This module defines the database models using `tortoise-orm`.

## Setup and Running Locally

1.  **Install dependencies**:

    ```bash
    poetry install
    ```

2.  **Set up the environment**:

    Create a `.env` file in the root of the project with the following variables:

    ```
    POSTGRES_USER=<your_postgres_user>
    POSTGRES_PASSWORD=<your_postgres_password>
    POSTGRES_HOST=<your_postgres_host>
    DB_NAME=<your_database_name>
    AWS_DEFAULT_REGION=<your_aws_region>
    AWS_ACCESS_KEY_ID=<your_aws_access_key_id>
    AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key>
    SQS_QUEUE_URL=<your_sqs_queue_url>
    S3_BUCKET_NAME=<your_s3_bucket_name>
    ```

3.  **Run the application**:

    ```bash
    python src/ex7_consumer/app.py
    ```

## Running with Docker

1.  **Build the Docker image**:

    ```bash
    docker build -t ex7-consumer .
    ```

2.  **Run the Docker container**:

    You can run the container with the environment variables in the `.env` file:

    ```bash
    docker run --env-file .env ex7-consumer
    ```

## Configuration

-   `POSTGRES_USER`: The username for the PostgreSQL database.
-   `POSTGRES_PASSWORD`: The password for the PostgreSQL database.
-   `POSTGRES_HOST`: The host of the PostgreSQL database.
-   `DB_NAME`: The name of the database.
-   `AWS_DEFAULT_REGION`: The AWS region for SQS and S3.
-   `AWS_ACCESS_KEY_ID`: The AWS access key ID.
-   `AWS_SECRET_ACCESS_KEY`: The AWS secret access key.
-   `SQS_QUEUE_URL`: The URL of the SQS queue.
-   `S3_BUCKET_NAME`: The name of the S3 bucket for exports.
-   `SCHEMA_PATH`: The path to the Avro schema file.

## SQL Query (`delivery.sql`)

The `delivery.sql` file contains a complex query that joins data from `reclamacoes_sqs_events`, `empregados`, and `bancos` tables. It performs several data cleaning and transformation steps, including:

-   Extracting and cleaning data from the JSON body of the SQS messages.
-   Deduplicating records based on `md5_body`.
-   Normalizing `cnpj` and `instituicao_financeira` fields.
-   Aggregating data by year, quarter, category, type, CNPJ, and financial institution.
-   Joining with employee and bank data.

The final result is a detailed report of financial complaints and related company information.
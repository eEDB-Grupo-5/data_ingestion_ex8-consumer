import asyncio
import logging
import uuid
import io

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tortoise import Tortoise

from ex7_consumer import settings
from ex7_consumer.database import get_db_connection
from ex7_consumer.s3 import get_s3_client, upload_file

logger = logging.getLogger(__name__)


async def export_to_s3():
    """Exports data from PostgreSQL to S3 in Parquet format."""
    while True:
        await asyncio.sleep(60)
        logger.info("Starting export process...")
        try:
            conn = get_db_connection()
            with open("src/ex7_consumer/models/delivery.sql", "r") as f:
                query = f.read()
            df = await conn.execute_query_dict(query)
            if not df:
                logger.info("No data to export.")
                continue

            s3_client = get_s3_client()
            bucket_name = settings.S3_BUCKET_NAME
            file_name = await get_current_file_name(s3_client, bucket_name)

            table = pa.Table.from_pandas(pd.DataFrame(df))

            with io.BytesIO() as bio:
                pq.write_table(table, bio)
                bio.seek(0)
                upload_file(s3_client, bio, bucket_name, file_name)

        except Exception as e:
            logger.error(f"Error during export: {e}")


async def get_current_file_name(s3_client, bucket_name):
    """Gets the current file name to write to, creating a new one if necessary."""
    files = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents", [])
    if not files:
        return f"export_{uuid.uuid4()}.parquet"

    latest_file = max(files, key=lambda x: x["LastModified"])
    file_name = latest_file["Key"]

    return file_name

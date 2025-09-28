import asyncio
import base64
import io
import logging
from typing import Any, Dict

from avro.io import DatumReader, BinaryDecoder
from avro import schema
from avro.io import DatumReader, BinaryDecoder
from tortoise import run_async
from ex7_consumer import settings
from ex7_consumer.database import init_db, save_event
from ex7_consumer.sqs import get_sqs_client, receive_messages, delete_message
from ex7_consumer.export import export_to_s3
from ex7_consumer.settings import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_parsed_schema() -> Dict[str, Any]:
    """Builds and parses the Avro schema."""
    try:
        with open(SCHEMA_PATH, "r") as f:
            parsed_schema = schema.parse(f.read()) 
        return parsed_schema
    except FileNotFoundError:
        logger.error(f"Schema file not found at: {SCHEMA_PATH}")
        raise
    except Exception as e:
        logger.error(f"Failed to parse schema: {e}")
        raise

async def process_message(sqs_client, message):
    try:
        body = base64.b64decode(message["Body"])

        with io.BytesIO(body) as bio:
            reader = DatumReader(AVRO_SCHEMA)
            decoder = BinaryDecoder(bio)
            record = reader.read(decoder)

            await save_event(
                message_id=message["MessageId"],
                receipt_handle=message["ReceiptHandle"],
                md5_body=message["MD5OfBody"],
                body=record,
            )
            logger.info(f"Saved event to database: {record}")
            await asyncio.to_thread(
                delete_message,
                sqs_client,
                settings.SQS_QUEUE_URL,
                message["ReceiptHandle"],
            )
    except Exception as e:
        logger.error(f"Error processing message: {e}")

async def main():
    await init_db()
    sqs_client = get_sqs_client()
    global AVRO_SCHEMA 
    AVRO_SCHEMA = get_parsed_schema()

    asyncio.create_task(export_to_s3())

    while True:
        logger.info("Polling SQS for messages...")
        messages = receive_messages(sqs_client, settings.SQS_QUEUE_URL)
        messages_list = messages.get("Messages")
        if not messages_list:
            continue

        tasks = [
            process_message(sqs_client, message) for message in messages_list
        ]
        # tasks.append(export_to_s3())
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    run_async(main())
from tortoise import Tortoise, connections
from ex7_consumer.models import Events
from ex7_consumer import settings

async def init_db():
    await Tortoise.init(
        db_url=f"postgres://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@{settings.POSTGRES_HOST}/{settings.DB_NAME}",
        modules={"models": ["ex7_consumer.models"]},
        use_tz=True
    )
    await Tortoise.generate_schemas()

def get_db_connection():
    return connections.get("default")

async def save_event(message_id: str, receipt_handle: str, md5_body: str, body: dict):
    await Events.create(
        message_id=message_id,
        receipt_handle=receipt_handle,
        md5_body=md5_body,
        body=body
    )

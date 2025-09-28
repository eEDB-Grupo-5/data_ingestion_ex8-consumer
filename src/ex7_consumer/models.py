from tortoise.models import Model
from tortoise import fields

class Events(Model):
    message_id = fields.CharField(max_length=100, pk=True)
    receipt_handle = fields.CharField(max_length=255)
    md5_body = fields.CharField(max_length=50)
    body = fields.JSONField()
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        schema = "events"
        table = "reclamacoes_sqs_events"

    def __str__(self):
        return str(self.event_data)
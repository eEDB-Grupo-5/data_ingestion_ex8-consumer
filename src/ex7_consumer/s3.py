import logging
import boto3
from botocore.exceptions import ClientError
from ex7_consumer import settings

logger = logging.getLogger(__name__)

def get_s3_client():
    """Get S3 client."""
    return boto3.client(
        "s3",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_DEFAULT_REGION,
    )

def upload_file(s3_client, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket."""
    if object_name is None:
        object_name = file_name
    try:
        s3_client.upload_fileobj(file_name, bucket, object_name)
        logger.info(f"File uploaded to {bucket}/{object_name}")
    except ClientError as e:
        logger.error(f"Error uploading file to S3: {e}")
        return False
    return True

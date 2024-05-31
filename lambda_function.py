import gzip
import logging
import os
import uuid
from typing import Any, Dict
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3 = boto3.resource('s3')

def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """
    AWS Lambda handler to process S3 events for gzip files.

    :param event: Event data from S3
    :param context: Runtime information
    """
    key = event['Records'][0]['s3']['object']['key']
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    logger.info('Received file: %s', key)

    if ".gz" in key:
        temp_compressed_file = f"/tmp/{uuid.uuid4()}.gz"
        temp_uncompressed_file = f"/tmp/{uuid.uuid4()}"

        try:
            # Download the gzip file from S3
            s3.Bucket(bucket_name).download_file(Key=key, Filename=temp_compressed_file)
            logger.info('Downloaded file: %s', key)

            # Decompress the gzip file
            with gzip.open(temp_compressed_file, 'rb') as compressed_file:
                with open(temp_uncompressed_file, 'wb') as uncompressed_file:
                    uncompressed_file.write(compressed_file.read())
            logger.info('Decompressed file')

            # Upload the decompressed file back to S3 with a new key
            new_key = key.replace('.gz', '')
            with open(temp_uncompressed_file, 'rb') as data:
                s3.Bucket(bucket_name).put_object(Key=new_key, Body=data)
            logger.info('Uploaded decompressed file as: %s', new_key)
        
        finally:
            # Clean up temporary files
            try:
                os.remove(temp_compressed_file)
                os.remove(temp_uncompressed_file)
            except OSError as e:
                logger.error('Error deleting temporary files: %s', e)

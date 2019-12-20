import gzip
import boto3

print('Loading function')

s3 = boto3.resource('s3')
TEMP_COMPRESSED_FILE = '/tmp/tempfile.gz'
TEMP_UNCOMPRESSED_FILE = '/tmp/tempfile'

def lambda_handler(event, context):
    key = event['Records'][0]['s3']['object']['key']
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    print(key)
    if ".gz" in key:
        s3.Bucket(bucket_name).download_file(key, TEMP_COMPRESSED_FILE)
        print("got file", key)

        compressed_file = gzip.GzipFile(TEMP_COMPRESSED_FILE, 'rb')
        data_stream = compressed_file.read()
        compressed_file.close()
        uncompressed_file = open(TEMP_UNCOMPRESSED_FILE, 'wb')
        uncompressed_file.write(data_stream)
        uncompressed_file.close()
        print("unzipped file")

        new_name = key.replace('.gz', '')
        print('new name', new_name)
        uncompressed_file = open(TEMP_UNCOMPRESSED_FILE, 'rb')
        s3.Bucket(bucket_name).put_object(Key=new_name, Body=uncompressed_file)

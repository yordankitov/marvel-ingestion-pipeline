import boto3

client = boto3.resource('s3')


def create_bucket(bucket_prefix):
    bucket_name = bucket_prefix
    bucket_response = client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-central-1'})

    return bucket_response


def upload_file(bucket_name, file_name):
    client.Object(bucket_name, file_name).upload_file(Filename=file_name)

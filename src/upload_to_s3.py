import boto3

client = boto3.resource('s3')


def create_bucket(bucket_name):
    try:
        bucket_response = client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-central-1'})
    except Exception as e:
        print(e)

    return bucket_response


def upload_file(bucket_name, file_name):
    try:
        client.Object(bucket_name, file_name).upload_file(Filename=file_name)
    except Exception as e:
        print(e)
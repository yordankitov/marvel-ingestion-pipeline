import boto3

client = boto3.client('s3')
resource = boto3.resource('s3')

def create_bucket(client, bucket_name):
    try:
        bucket_response = client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-central-1'})
    except Exception as e:
        print(e)

    return bucket_response


def upload_file(client, bucket_name, file_name):
    try:
        client.Object(bucket_name, file_name).upload_file(Filename=file_name)
    except Exception as e:
        print(e)


def check_bucket_exists_and_you_have_permission_for_access():
    response = client.head_bucket(
        Bucket='il-tapde-final-exercise-yordan')

    if response.get('ResponseMetadata').get('HTTPStatusCode') == 200:
        print('mint')
    else:
        print('oops')

# check_bucket_exists_and_you_have_permission_for_access()
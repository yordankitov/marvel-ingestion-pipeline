import boto3

client = boto3.client("s3")
resource = boto3.resource("s3")


def create_bucket(bucket_name):
    try:
        bucket_response = resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
    except Exception as e:
        print(e)

    return bucket_response


# LOCAL FILE
# def upload_file(bucket_name, file_name):
#     try:
#         resource.Object(bucket_name, file_name).upload_file(Filename=file_name)
#     except Exception as e:
#         print(e)


def check_bucket_exists_and_you_have_permission_for_access():
    try:
        response = client.head_bucket(Bucket="il-tapde-final-exercise-yordan")

        if response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            pass
        #     will have to do uploda the buckets
        else:
            print(
                "You do not have permission to access the bucket or it does not exist!"
            )
    except Exception as e:
        print(e)


# check_bucket_exists_and_you_have_permission_for_access()


def upload_file(file_name, content):
    try:
        # resource.Object(bucket_name, file_name).upload_file(Filename=file_name)
        resource.Object("il-tapde-final-exercise-yordan", file_name).put(Body=content)
    except Exception as e:
        print(e)

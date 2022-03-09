import boto3

client = boto3.client("s3")
resource = boto3.resource("s3")


def create_bucket():
    try:
        bucket_response = resource.create_bucket(
            Bucket="il-tapde-final-exercise-yordan",
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
    except Exception as e:
        print(e)

    return bucket_response


def check_bucket_exists_and_you_have_permission_for_access():
    try:
        response = client.head_bucket(Bucket="il-tapde-final-exercise-yordan")

        if response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            print("Bucket does not exist")
        else:
            print(
                "You do not have permission to access the bucket or it does not exist!"
            )
    except Exception as e:
        print(e)


def upload_file(file_name, content):
    try:
        resource.Object("il-tapde-final-exercise-yordan", file_name).put(Body=content)
    except Exception as e:
        print(e)

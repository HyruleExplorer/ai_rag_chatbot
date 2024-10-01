import boto3
import os


def load():
    try:
        # Connecting to S3 Bucket and uploading the Vector DB
        AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
        S3_BUCKET = os.environ.get("S3_BUCKET")
        FILE_NAME = "confluence_vdb"
        s3_client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url="https://s3url.com",
            verify=False,
        )
        # s3_client.upload_file('./confluence_vdb', s3_bucket_name, 'confluence_vdb')
        s3_client.download_file(S3_BUCKET, FILE_NAME, FILE_NAME)
        print(f"File {FILE_NAME} downloaded successfully!")

    except Exception as e:
        print("There was an error downloading the confluence vdb file: " + str(e))


if __name__ == "__main__":
    load()

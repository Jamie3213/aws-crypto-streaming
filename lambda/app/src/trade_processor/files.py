import boto3

from .trades import GzipCompressedTrades


class S3File:
    def __init__(self, bucket: str, key: str) -> None:
        self.bucket = bucket
        self.key = key

    def get_file(self) -> GzipCompressedTrades:
        client = boto3.client("s3")
        response = client.get_object(Bucket=self.bucket, Key=self.key)
        body = response["Body"].read()
        return GzipCompressedTrades(body)

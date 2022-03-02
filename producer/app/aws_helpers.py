import base64

import boto3


def get_secrets_manager_secret(name: str) -> str:
    """Returns the secret from AWS Secrets Manager."""
    secret_client = boto3.client("secretsmanager")
    secret_dict = secret_client.get_secret_value(SecretId=name)

    secret_string = secret_dict.get("SecretString")
    secret_binary = secret_dict.get("SecretBinary")

    return secret_string if secret_string else base64.b64decode(secret_binary)

def put_record_to_kinesis_stream(record: bytes, stream: str) -> None:
    # Writes a record to a Kinesis Data Firehose Delivery Stream.
    firehose_client = boto3.client("firehose")
    put_record = {"Data": record}
    firehose_client.put_record(DeliveryStreamName=stream, Record=put_record)

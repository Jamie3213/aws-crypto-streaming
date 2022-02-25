import base64

from datetime import datetime

import boto3


def get_secrets_manager_secret(name: str) -> str:
    """Returns the secret from AWS Secrets Manager.

    Args:
      name (str): The name of the secret or the secret ARN

    Returns:
      str: The secret string
    """
    secret_client = boto3.client("secretsmanager")
    secret_dict = secret_client.get_secret_value(SecretId=name)

    secret_string = secret_dict.get("SecretString")
    secret_binary = secret_dict.get("SecretBinary")

    return secret_string if secret_string else base64.b64decode(secret_binary)

def _put_record_to_kinesis_stream(record: bytes, stream: str) -> None:
    # Writes a record to a Kinesis Data Firehose Delivery Stream.
    firehose_client = boto3.client("firehose")
    put_record = {"Data": record}
    firehose_client.put_record(DeliveryStreamName=stream, Record=put_record)

def _ensure_date(date_as_string: str, format_in: str, format_out: str) -> str:
        # If the micro-second part of a timestamp is missing, then zeros are
        # added. The timestamp is then converted to the format specified and
        # returned as a string.
        try:
            date_as_datetime = datetime.strptime(date_as_string, format_in)
        except ValueError:
            first_part = date_as_string[:19]
            last_part = date_as_string[19:]
            f_part = f".{6 * '0'}"
            date_as_datetime = datetime.strptime(
                f"{first_part}{f_part}{last_part}", format_in
            )

        return datetime.strftime(date_as_datetime, format_out)

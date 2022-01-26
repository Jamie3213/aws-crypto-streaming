import base64
import gzip
import json
from dataclasses import asdict
from typing import List

import boto3

from tiingo_session import TradeUpdateMessage


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


def put_record_to_firehose(stream_name: str, record: bytes) -> None:
    """Writes a record to a Kinesis Data Firehose Delivery Stream.

    Args:
      stream_name (str): The name of the Firehose stream
      batch (bytes): A compressed bytes object of JSON messages

    Returns:
      None
    """
    firehose_client = boto3.client("firehose")
    put_record = {"Data": record}
    firehose_client.put_record(DeliveryStreamName=stream_name, Record=put_record)


def compress_message_batch(batch: List[TradeUpdateMessage]) -> bytes:
    """Converts a list of TradeUpdateMessage dataclasses to a string of new line delimited
    JSON documents and GZIP compresses the result.

    Args:
      batch (List[TradeUpdateMessage]): List of messages from the API.

    Returns:
      bytes: compressed string of JSON records.
    """
    batch_dicts = [asdict(message) for message in batch]
    batch_strings = [json.dumps(message) for message in batch_dicts]
    new_line_delimited = "\n".join(batch_strings) + "\n"
    batch_bytes = new_line_delimited.encode("utf-8")

    return gzip.compress(batch_bytes)

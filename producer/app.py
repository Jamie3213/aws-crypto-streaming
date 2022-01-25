import base64
import gzip
import json
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Literal

import boto3
import yaml
from botocore.exceptions import ClientError
from websocket import WebSocket

from logger import logger


@dataclass
class TradeUpdateMessage:
    """
    Class to store trade updates from the Tiingo crypto API.

    Attributes:
        update_message_type (str): The type of price update - "Q" if top-of-book, "T" if last trade
        ticker (str): The asset ticker
        date (str): The datetime the trade came in
        exchange (str): The exchange on which the trade was made
        last_size (str): The amount of crypto volume done at the last price in base currency
        last_price (str): The last price the last trade was executed at
        processed_at (str): The datetime the API message was processed by the application
    """

    update_message_type: Literal["Q", "T"]
    ticker: str
    date: str
    exchange: str
    last_size: float
    last_price: float
    processed_at: str


class WebSocketSubscribeError(Exception):
    pass


def get_secret(name: str) -> str:
    """
    Returns the secret from AWS Secrets Manager.

    Args:
        name (str): The name of the secret or the secret ARN

    Returns:
        str: The secret string
    """
    secret_client = boto3.client("secretsmanager")
    secret_dict = secret_client.get_secret_value(SecretId=name)

    secret_string = secret_dict.get("SecretString")
    secret_binary = secret_dict.get("SecretBinary")
    secret = secret_string if secret_string else base64.b64decode(secret_binary)

    return secret


def create_websocket(url: str, token: str) -> WebSocket:
  """
  Connects and subscribes to the specified WebSocket.

  Args:
      url (str): The websocket URL endpoint
      token (str): The auth token for the API

  Returns:
      websocket.WebSocket: WebSocket object
  """

  ws = WebSocket()
  ws.connect(url)

  subscribe = {
    "eventName": "subscribe",
    "authorization": token,
    "eventData": {"thresholdLevel": 5},
  }

  # Subscribe and flush the API
  ws.send(json.dumps(subscribe))
  response = json.loads(ws.recv())["response"]
  response_code = response["code"]
  response_message = response["message"]

  if response_code != 200:
    raise WebSocketSubscribeError(
      f"""Subscription failed with error code {response_code} and message '{response_message}'."""
    )

  heartbeat = json.loads(ws.recv())["response"]
  heartbeat_code = heartbeat["code"]
  heartbeat_message = heartbeat["message"]

  if heartbeat_code != 200:
    raise WebSocketSubscribeError(
      f"""Heartbeat check failed with error code {heartbeat_code} and message '{heartbeat_message}'."""
    )

  return ws


def process_message(message: str) -> dict:
  """
  Returns a dictionary of the API record after conversion to a TradeUpdateMessage dataclass.

  Args:
      message (dict): The JSON string message from the API.

  Returns:
      dict: A dictionary of the TradeUpdateMessage dataclass.
  """
  record = json.loads(message)
  data = record["data"]

  try:
    trade_datetime_dt = datetime.strptime(data[2], "%Y-%m-%dT%H:%M:%S.%f%z")
  except ValueError:
    first_part = data[2][:19]
    last_part = data[2][19:]
    add_f_part = f"{first_part}.000000{last_part}"
    trade_datetime_dt = datetime.strptime(add_f_part, "%Y-%m-%dT%H:%M:%S.%f%z")

  update_message_type = data[0]
  ticker = data[1]
  trade_datetime = trade_datetime_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
  exchange = data[3]
  last_size = data[4]
  last_price = data[5]
  now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

  trade_update_message = TradeUpdateMessage(
    update_message_type,
    ticker,
    trade_datetime,
    exchange,
    last_size,
    last_price,
    now,
  )

  return asdict(trade_update_message)


def put_batch_to_kinesis_firehose(stream_name: str, batch: bytes) -> None:
  """
  Writes a Snappy compressed record batch to a Kinesis Data Firehose Delivery Stream.

  Args:
      stream_name (str): The name of the Firehose stream
      batch (bytes): A compressed bytes object of JSON messages
  """
  firehose_client = boto3.client("firehose")
  put_record = {"Data": batch}
  firehose_client.put_record(DeliveryStreamName=stream_name, Record=put_record)


def main() -> None:
  # Get config variables
  logger.info("Reading YAML config and extracting variables.")
  with open("config.yml", "r") as stream:
    try:
      config = yaml.safe_load(stream)
    except yaml.YAMLError as e:
      logger.error(traceback.format_exc())
      raise e

  API_URL = config["Api"]["Url"]
  API_SECRET_NAME = config["Api"]["SecretName"]
  FIREHOSE_STREAM_NAME = config["Firehose"]["StreamName"]
  BATCH_SIZE = config["Firehose"]["BatchSize"]

  # Subscribe to API
  logger.info("Getting API token from Secrets Manager.")
  try:
    token = get_secret(API_SECRET_NAME)
  except ClientError as e:
    logger.error(traceback.format_exc())
    raise e

  logger.info("Subscribing to Tiingo WebSocket Crypto API.")
  try:
    ws = create_websocket(API_URL, token)
  except Exception as e:
    logger.error(traceback.format_exc())
    raise Exception(f"Failed to connect and subscribe to WebSocket with error: {e}")

  while True:
    # Create record batches
    batch = [ws.recv() for _ in range(BATCH_SIZE)]
    trade_updates = [process_message(message) for message in batch]
    strings = [json.dumps(record) for record in trade_updates]
    single_string = "\n".join(strings) + "\n"
    batch_as_bytes = single_string.encode("utf-8")
    compressed_batch = gzip.compress(batch_as_bytes)

    try:
      put_batch_to_kinesis_firehose(FIREHOSE_STREAM_NAME, compressed_batch)
    except ClientError as e:
      logger.error(traceback.format_exc())
      raise e
        

if __name__ == "__main__":
  main()

import boto3
from shared.config import get_settings

_settings = get_settings()

_sqs_client = None
_dynamodb_table = None


def get_sqs_client():
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = boto3.client("sqs", region_name=_settings.AWS_REGION)
    return _sqs_client


def get_dynamodb_table():
    global _dynamodb_table
    if _dynamodb_table is None:
        dynamodb = boto3.resource("dynamodb", region_name=_settings.AWS_REGION)
        _dynamodb_table = dynamodb.Table(_settings.DYNAMODB_TABLE_NAME)
    return _dynamodb_table


def set_sqs_client(client) -> None:
    global _sqs_client
    _sqs_client = client


def set_dynamodb_table(table) -> None:
    global _dynamodb_table
    _dynamodb_table = table

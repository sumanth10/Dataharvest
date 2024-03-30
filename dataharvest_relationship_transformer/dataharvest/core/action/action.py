import os
import boto3
import logging

from scalable_python_lambda_sdk.scalable.execption.infrastructure_error_execption import (
    InfrastructureError,
)

PROCESSING_FILE_STATE_TABLE_NAME = os.getenv(key="PROCESSING_FILE_STATE_TABLE_NAME")
EXT_CLIENT = os.getenv(key="EXT_CLIENT")


class PrimaryKey:
    def __init__(self, partition_key, sort_key):
        self.partition_key = partition_key
        self.sort_key = sort_key

    def _get_primary_key(self):
        return {
            "partition_key": f"{EXT_CLIENT}#{self.partition_key}",
            "sort_key": str(self.sort_key),
        }


class Action:
    def __init__(self) -> None:
        self.dynamodb = boto3.resource("dynamodb")
        self.processing_file_state_table = self.dynamodb.Table(
            "processing_file_state_table"
        )

    def update(self, event):
        logging.info("Persisting relation_established key to DynamoDB")
        try:
            partition_key = event["dynamodb"]["Keys"]["PK"]
            sort_key = event["dynamodb"]["Keys"]["SK"]
            self.primary_key = PrimaryKey(
                partition_key=partition_key, sort_key=sort_key
            )._get_primary_key()
            update_expression = "SET relation_established = :val"
            expression_attribute_values = {":val": True}

            response = self.processing_file_state_table.update_item(
                Key=self.primary_key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="UPDATED_NEW",
            )
            return response
        except Exception as error:
            if "ConditionalCheckFailedException" in str(error):
                raise InfrastructureError("DynamoDB", f"Item already exist")
            else:
                raise InfrastructureError("DynamoDB", f"Error updating {error}")

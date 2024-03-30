import boto3
import os
import logging

from scalable_python_lambda_sdk.scalable.execption.infrastructure_error_execption import (
    InfrastructureError,
)


PROCESSING_FILE_STATE_TABLE_NAME = os.getenv(key="PROCESSING_FILE_STATE_TABLE_NAME")
EXT_CLIENT = os.getenv(key="EXT_CLIENT")


class PrimaryKey:
    def __init__(self, sort_key):
        self.sort_key = sort_key

    def _get_primary_key(self):
        return {
            "partition_key": f"{EXT_CLIENT}",
            "sort_key": str(self.sort_key),
        }


class Action:
    def __init__(self) -> None:
        self.dynamodb = boto3.resource("dynamodb")
        self.processing_file_state_table = self.dynamodb.Table(
            "processing_file_state_table"
        )

    def _update(self, event, update_expression, expression_attribute_values):
        try:

            object_key = event["Records"][0]["s3"]["object"]["key"]
            self.action_type = object_key.split("_")[0]
            self.persist_date = object_key.split("_")[1]
            self.primary_key = PrimaryKey(sort_key=self.persist_date)._get_primary_key()
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
                raise Exception(f"Error updating {error}")

    def persist():
        raise NotImplementedError()


class UpdateAccountPersistAction(Action):
    def __init__(self) -> None:
        super().__init__()

    def persist(self, event):
        logging.info("Persisting account_persisted key to DynamoDB")
        try:
            update_expression = "SET account_persisted = :val"
            expression_attribute_values = {":val": True}

            self._update(event, update_expression, expression_attribute_values)
            return True
        except Exception as error:
            raise InfrastructureError(
                "DynamoDB", f"Error persisting account_persisted state {error}"
            )


class UpdateClientPersistAction(Action):
    def __init__(self) -> None:
        super().__init__()

    def persist(self, event):
        logging.info("Persisting client_persisted key to DynamoDB")
        try:
            update_expression = "SET client_persisted = :val"
            expression_attribute_values = {":val": True}

            self._update(event, update_expression, expression_attribute_values)
            return True
        except Exception as error:
            raise InfrastructureError(
                "DynamoDB", f"Error persisting client_persisted state, {error}"
            )


class UpdatePortfolioPersistAction(Action):
    def __init__(self) -> None:
        super().__init__()

    def persist(self, event):
        logging.info("Persisting portfolio_persisted key to DynamoDB")
        try:
            update_expression = "SET portfolio_persisted = :val"
            expression_attribute_values = {":val": True}

            self._update(event, update_expression, expression_attribute_values)
            return True
        except Exception as error:
            raise InfrastructureError(
                "DynamoDB", f"Error persisting portfolio_persisted state, {error}"
            )


class UpdateTransactionPersistAction(Action):
    def __init__(self) -> None:
        super().__init__()

    def persist(self, event):
        logging.info("Persisting transaction_persisted key to DynamoDB")
        try:
            update_expression = "SET transaction_persisted = :val"
            expression_attribute_values = {":val": True}

            self._update(event, update_expression, expression_attribute_values)
            return True
        except Exception as error:
            raise InfrastructureError(
                "DynamoDB", f"Error persisting transaction_persisted state, {error}"
            )

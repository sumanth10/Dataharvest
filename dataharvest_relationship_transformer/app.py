import logging


from scalable_python_lambda_sdk.scalable.decorators.logging import configure_logging
from scalable_python_lambda_sdk.scalable.model.file_type import FileType
from scalable_relationship_transformer.scalable.core.action.action import Action
from scalable_relationship_transformer.scalable.core.infrastructure.postgres_sql_connector import (
    PostGresSQLOutputConnector,
)
from scalable_relationship_transformer.scalable.core.relationship_handler.relationship_handler import (
    RelationshipHandler,
)


def get_sort_key_from_event(event):
    try:
        sort_key = event["dynamodb"]["Keys"]["SK"]
        return sort_key
    except KeyError:
        raise ("Sort key 'SK' not found in the event.")

# Please add CSV file in scalable/pipelines/base_modification_pipeline.py to test
@configure_logging()
def lambda_handler(event, context):
    logging.info(
        f"Starting relationship transformer lambda execution from event {event}"
    )
    try:
        postgres_connection = PostGresSQLOutputConnector()

        file_type = [FileType.PORTFOLIOS.value, FileType.TRANSTACTIONS.value]

        for value in file_type:
            date_of_processing = get_sort_key_from_event(event)
            orm_object = RelationshipHandler(
                value, date_of_processing, postgres_connection
            ).process_files_and_get_orm()
            postgres_connection.write_to_postgres(orm_object)
        Action().update(event)
        logging.info(f"Relationship establised successfully")

    except Exception as error:
        logging.error(error)

    return


# For local testing
if __name__ == "__main__":
    # Create a sample event for testing
    sample_event = {
        "eventID": "c9fbe7d0261a5163fcb6940593e41797",
        "eventName": "INSERT",
        "eventVersion": "1.1",
        "eventSource": "aws:dynamodb",
        "awsRegion": "us-east-2",
        "dynamodb": {
            "ApproximateCreationDateTime": 1664559083.0,
            "Keys": {"PK": "ARSENAL", "SK": "20200130"},
            "NewImage": {
                "quantity": {"N": "50"},
                "company_id": {"S": "1000"},
                "fabric": {"S": "Florida Chocolates"},
                "price": {"N": "15"},
                "stores": {"N": "5"},
                "product_id": {"S": "1000"},
                "SK": {"S": "PRODUCT#CHOCOLATE#DARK#1000"},
                "PK": {"S": "COMPANY#1000"},
                "state": {"S": "FL"},
                "type": {"S": ""},
            },
            "SequenceNumber": "700000000000888747038",
            "SizeBytes": 174,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
        "eventSourceARN": "arn:aws:dynamodb:us-east-2:111122223333:table/chocolate-table-StreamsSampleDDBTable-LUOI6UXQY7J1/stream/2022-09-30T17:05:53.209",
    }
lambda_handler(sample_event, None)

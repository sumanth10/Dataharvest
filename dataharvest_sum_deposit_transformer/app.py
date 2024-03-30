import logging


from dataharvest_python_lambda_sdk.dataharvest.decorators.logging import configure_logging
from dataharvest_python_lambda_sdk.dataharvest.model.file_type import FileType
from dataharvest_sum_deposit_transformer.dataharvest.core.action.action import Action
from dataharvest_sum_deposit_transformer.dataharvest.core.infrastructure.postgres_sql_connector import (
    PostGresSQLOutputConnector,
)
from dataharvest_sum_deposit_transformer.dataharvest.core.sum_deposit_handler.sum_deposit_handler import (
    SumDepositHandler,
)


@configure_logging()
def lambda_handler(event, context):
    logging.info(
        f"Starting Saving deposit transformer lambda execution from event {event}"
    )
    try:
        postgres_connection = PostGresSQLOutputConnector()

        date_added = sample_event["detail"]["date_added"]
        primary_key = sample_event["detail"]["primary_key"]

        portfolio_orm_sum_of_deposit = SumDepositHandler(
            postgres_connection, date_added
        ).calculate_deposit()

        postgres_connection.write_to_postgres(portfolio_orm_sum_of_deposit)

        Action().update(primary_key)
        logging.info(f"Persisted successfully")
        
    except Exception as error:
        logging.error(error)

    return


# For local testing
if __name__ == "__main__":
    # Create a sample event for testing
    sample_event = {
        "version": "0",
        "id": "abc123",
        "detail-type": "Relationships established",
        "source": "calculate_details",
        "account": "123456789012",
        "time": "2024-02-26T15:00:00Z",
        "region": "us-east-1",
        "resources": [],
        "detail": {
            "date_added": "20200130",
            "primary_key": {"PK": "ARSENAL", "SK": "20200130"},
            "additional_info": "This is additional information related to the event",
        },
    }

lambda_handler(sample_event, None)

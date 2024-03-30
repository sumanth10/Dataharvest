import logging
from dataharvest_persistence.dataharvest.core.action.action_factory import ActionFactory
from dataharvest_persistence.dataharvest.core.event_handler.event_handler import EventHandler
from dataharvest_persistence.dataharvest.core.infrastructure.postgres_sql_connector import (
    PostGresSQLOutputConnector,
)

from dataharvest_python_lambda_sdk.dataharvest.decorators.logging import configure_logging


# Please add the csv file in dataharvest/core/pipelines/base_file_extraction_pipeline.py file
@configure_logging()
def lambda_handler(event, context):
    logging.info(f"Starting persistence lambda execution from event {event}")
    try:
        postgres_connection = PostGresSQLOutputConnector()

        orm_object = EventHandler().process_files(event)

        postgres_connection.write_to_postgres(orm_object)

        action = ActionFactory.get_action(
            event["Records"][0]["s3"]["object"]["key"].split("_")[0]
        )

        action.persist(event)

        logging.info(f"Persisted successfully")

    except Exception as error:
        logging.error(error)

    return


# For local testing
if __name__ == "__main__":
    # Create a sample event for testing
    sample_event = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-2",
                "eventTime": "2019-09-03T19:37:27.192Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {"principalId": "AWS:AIDAINPONIXQXHT3IKHL2"},
                "requestParameters": {"sourceIPAddress": "205.255.255.255"},
                "responseElements": {
                    "x-amz-request-id": "D82B88E5F771F645",
                    "x-amz-id-2": "vlR7PnpV2Ce81l0PRw6jlUpck7Jo5ZsQjryTjKlc5aLWGVHPZLj5NeC6qMa0emYBDXOo6QBU0Wo=",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "828aa6fc-f7b5-4305-8584-487c791949c1",
                    "bucket": {
                        "name": "DOC-EXAMPLE-BUCKET",
                        "ownerIdentity": {"principalId": "A3I5XTEXAMAI3E"},
                        "arn": "arn:aws:s3:::lambda-artifacts-deafc19498e3f2df",
                    },
                    "object": {
                        "key": "portfolios_20200130.csv",
                        "size": 1305107,
                        "eTag": "b21b84d653bb07b05b1e6b33684dc11b",
                        "sequencer": "0C0F6F405D6ED209E1",
                    },
                },
            }
        ]
    }
    lambda_handler(sample_event, None)

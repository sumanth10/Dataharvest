import logging
import csv
import os
from scalable_python_lambda_sdk.scalable.execption.infrastructure_error_execption import (
    InfrastructureError,
)

from scalable_relationship_transformer.scalable.core.infrastructure import s3_connector

INGEST_BUCKET = os.getenv(key="INGEST_BUCKET")


class BaseModificationPipeline(object):
    def __init__(self, *args):
        pass

    def transform(self, file_type, date_of_processing, database_reader):
        try:
            logging.info(f"Downloading the file for type {file_type}")

            # file_content = s3_connector.get_file_content_as_stream(INGEST_BUCKET, f"{file_type}_{date_of_processing}.csv")

            logging.info(f"Downloading compelete for type {file_type}")

            extracted_content = self._parse_file_content(file_type)

            logging.info(f"Creating ORM for type {file_type}")

            extracted_orm_content = self._get_row_as_orm(
                date_of_processing, database_reader
            )
            
            updated_orm_content = self._get_orm_output_to_persist(
                extracted_orm_content, extracted_content
            )
            logging.info(f"ORM creation completed for type {file_type}")

            return updated_orm_content
        except Exception as error:
            logging.error(f"Something went wrong {error} for type {file_type}")

    # def _parse_file_content(self, file_content):
    #     csv_data = file_content.read()
    #     return csv.reader(csv_data.decode("utf-8"))

    # Please add CSV file here to test
    def _parse_file_content(self, file_type):
        if file_type == "transactions":
            file_path = "/Users/sumantkulkarni/Documents/playground/scalable/transactions_20200130.csv"
        elif file_type == "portfolios":
            file_path = "/Users/sumantkulkarni/Documents/playground/scalable/portfolios_20200130.csv"
        with open(file_path, mode="r", encoding="utf-8") as file:
            csv_reader = csv.DictReader(file)
            # Convert csv_reader to a list of rows
            rows_list = list(csv_reader)
        return rows_list

    def _get_row_as_orm(self, database_reader):
        raise NotImplementedError()

    def _get_orm_output_to_persist(self, extracted_orm_content, extracted_content):
        raise NotImplementedError()

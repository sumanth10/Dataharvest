import logging
import csv


from dataharvest_persistence.dataharvest.core.infrastructure.s3_connector import S3Connector
from dataharvest_python_lambda_sdk.dataharvest.execption.infrastructure_error_execption import (
    InfrastructureError,
)


class BaseFileExtractionPipeline(object):
    def __init__(self, *args):
        pass

    def transform(self, bucket, file_type, date):
        try:
            logging.debug("Downloading the file")

            # file_content = S3Connector.get_file_contents_as_stream(bucket, f"{file_type}_{date}")

            logging.debug("Downloading compelete")

            extracted_content = self._parse_file_content()

            logging.debug("Creating ORM")

            orm_content = self._get_orm_output_to_persist(extracted_content, date)

            logging.debug("ORM creation completed")

            return orm_content

        except Exception as error:
            raise error

    def _parse_file_content(self):
        file_path = "/Users/sumantkulkarni/Documents/playground/dataharvest/portfolios_20200130.csv"
        with open(file_path, mode="r", encoding="utf-8") as file:
            csv_reader = csv.DictReader(file)
            # Convert csv_reader to a list of rows
            rows_list = list(csv_reader)
        # csv_data = file_content.read()
        # return csv.reader(csv_data.decode("utf-8"))
        return rows_list

    def _get_orm_output_to_persist(self, extracted_content, date_added):
        raise NotImplementedError()

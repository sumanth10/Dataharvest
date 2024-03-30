import logging

from scalable_python_lambda_sdk.scalable.model.file_type import FileType
from scalable_relationship_transformer.scalable.core.pipelines.pipeline_factory import (
    PipelineFactory,
)


class RelationshipHandler:
    def __init__(self, file_type, date_of_processing, postgres_connection):
        self.file_type = file_type
        self.date_of_processing = date_of_processing
        self.database_reader = postgres_connection

    def process_files_and_get_orm(self):
        logging.info("Processing the file from s3 bucket")
        try:
            file_type_enum = FileType(self.file_type)
            pipeline = PipelineFactory.get_file_parser(file_type_enum)
            return pipeline.transform(
                file_type_enum.value, self.date_of_processing, self.database_reader
            )
        except ValueError as error:
            logging.warning(error)
        except Exception as error:
            logging.error(error)

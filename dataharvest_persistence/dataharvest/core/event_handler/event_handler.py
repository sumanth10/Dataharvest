import logging

from dataharvest_persistence.dataharvest.core.pipelines.pipeline_factory import PipelineFactory
from dataharvest_python_lambda_sdk.dataharvest.execption.invalid_file_type_execption import InvalidFileTypeError
from dataharvest_python_lambda_sdk.dataharvest.model.file_type import FileType


EVENT_SOURCE = "s3"


class EventHandler:

    def process_files(self, event):
        logging.info("Starting file processing")
        try:
            bucket_name = self._get_bucket_from_event(event=event)
            object_key = self._get_object_key_from_event(event=event)
            orm_object = self._process_entry_from_bucket(bucket_name, object_key)
            return orm_object
        except ValueError as error:
            logging.warning(error)
        except Exception as error:
            raise error


    def _get_bucket_from_event(self, event):
        record = event["Records"][0]
        return record["s3"]["bucket"]["name"]

    def _get_object_key_from_event(self, event):
        record = event["Records"][0]
        return record["s3"]["object"]["key"]

    def _process_entry_from_bucket(self, bucket_name, object_key):
        parts = object_key.split("_")
        if len(parts) != 2:
            raise InvalidFileTypeError("Invalid object key format")
        file_type = parts[0]
        date = parts[1].split('.')[0]

        if file_type not in [ft.value for ft in FileType]:
            raise InvalidFileTypeError(f"Invalid file type: {file_type}")

        file_type_enum = FileType(file_type)

        pipeline = PipelineFactory.get_file_parser(file_type_enum)
        return pipeline.transform(bucket_name, file_type, date)

import logging
from scalable_python_lambda_sdk.scalable.schema_registry.postgressql.transaction_orm_object import (
    Transaction,
)
from scalable_relationship_transformer.scalable.core.pipelines.base_modification_pipeline import (
    BaseModificationPipeline,
)


class TransactionModificationPipeline(BaseModificationPipeline):

    def _get_orm_output_to_persist(self, extracted_orm_content, extracted_content):
        logging.info("Getting Transaction ORM object to persist")
        # TODO: Before adding add multiple layered checks to confirm all object in postgres and csv exist
        # TODO: If no layered checks exist , add checks on extracted_orm_content to see if the record_id exists
        for row_number, row in enumerate(extracted_content):
            extracted_orm_content[row_number].account_number = row.get("accout_number")

        return extracted_orm_content

    def _get_row_as_orm(self, target_date, database_reader):
        return database_reader.get_all(Transaction, target_date, Transaction.record_id)

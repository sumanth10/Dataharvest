import logging
from dataharvest_persistence.dataharvest.core.pipelines.base_file_extraction_pipeline import BaseFileExtractionPipeline
from dataharvest_python_lambda_sdk.dataharvest.schema_registry.postgressql.transaction_orm_object import Transaction


class TransactionFileExtractionPipeline(BaseFileExtractionPipeline):
    def _get_orm_output_to_persist(self, extracted_content, date_added):
        logging.info("Getting Transaction ORM object to persist")
        orm_objects = []
        for row in extracted_content:
            # Extract data from the row
            transaction_reference = row.get("transaction_reference")
            amount = row.get("amount")
            keyword = row.get("keyword")

            # Create a Transaction ORM object
            transaction = Transaction(
                transaction_reference=transaction_reference,
                amount=amount,
                keyword=keyword,
                date_added=date_added
            )
            orm_objects.append(transaction)

        return orm_objects

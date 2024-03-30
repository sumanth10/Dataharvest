import logging
from dataharvest_persistence.dataharvest.core.pipelines.base_file_extraction_pipeline import (
    BaseFileExtractionPipeline,
)
from dataharvest_python_lambda_sdk.dataharvest.schema_registry.postgressql.accounts_orm_object import (
    Account,
)


class AccountFileExtractionPipeline(BaseFileExtractionPipeline):
    def _get_orm_output_to_persist(self, extracted_content, date_added):
        logging.info("Getting Account ORM object to persist")
        orm_objects = []
        for row in extracted_content:
            record_id = row.get("record_id")
            account_number = row.get("accout_number")
            cash_balance = row.get("cash_balance")
            currency = row.get("currency")
            taxes_paid = row.get("taxes_paid")

            # Create an Account ORM object
            account = Account(
                record_id=record_id,
                account_number=account_number,
                cash_balance=cash_balance,
                currency=currency,
                taxes_paid=taxes_paid,
                date_added=date_added,
            )
            orm_objects.append(account)

        return orm_objects

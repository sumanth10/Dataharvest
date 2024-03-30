import logging
from dataharvest_persistence.dataharvest.core.pipelines.base_file_extraction_pipeline import (
    BaseFileExtractionPipeline,
)
from dataharvest_python_lambda_sdk.dataharvest.schema_registry.postgressql.clients_orm_object import (
    Client,
)


class ClientFileExtractionPipeline(BaseFileExtractionPipeline):
    def _get_orm_output_to_persist(self, extracted_content, date_added):
        logging.info("Getting Account ORM object to persist")
        orm_objects = []
        for row in extracted_content:
            # Extract data from the row
            record_id = row.get("record_id")
            first_name = row.get("first_name")
            last_name = row.get("last_name")
            client_reference = row.get("client_reference")
            tax_free_allowance = row.get("tax_free_allowance")

            # Create a Client ORM object
            client = Client(
                record_id=record_id,
                first_name=first_name,
                last_name=last_name,
                client_reference=client_reference,
                tax_free_allowance=tax_free_allowance,
                date_added=date_added,
            )
            orm_objects.append(client)

        return orm_objects

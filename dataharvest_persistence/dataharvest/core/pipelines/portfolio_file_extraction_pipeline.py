import logging
from dataharvest_persistence.dataharvest.core.pipelines.base_file_extraction_pipeline import BaseFileExtractionPipeline
from dataharvest_python_lambda_sdk.dataharvest.schema_registry.postgressql.portfolios_orm_object import Portfolio


class PortfolioFileExtractionPipeline(BaseFileExtractionPipeline):
    def _get_orm_output_to_persist(self, extracted_content, date_added):
        logging.info("Getting Portfolio ORM object to persist")
        orm_objects = []
        for row in extracted_content:
            # Extract data from the row
            portfolio_reference = row.get("portfolio_reference")
            agent_code = row.get("agent_code")

            # Create a Portfolio ORM object
            portfolio = Portfolio(
                portfolio_reference=portfolio_reference,
                agent_code=agent_code,
                date_added=date_added
            )
            orm_objects.append(portfolio)

        return orm_objects

from enum import Enum
import logging

from dataharvest_persistence.dataharvest.core.pipelines.account_file_extraction_pipeline import (
    AccountFileExtractionPipeline,
)

from dataharvest_persistence.dataharvest.core.pipelines.clients_file_extraction_pipeline import (
    ClientFileExtractionPipeline,
)
from dataharvest_persistence.dataharvest.core.pipelines.portfolio_file_extraction_pipeline import (
    PortfolioFileExtractionPipeline,
)
from dataharvest_persistence.dataharvest.core.pipelines.transactions_file_extraction_pipeline import (
    TransactionFileExtractionPipeline,
)
from dataharvest_python_lambda_sdk.dataharvest.execption.unmapper_connection_type_execption import UnmappedConnectionTypeError
from dataharvest_python_lambda_sdk.dataharvest.model.file_type import FileType


class PipelineFactory:
    FILE_TYPE_MAPPER = {
        FileType.CLIENTS: ClientFileExtractionPipeline(),
        FileType.ACCOUNTS: AccountFileExtractionPipeline(),
        FileType.PORTFOLIOS: PortfolioFileExtractionPipeline(),
        FileType.TRANSTACTIONS: TransactionFileExtractionPipeline(),
    }

    @staticmethod
    def get_file_parser(file_type: FileType):
        logging.info(f"Getting mapper object for file type {file_type}")
        if file_type not in PipelineFactory.FILE_TYPE_MAPPER:
            raise UnmappedConnectionTypeError(
                f"There are no associated file parser for the provide file ({file_type})"
            )

        return PipelineFactory.FILE_TYPE_MAPPER[file_type]

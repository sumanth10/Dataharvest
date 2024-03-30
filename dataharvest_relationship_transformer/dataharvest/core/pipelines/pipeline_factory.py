import logging
from scalable_python_lambda_sdk.scalable.execption.unmapper_connection_type_execption import UnmappedConnectionTypeError
from scalable_python_lambda_sdk.scalable.model.file_type import FileType
from scalable_relationship_transformer.scalable.core.pipelines.portfolio_file_extraction_pipeline import PortfolioModificationPipeline
from scalable_relationship_transformer.scalable.core.pipelines.transactions_file_extraction_pipeline import TransactionModificationPipeline


class PipelineFactory:
    FILE_TYPE_MAPPER = {
        FileType.PORTFOLIOS: PortfolioModificationPipeline(),
        FileType.TRANSTACTIONS: TransactionModificationPipeline(),
    }

    @staticmethod
    def get_file_parser(file_type: FileType):
        logging.info("Getting mapper object from the pipelinefactory")
        if file_type not in PipelineFactory.FILE_TYPE_MAPPER:
            raise UnmappedConnectionTypeError(
                f"There are no associated file parser for the provide file ({file_type})"
            )

        return PipelineFactory.FILE_TYPE_MAPPER[file_type]

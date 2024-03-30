from scalable.core.action.action import (
    UpdateAccountPersistAction,
    UpdateClientPersistAction,
    UpdatePortfolioPersistAction,
    UpdateTransactionPersistAction,
)
from scalable_python_lambda_sdk.scalable.execption.unmapper_connection_type_execption import UnmappedConnectionTypeError

from scalable_python_lambda_sdk.scalable.model.file_type import FileType


class ActionFactory:
    ACTION_MAP = {
        FileType.CLIENTS: UpdateClientPersistAction(),
        FileType.ACCOUNTS: UpdateAccountPersistAction(),
        FileType.PORTFOLIOS: UpdatePortfolioPersistAction(),
        FileType.TRANSTACTIONS: UpdateTransactionPersistAction(),
    }

    @staticmethod
    def get_action(action: FileType):
        action = FileType(action)
        if action not in ActionFactory.ACTION_MAP:
            raise UnmappedConnectionTypeError(
                f"Received an unmapped action {action}. Please check the file processed"
            )

        return ActionFactory.ACTION_MAP[action]

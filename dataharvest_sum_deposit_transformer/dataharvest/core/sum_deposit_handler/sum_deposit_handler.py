from dataharvest_python_lambda_sdk.dataharvest.schema_registry.postgressql.portfolios_orm_object import (
    Portfolio,
)
from dataharvest_python_lambda_sdk.dataharvest.schema_registry.postgressql.transaction_orm_object import (
    Transaction,
)
import logging


class SumDepositHandler(object):
    def __init__(self, database_reader, calculation_date):
        self.database_reader = database_reader
        self.calculation_date = calculation_date

    def calculate_deposit(self):
        try:
            logging.info(f"Calculating the sum of deposit")
            portfolio_orm = self._get_portfolio_orm()
            for portfolio in portfolio_orm:
                account_number = portfolio.account_number
                deposit_amount = self._get_total_deposit(account_number)
                portfolio.sum_of_deposit = deposit_amount
            return portfolio_orm
        except Exception as error:
            raise error

    def _get_portfolio_orm(self):
        logging.info(f"Getting profolio ORM object")
        try:
            portfolio_orm = self.database_reader.get_all(
                Portfolio, self.calculation_date, Portfolio.record_id
            )
            return portfolio_orm
        except Exception as error:
            raise error

    def _get_total_deposit(self, account_number):
        logging.info(f"Getting transaction details")
        try:
            filters = {"account_number": account_number, "keyword": "DEPOSIT"}
            transactions_orm = self.database_reader.get_all(
                Transaction,
                self.calculation_date,
                Transaction.record_id,
                filters=filters,
            )
            total_deposit = sum(
                [transaction.amount for transaction in transactions_orm]
            )
            return total_deposit
        except Exception as error:
            raise error

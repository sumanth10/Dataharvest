from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from scalable_python_lambda_sdk.scalable.schema_registry.postgressql import Base
from scalable_python_lambda_sdk.scalable.schema_registry.postgressql.portfolios_orm_object import Portfolio
from scalable_python_lambda_sdk.scalable.schema_registry.postgressql.transaction_orm_object import Transaction
from scalable_python_lambda_sdk.scalable.schema_registry.postgressql.accounts_orm_object import Account
from scalable_python_lambda_sdk.scalable.schema_registry.postgressql.clients_orm_object import Client


class DatabaseSession:
    def __init__(self, db_conn_string, debug_mode=False):
        self.engine = self.create_db_engine(db_conn_string, debug_mode)
        self.Session = self.create_db_session()

    def create_db_engine(self, db_conn_string, debug_mode=False):
        try:
            engine = create_engine(
                db_conn_string,
                echo=debug_mode,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
                pool_pre_ping=True,
                pool_use_lifo=True,
            )
            Base.metadata.create_all(engine)
            return engine
        except Exception as e:
            raise e

    def create_db_session(self):
        Session = sessionmaker(bind=self.engine)
        return Session()

    def get_session(self):
        return self.Session()

    def close_session(self):
        self.Session.close()

from scalable_python_lambda_sdk.scalable.execption.infrastructure_error_execption import InfrastructureError
from scalable_python_lambda_sdk.scalable.infrastructure.postgressql.connection_utils import (
    DatabaseSession,
)
import logging


class PostGresSQLOutputConnector:
    def __init__(self):
        # TODO: Getting connection string from AWS secrets
        try:
            logging.info("Connecting to PostgreSQL server")
            self.db_session = DatabaseSession(
                "postgresql://postgres:mysecretpassword@localhost/scalable"
            )
            logging.info("Connected succesfully")
        except Exception as error:
            raise InfrastructureError(
                "postgresSQL", f"Error while trying to connect to postgresSQL: {error}"
            )

    def get_instance(self):
        return self.db_session.get_session()

    def write_to_postgres(self, orm_objects):
        session = self.db_session.create_db_session()
        duplicate_keys = []
        try:
            for orm_object in orm_objects:
                try:
                    session.add(orm_object)
                    session.commit()
                except Exception as error:
                    session.rollback()
                    # Check if the exception is due to a unique constraint violation
                    if "duplicate key value violates unique constraint" in str(error):
                        # Log the key that caused the duplicate key violation
                        duplicate_keys.append(orm_object)
                    else:
                        raise InfrastructureError(
                            "postgresSQL",
                            f"Error while trying to persist to postgresSQL: {error}",
                        )
        except Exception as error:
            session.rollback()
            raise InfrastructureError(
                "postgresSQL", f"Error while recursing through objects: {error}"
            )
        finally:
            self.db_session.close_session()

        # Log the duplicate keys
        if duplicate_keys:
            logging.info("Duplicate keys detected:")
            for key in duplicate_keys:
                logging.info(key)

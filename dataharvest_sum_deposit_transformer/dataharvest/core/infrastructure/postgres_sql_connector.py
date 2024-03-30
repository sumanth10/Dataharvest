import logging
from dataharvest_python_lambda_sdk.dataharvest.execption.infrastructure_error_execption import (
    InfrastructureError,
)

from dataharvest_python_lambda_sdk.dataharvest.infrastructure.postgressql.connection_utils import (
    DatabaseSession,
)


class PostGresSQLOutputConnector:
    def __init__(self) -> None:
        #  TODO: Get Postgres details from AWS secrets
        self.db_session = DatabaseSession(
            "postgresql://postgres:mysecretpassword@localhost/dataharvest"
        )

    def get_instance(self):
        return self.db_session.get_session()

    def write_to_postgres(self, orm_objects):
        session = self.db_session.create_db_session()
        duplicate_keys = []
        try:
            for orm_object in orm_objects:
                try:
                    session.merge(orm_object)
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

    def get_all(self, entity_type, target_date, order_by, filters=None):
        logging.info(f"Getting list of ORM object based on filter")
        session = self.db_session.create_db_session()
        query = session.query(entity_type)
        query = query.filter(entity_type.date_added == target_date)
        if filters:
            for column, value in filters.items():
                query = query.filter(getattr(entity_type, column) == value)
        if order_by:
            query = query.order_by(order_by)

        results = query.all()
        session.close()
        return results
    

    
    

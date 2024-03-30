from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from scalable_python_lambda_sdk.scalable.schema_registry.postgressql import Base

# Base = declarative_base()


class Client(Base):
    __tablename__ = "clients"

    record_id = Column(Integer)
    first_name = Column(String(100))
    last_name = Column(String(100))
    client_reference = Column(String(50), primary_key=True, unique=True)
    tax_free_allowance = Column(Integer)
    date_added = Column(Integer)
    # Relationship with Portfolio model (one-to-many)
    portfolio = relationship("Portfolio", back_populates="client")

    def __repr__(self):
        return f"<Client(first_name='{self.first_name}', last_name='{self.last_name}', client_reference='{self.client_reference}', tax_free_allowance={self.tax_free_allowance})>"

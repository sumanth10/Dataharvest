from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from scalable_python_lambda_sdk.scalable.schema_registry.postgressql import Base


# Initialised from the init file 
class Account(Base):
    __tablename__ = "accounts"

    record_id = Column(Integer)
    account_number = Column(Integer, primary_key=True, unique=True)
    cash_balance = Column(Float)
    currency = Column(String(10))
    taxes_paid = Column(Float)
    date_added = Column(Integer)

    # Relationship with Transaction model (one-to-many)
    transaction = relationship("Transaction", back_populates="account")
    portfolio = relationship("Portfolio", back_populates="account")

    def __repr__(self):
        return f"<Account(account_number={self.account_number}, cash_balance={self.cash_balance}, currency='{self.currency}', taxes_paid={self.taxes_paid})>"

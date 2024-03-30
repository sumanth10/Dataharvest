from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from scalable_python_lambda_sdk.scalable.schema_registry.postgressql import Base


class Transaction(Base):
    __tablename__ = "transactions"

    record_id = Column(Integer, primary_key=True)
    account_number = Column(Integer, ForeignKey("accounts.account_number"))
    transaction_reference = Column(String(50))
    amount = Column(Float)
    keyword = Column(String(10))
    date_added = Column(Integer)

    account = relationship("Account", back_populates="transaction")

    def __repr__(self):
        return f"<Transaction(account_number={self.account_number}, transaction_reference='{self.transaction_reference}', amount={self.amount}, keyword='{self.keyword}')>"

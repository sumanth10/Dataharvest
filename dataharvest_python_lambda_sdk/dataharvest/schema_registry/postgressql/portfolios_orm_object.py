from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from scalable_python_lambda_sdk.scalable.schema_registry.postgressql import Base


class Portfolio(Base):
    __tablename__ = "portfolios"

    record_id = Column(Integer, primary_key=True)
    account_number = Column(Integer, ForeignKey("accounts.account_number"))
    portfolio_reference = Column(String(50))
    client_reference = Column(String(50), ForeignKey("clients.client_reference"))
    agent_code = Column(String(10))
    date_added = Column(Integer)
    sum_of_deposit = Column(Integer)

    client = relationship("Client", back_populates="portfolio")
    account = relationship("Account", back_populates="portfolio")

    def __repr__(self):
        return f"<Portfolio(account_number={self.account_number}, portfolio_reference='{self.portfolio_reference}', client_reference='{self.client_reference}', agent_code='{self.agent_code}')>"

from sqlalchemy import Column, Integer, String, Numeric, ForeignKey, CheckConstraint
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Transactions(Base):
    __tablename__ = 'transactions'

    id = Column(Integer, primary_key=True)
    transaction_id = Column(String, nullable=False, unique= True)
    timestamp = Column(String, nullable=False)
    amount = Column(Numeric, nullable=False)
    currency = Column(String, nullable=False)
    sender_account = Column(String, nullable=False)
    receiver_account = Column(String, nullable=False)
    sender_country = Column(String, nullable=False)
    sender_municipality = Column(String, nullable=False)
    receiver_country = Column(String, nullable=False)
    receiver_municipality = Column(String, nullable=False)
    transaction_type = Column(String, nullable=False)
    notes = Column(String, nullable=False)

class Account(Base):
    __tablename__ = 'accounts'

    id = Column(Integer, primary_key=True)
    customer_name = Column(String, nullable = False)
    address = Column(String, nullable = False)
    phone_number = Column(String, nullable = False)
    person_number = Column(String, nullable = False)
    account_number = Column(String, nullable = False, unique= True)
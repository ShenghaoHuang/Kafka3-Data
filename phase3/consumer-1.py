import os
from kafka import KafkaConsumer, TopicPartition
from json import loads
from sqlalchemy import Column, Integer, String, create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

sql_user = os.getenv('sql_user')
sql_pw = os.getenv('sql_pw')
Base = declarative_base()
sql_connector = f'mysql+mysqlconnector://{sql_user}:{sql_pw}@localhost/transaction'
engine = create_engine(sql_connector)
Session = sessionmaker(bind=engine)

class Customer(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    custid = Column(Integer, primary_key=True)
    createdate = Column(Integer)
    fname = Column(String(250), nullable=False)
    lname = Column(String(250), nullable=False)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current balance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.

        #Go back to the readme.

    def handleMessages(self):
        self.consumer.assign([TopicPartition('bank-customer-events', 0)])
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL using SQLalchemy
            info = Customer(custid=message['custid'], createdate=message['createdate'], fname=message['fname'], lname=message['lname'])
            session = Session()
            session.add(info)
            session.commit()


if __name__ == "__main__":
    Base.metadata.create_all(engine)
    c = XactionConsumer()
    c.handleMessages()
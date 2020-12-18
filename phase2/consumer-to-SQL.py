import statistics
from kafka import KafkaConsumer, TopicPartition
from json import loads

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
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
        deposit = []
        withdrawl = []
        limit = 0
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL using SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                deposit.append(message['amt'])
            else:
                self.custBalances[message['custid']] -= message['amt']
                withdrawl.append(message['amt'])
            print(self.custBalances)
            if len(deposit) >= 1:
                average = sum(deposit)/len(deposit)
                print(f'Total Deposit amount: {len(deposit)}, average deposit = {round(average, 2)}')

            if len(withdrawl) >= 1:
                average_w = sum(withdrawl)/len(withdrawl)
                print(f'Total Withdrawal amount: {len(withdrawl)}, average withdraw = {round(average_w, 2)}')
            for x in self.custBalances:
                if self.custBalances[x] < -5000:
                    limit = self.custBalances[x]
                    for key, value in self.custBalances.items():
                        if limit == value:
                            print(f'{key} HAS EXCEEDED THE LIMIT OF -5000 BEWARE')


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
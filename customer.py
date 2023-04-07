import time, asyncio
import pandas as pd
from datetime import datetime

from helpers.kafka_helper import init_consumer, produce_record, consume_record
from helpers.coinone_helper import get_currency_data


def consumer_currency_data(consumer):
    data = pd.DataFrame(columns=["time", "value"])

    record = consume_record(consumer)
    print(f"Consume record form topic BTC at time {datetime.utcnow()}")
    for r in record:
        print(r)

def run_consumer():
    consumer_BTC = init_consumer(topic_list="topic_BTC")
    consumer_ETH = init_consumer(topic_list="topic_ETH")
    consumer_LINK = init_consumer(topic_list="topic_LINK")

    while True:
        consumer_currency_data(consumer_BTC),
        consumer_currency_data(consumer_ETH),
        consumer_currency_data(consumer_LINK)


if __name__ == "__main__":
    run_consumer()

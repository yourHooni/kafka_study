import json
from datetime import datetime
from typing import Union, List

from kafka import KafkaProducer, KafkaConsumer


def init_producer():
    """
    kafka producer 인스턴스 생성

    :return: kafka producer
    """
    print("initializing kafka producer at {}".format(datetime.utcnow()))
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("initialized kafka producer at {}".format(datetime.utcnow()))
    return producer


def init_consumer(topic_list: Union[List[str], str], timeout=1000):
    """
    kafka consumer 인스턴스 생성

    :return: kafka consumer
    """
    print("initializing kafka consumer at {}".format(datetime.utcnow()))
    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    consumer.subscribe(topic_list)
    print("initialized kafka consumer at {}".format(datetime.utcnow()))
    return consumer


def produce_record(data, producer, topic, partition=0):
    producer.send(topic=topic, partition=partition, value=data)


def consume_record(consumer):
    return [record for record in consumer]

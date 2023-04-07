import time, asyncio

from helpers.kafka_helper import init_producer, produce_record
from helpers.coinone_helper import get_currency_data


async def produce_currency_data(producer, currency, ref_currency, time_interval):
    while True:
        t = time.time()
        result = get_currency_data(currency=currency, ref_currency=ref_currency)

        produce_record(result, producer, f"topic_{currency}")
        await asyncio.sleep(time_interval - (time.time() - t))


async def run_producer():
    api_call_period = 5

    producer = init_producer()

    await asyncio.gather(
        produce_currency_data(producer=producer, currency="BTC", ref_currency="USD", time_interval=api_call_period),
        produce_currency_data(producer=producer, currency="ETH", ref_currency="USD", time_interval=api_call_period),
        produce_currency_data(producer=producer, currency="LINK", ref_currency="USD", time_interval=api_call_period)
    )


if __name__ == "__main__":
    asyncio.run(run_producer())


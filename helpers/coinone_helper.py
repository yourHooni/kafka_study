import requests
import json
from time import time
from datetime import datetime

coinone_base_url = "https://api.coinbase.com/v2"


def get_currency_data(currency, ref_currency, market_type="spot"):
    """
    currency 정보 조회
    :param currency: 자산
    :param ref_currency: ref 자산
    :param market_type: market type
    :return: 조회 결과
    """
    uri = f"{coinone_base_url}/prices/{currency}-{ref_currency}/{market_type}"
    res = requests.get(uri)

    if res.status_code == 200:
        raw_data = json.loads(res.content)
        data = {
            "timestamp": int(time() * 1000),
            "currency": raw_data["data"]["base"],
            "amount": float(raw_data["data"]["amount"])
        }
        print("Record: {}".format(data))
        return data
    else:
        print("Failed API request at time {0}".format(datetime.utcnow()))
        return None


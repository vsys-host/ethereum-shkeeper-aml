import requests as rq
import hashlib

from .config import config

if (config['EXTERNAL_DRAIN_CONFIG'] and
    (config['EXTERNAL_DRAIN_CONFIG']['aml_check']['state'] == 'enabled')):
    ACCESS_URL = config['EXTERNAL_DRAIN_CONFIG']['aml_check']['access_point']
    ACCESS_KEY = config['EXTERNAL_DRAIN_CONFIG']['aml_check']['access_key']
    ACCESS_ID = config['EXTERNAL_DRAIN_CONFIG']['aml_check']['access_id']
    FLOW = config['EXTERNAL_DRAIN_CONFIG']['aml_check']['flow']
else:
    pass


def get_min_check_amount(symbol):
    return float(config['EXTERNAL_DRAIN_CONFIG']['aml_check']['cryptos'][symbol]['min_check_amount'])

def aml_check_transaction(address, txid):
    symbol = 'ETH'
    token_string = f'{txid}:{ACCESS_KEY}:{ACCESS_ID}'
    token = str(hashlib.md5(token_string.encode()).hexdigest())
    payload = f'hash={txid}&address={address}&asset={symbol}&direction=deposit&token={token}&accessId={ACCESS_ID}&locale=en_US&flow={FLOW}'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    response = rq.post(f'{ACCESS_URL}/',headers=headers, data=payload) 
    response.raise_for_status()
    return response.json()


def aml_recheck_transaction(uid, txid):
    token_string = f'{txid}:{ACCESS_KEY}:{ACCESS_ID}'
    token = str(hashlib.md5(token_string.encode()).hexdigest())
    payload = f'uid={uid}&accessId={ACCESS_ID}&token={token}'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    response = rq.post(f'{ACCESS_URL}/recheck',headers=headers, data=payload) 
    response.raise_for_status()
    return response.json()

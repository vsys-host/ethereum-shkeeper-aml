
import decimal
import time
import copy
import requests
import eth_account 
from web3 import Web3, HTTPProvider
from decimal import Decimal

from celery.schedules import crontab
from celery.utils.log import get_task_logger
import requests as rq

from . import celery
from .config import config, get_min_token_transfer_threshold
from .models import Accounts, Transactions, Externaldrains, db
from .encryption import Encryption
from .token import Token, Coin, get_all_accounts
from .unlock_acc import get_account_password
from .utils import skip_if_running
from .aml_bot_api import aml_check_transaction, aml_recheck_transaction

logger = get_task_logger(__name__)

w3 = Web3(HTTPProvider(config["FULLNODE_URL"], request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))

@celery.task()
def make_multipayout(symbol, payout_list, fee):
    if symbol == "ETH":
        coint_inst = Coin(symbol)
        payout_results = coint_inst.make_multipayout_eth(payout_list, fee)
        post_payout_results.delay(payout_results, symbol)
        return payout_results    
    elif symbol in config['TOKENS'][config["CURRENT_ETH_NETWORK"]].keys():
        token_inst = Token(symbol)
        payout_results = token_inst.make_token_multipayout(payout_list, fee)
        post_payout_results.delay(payout_results, symbol)
        return payout_results    
    else:
        return [{"status": "error", 'msg': "Symbol is not in config"}]



@celery.task()
def post_payout_results(data, symbol):
    while True:
        try:
            return requests.post(
                f'http://{config["SHKEEPER_HOST"]}/api/v1/payoutnotify/{symbol}',
                headers={'X-Shkeeper-Backend-Key': config['SHKEEPER_KEY']},
                json=data,
            )
        except Exception as e:
            logger.exception(f'Shkeeper payout notification failed: {e}')
            time.sleep(10)


@celery.task()
def walletnotify_shkeeper(symbol, txid):
    while True:
        try:
            r = rq.post(
                    f'http://{config["SHKEEPER_HOST"]}/api/v1/walletnotify/{symbol}/{txid}',
                    headers={'X-Shkeeper-Backend-Key': config['SHKEEPER_KEY']}
                )
            return r
        except Exception as e:
            logger.warning(f'Shkeeper notification failed for {symbol}/{txid}: {e}')
            time.sleep(10)


@celery.task()
def refresh_balances():
    
    updated = 0

    try:
        from app import create_app
        app = create_app()
        app.app_context().push()

        list_acccounts = get_all_accounts()
        for account in list_acccounts:
            try:
                pd = Accounts.query.filter_by(address = account).first()
            except:
                db.session.rollback()
                raise Exception(f"There was exception during query to the database, try again later")

            acc_balance = decimal.Decimal(w3.fromWei(w3.eth.get_balance(account), "ether"))
            if Accounts.query.filter_by(address = account, crypto = "ETH").first():
                pd = Accounts.query.filter_by(address = account, crypto = "ETH").first()            
                pd.amount = decimal.Decimal(w3.fromWei(w3.eth.get_balance(account), "ether"))                     
                with app.app_context():
                    db.session.add(pd)
                    db.session.commit()
                    db.session.close()
            
            have_tokens = False
                
            for token in config['TOKENS'][config["CURRENT_ETH_NETWORK"]].keys():
                token_instance = Token(token)
                if Accounts.query.filter_by(address = account, crypto = token).first():
                    pd = Accounts.query.filter_by(address = account, crypto = token).first()
                    balance = decimal.Decimal(token_instance.contract.functions.balanceOf(w3.toChecksumAddress(account)).call())
                    normalized_balance = balance / decimal.Decimal(10** (token_instance.contract.functions.decimals().call()))
                    pd.amount = normalized_balance
                    
                    with app.app_context():
                        db.session.add(pd)
                        db.session.commit() 
                        db.session.close()  
                    if normalized_balance >= decimal.Decimal(get_min_token_transfer_threshold(token)):
                        have_tokens = copy.deepcopy(token)
                    
            if have_tokens in config['TOKENS'][config["CURRENT_ETH_NETWORK"]].keys():
                if 'EXTERNAL_DRAIN_CONFIG' not in config:
                    drain_account.delay(have_tokens, account) 
                else:
                    if (((config['EXTERNAL_DRAIN_CONFIG']['aml_check']['state'] == 'enabled') and
                        (have_tokens in config['EXTERNAL_DRAIN_CONFIG']['aml_check']['cryptos'].keys()) and
                        account != token_instance.get_fee_deposit_account()) 
                        or 
                        ((config['EXTERNAL_DRAIN_CONFIG']['regular_split']['state'] == 'enabled') and
                        (have_tokens in config['EXTERNAL_DRAIN_CONFIG']['regular_split']['cryptos'].keys()) and
                        account != token_instance.get_fee_deposit_account())):
                        txids =  Transactions.query.filter_by(address = account, crypto = have_tokens).all()
                        if txids: 
                            for tx in txids:
                                external_drain_account.delay(have_tokens, account, tx.tx_id)
                    else:
                        logger.warning(f"{token} not in both methods in EXTERNAL_DRAIN_CONFIG, check config")
            else:
                coin_inst = Coin('ETH')
                if acc_balance >= decimal.Decimal(config['MIN_TRANSFER_THRESHOLD']):
                    if 'EXTERNAL_DRAIN_CONFIG' not in config:
                        drain_account.delay("ETH", account)
                    else:
                        if (((config['EXTERNAL_DRAIN_CONFIG']['aml_check']['state'] == 'enabled') and 
                            ('ETH' in config['EXTERNAL_DRAIN_CONFIG']['aml_check']['cryptos'].keys()) and
                            account != coin_inst.get_fee_deposit_account())
                            or
                            ((config['EXTERNAL_DRAIN_CONFIG']['regular_split']['state'] == 'enabled') and 
                            ('ETH' in config['EXTERNAL_DRAIN_CONFIG']['regular_split']['cryptos'].keys()) and
                            account != coin_inst.get_fee_deposit_account())):
                            logger.warning(f"Found ETH on {account}, draining")
                            txids =  Transactions.query.filter_by(address = account, crypto = 'ETH').all()
                            if txids:
                                for tx in txids:
                                    external_drain_account.delay('ETH', account, txids[-1].tx_id)
                        else:
                            logger.warning(f"{token} not in both methods in EXTERNAL_DRAIN_CONFIG, check config")     
    
            updated = updated + 1                
    
            with app.app_context():
                db.session.add(pd)
                db.session.commit()
                db.session.close()
    finally:
        with app.app_context():
            db.session.remove()
            db.engine.dispose()  
 
    return updated

@celery.task(bind=True)
@skip_if_running
def drain_account(self, symbol, account):
    if config['EXTERNAL_DRAIN_CONFIG']:
        logger.warning('EXTERNAL_DRAIN_CONFIG is in config, drain to main account is disabled.')
        return "Disabled"
    logger.warning(f"Start draining from account {account} crypto {symbol}")
    if symbol == "ETH":
        inst = Coin(symbol)
        destination = inst.get_fee_deposit_account()
        results = inst.drain_account(account, destination)
    elif symbol in config['TOKENS'][config["CURRENT_ETH_NETWORK"]].keys():
        inst = Token(symbol)
        destination = inst.get_fee_deposit_account()
        results = inst.drain_token_account(account, destination)
    else:
        raise Exception(f"Symbol is not in config")
    
    # return "Disabled"



@celery.task(bind=True)
@skip_if_running
def external_drain_account(self, symbol, account, tx_id):
    logger.warning(f"Start external draining from account {account} crypto {symbol}")
    coin_inst = Coin(symbol)
    if account == coin_inst.get_fee_deposit_account():
        logger.warning(f"Cannot external drain from fee-deposit account - {account}, skip ")
        return False
    # return False
    if symbol == "ETH":
        results = coin_inst.external_drain_account(tx_id, account)
        return results
    elif symbol in config['TOKENS'][config["CURRENT_ETH_NETWORK"]].keys():
        inst = Token(symbol)
        results = inst.external_drain_account(tx_id ,account)
        return results
    else:
        raise Exception(f"Symbol is not in config")
    

# @celery.task(bind=True)
# @skip_if_running
# def event_external_drain_account(self, symbol, account, tx_id):
#     time.sleep(320) # check addresses and transactions no earlier than 5 minutes after the first transaction's confirmation to be sure the data is updated in AMLBot.
#     external_drain_account.delay(symbol, account, tx_id)
 

@celery.task(bind=True)
@skip_if_running
def check_transaction(self, symbol, account, txid):
    result = aml_check_transaction(account, txid)
    if (result['result']  and 
        result['data']['status'] == 'pending'and 
        'uid' in result['data'].keys()):
        status = 'rechecking'
        uid = result['data']['uid']
        score = -1
    elif (result['result']  and 
          'riskscore' in result['data'].keys() and
          'uid' in result['data'].keys() and
           result['data']['status'] == 'success'):
        status = 'ready'
        score = result['data']['riskscore']
        uid = result['data']['uid']
    else:
        logger.warning(f'Cannot update the transaction, something wrong - {result}')
        return False
    
    time.sleep(5)
    
    try:

        from app import create_app
        app = create_app()
        app.app_context().push()
        try:
            pd = Transactions.query.filter_by(address = account, tx_id = txid).first()
        except:
            db.session.rollback()
            raise Exception(f"There was exception during query to the database, try again later")
        pd.uid = uid
        pd.score = score
        pd.status = status
        with app.app_context():
            db.session.add(pd)
            db.session.commit()
            db.session.close()  

    finally:
        with app.app_context():
            db.session.remove()
            db.engine.dispose()


    if status == 'ready':
        external_drain_account.delay(symbol, account, txid)    
        return True
    
@celery.task(bind=True)
@skip_if_running
def recheck_transactions(self):
    try:
        from app import create_app
        app = create_app()
        app.app_context().push()
        pd = Transactions.query.filter_by(ttype = 'aml', status = 'rechecking').all()
        pd_pending = Transactions.query.filter_by(ttype = 'aml', status = 'pending').all()
    except:
        db.session.rollback()
        raise Exception(f"There was exception during query to the database, try again later")
    finally:
        with app.app_context():
            db.session.remove()
            db.engine.dispose()  
 
    if pd:
        for tx in pd:
            recheck_transaction.delay(tx.uid, tx.tx_id)
    if pd_pending:
        for tx in pd_pending:
            check_transaction.delay(tx.crypto, tx.address, tx.tx_id)
    return True



@celery.task(bind=True)
@skip_if_running
def recheck_transaction(self, uid, txid ):
    result = aml_recheck_transaction(uid, txid)
    if (result['result'] and 
        result['data']['status'] == 'pending'and 
        'uid' in result['data'].keys()):
        status = 'rechecking'
        uid = result['data']['uid']
        score = -1
    elif (result['result']  and 
          'riskscore' in result['data'].keys() and
          'uid' in result['data'].keys() and
          result['data']['status'] == 'success'):
        status = 'ready'
        score = result['data']['riskscore']
        uid = result['data']['uid']
    else:
        logger.warning(f'Cannot update the transaction, something wrong - {result}')
        return False
    
    # address = result['data']['address']

    try:
        pd = Transactions.query.filter_by( tx_id = txid).first() #address = address
    except:
        db.session.rollback()
        raise Exception(f"There was exception during query to the database, try again later")
    if not pd:
        logger.warning(f'Cannot find tx {txid} in DB')
        return False
    try:
        from app import create_app
        app = create_app()
        app.app_context().push()
        pd.uid = uid
        pd.score = score
        pd.status = status

        if status == 'ready':
            external_drain_account.delay(pd.crypto, pd.address, txid)

        with app.app_context():
            db.session.add(pd)
            db.session.commit()
            db.session.close()  
    finally:
        with app.app_context():
            db.session.remove()
            db.engine.dispose()


# @celery.task(bind=True)
# @skip_if_running
# def recheck_drainings(self):
#     try:
#         from app import create_app
#         app = create_app()
#         app.app_context().push()
#         pd_tr = Transactions.query.filter(Transactions.status != 'drained', Transactions.status != 'skipped').all()
#         pd_ex = Externaldrains.query.filter_by(status = 'pending').all()

#         for transaction in pd_tr:
#             ex_drain_list = []
#             ex_sum = Decimal(0)
#             for ex_drain in pd_ex:
#                 if transaction.tx_id == ex_drain.tx_id:
#                     ex_drain_list.append(ex_drain)
#                     ex_sum = ex_sum + Decimal(ex_drain.amount_calc)
#             if ex_sum == Decimal(transaction.amount):

#     except:
#         db.session.rollback()
#         raise Exception(f"There was exception during query to the database, try again later")
#     finally:
#         with app.app_context():
#             db.session.remove()
#             db.engine.dispose()  


@celery.task(bind=True)
@skip_if_running
def move_accounts_to_db(self):
    inst = Coin("ETH")
    while not get_account_password():
        logger.warning("Cannot get account password, retry later")
        time.sleep(60)
    account_pass = get_account_password()
    logger.warning(f"Start moving accounts from files to DB")
    r = requests.get('http://'+config["ETHEREUM_HOST"]+':8081',  
                    headers={'X-Shkeeper-Backend-Key': config["SHKEEPER_KEY"]})
    key_list = r.text.split("href=\"")
    for key in key_list:
        if "UTC-" in key:
            geth_key=requests.get('http://'+config["ETHEREUM_HOST"]+':8081/'+str(key.split('>')[0][:-1]), 
                                headers={'X-Shkeeper-Backend-Key': config["SHKEEPER_KEY"]}).json(parse_float=Decimal)
            decrypted_key = eth_account.Account.decrypt(geth_key, account_pass)
            account = eth_account.Account.from_key(decrypted_key)
            inst.save_wallet_to_db(account)
            logger.info(f'Added new wallet added to DB')
  
    return True


@celery.task(bind=True)
@skip_if_running
def create_fee_deposit_account(self):
    logger.warning(f"Creating fee-deposit account")
    inst = Coin("ETH")
    inst.set_fee_deposit_account()    
    return True
        


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # sender.add_periodic_task(
    #     crontab(hour=0, minute=0),
    #     transfer_unused_fee.s(),
    # )

    # Update cached account balances
    sender.add_periodic_task(int(config['UPDATE_TOKEN_BALANCES_EVERY_SECONDS']), refresh_balances.s())
    if config['EXTERNAL_DRAIN_CONFIG']:
        sender.add_periodic_task(int(config['RECHECK_TXS_EVERY_SECONDS']), recheck_transactions.s())



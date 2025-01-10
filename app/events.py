from collections import defaultdict
import time
from decimal import Decimal

from web3 import Web3, HTTPProvider

from .models import Settings, db, Wallets, Accounts, Transactions
from .config import config, get_contract_abi, get_contract_address
from .logging import logger
from .token import Token, Coin, get_all_accounts, get_external_drain_type
from .aml_bot_api import get_min_check_amount, aml_check_transaction


w3 = Web3(HTTPProvider(config["FULLNODE_URL"], request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))

def handle_event(transaction):        
    logger.info(f'new transaction: {transaction!r}')


# def add_transaction_to_db(hash, account, amount, symbol):
#     logger.info('Adding tx to DB')
#     drain_type = get_external_drain_type(symbol)
#     status = ''
#     if not drain_type:
#         logger.warning("Drain type is False, giving up")
#         return False
#     elif drain_type == 'aml':
#         if float(amount) > float(get_min_check_amount(symbol)):
#             aml_check_transaction(account, hash)
#         else:
#             logger.warning('Transaction amount is lower than min check amount in config. Adding it with max score')
#             score = 1

#         pass
#     elif drain_type == 'regular':
#         status = 'regular'
#         score = -1
#     else:
#         logger.warning("Type is undefined")
#         return False


#     from app import create_app
#     app = create_app()
#     app.app_context().push()

#     with app.app_context():
#         db.session.add(Transactions(tx_id = hash,
#                                     status = status,
#                                     crypto = symbol,
#                                     score = score,
#                                     amount = amount,
#                                     address = account))

#         db.session.commit()
#         db.session.close() 



def log_loop(last_checked_block, check_interval):
    from .tasks import walletnotify_shkeeper, drain_account, external_drain_account, check_transaction
    from app import create_app
    app = create_app()
    app.app_context().push()

    coin_inst = Coin('ETH')


    def add_transaction_to_db(hash, account, amount, symbol, internal_type=False):
        logger.info('Adding tx to DB')
        drain_type = get_external_drain_type(symbol)
        status = ''
        if internal_type:
            if internal_type == "from_fee":
                ttype = 'from_fee'
                status = 'skipped'
                score = -1
        elif not drain_type:
            logger.warning("Drain type is False, giving up")
            return False
        elif drain_type == 'aml':
            if float(amount) > float(get_min_check_amount(symbol)):
                check_transaction.delay(symbol, account, hash)
                ttype = 'aml'
                status = 'pending'
                score = -1
            else:
                logger.warning('Transaction amount is lower than min check amount in config. Adding it with max score')
                ttype = 'aml'
                status = 'ready'
                score = 1
        elif drain_type == 'regular':
            ttype = 'regular'
            status = 'pending'
            score = -1
        else:
            logger.warning("Type is undefined")
            return False
        
        from app import create_app
        app = create_app()
        app.app_context().push()

        try:
    
            with app.app_context():
                db.session.add(Transactions(tx_id = hash,
                                            status = status,
                                            ttype = ttype,
                                            crypto = symbol,
                                            score = score,
                                            amount = amount,
                                            address = account))
        
                db.session.commit()
                db.session.close() 
        except:
            with app.app_context():
                db.session.remove()
                db.session.commit()
                db.session.close() 
     



    while True:       
        
        last_block =  w3.eth.block_number
        if last_checked_block == '' or last_checked_block is None:
            last_checked_block = last_block

        if last_checked_block > last_block:
            logger.exception(f'Last checked block {last_checked_block} is bigger than last block {last_block} in blockchain')
        elif last_checked_block == last_block - 2:
            pass
        else:      
            list_accounts = set(get_all_accounts()) 
            for x in range(last_checked_block + 1, last_block):
                logger.warning(f"now checking block {x}")                
                block = w3.eth.getBlock(x, True)       
                for transaction in block.transactions:
                    if transaction['to'] in list_accounts or transaction['from'] in list_accounts:
                        handle_event(transaction)
                        walletnotify_shkeeper.delay('ETH', transaction['hash'].hex())
                        if ((transaction['to'] in list_accounts and 
                             transaction['from']  not in list_accounts) and 
                            transaction['to'] != coin_inst.get_fee_deposit_account()):
                            if 'EXTERNAL_DRAIN_CONFIG' not in config:
                                drain_account.delay('ETH', transaction['to'])
                            else:
                                add_transaction_to_db(transaction['hash'].hex(), 
                                                      transaction['to'], 
                                                      w3.fromWei(transaction["value"], "ether"), 
                                                      'ETH')
                            if ((w3.eth.block_number - x) < 40) and (config['EXTERNAL_DRAIN_CONFIG']):
                               # check addresses and transactions no earlier than 5 minutes after the first transaction's confirmation to be sure the data is updated in AMLBot.
                               external_drain_account.apply_async(args=['ETH', transaction['to'], transaction['hash'].hex()], countdown=320)
                        elif ((transaction['to'] in list_accounts and 
                             transaction['from'] == coin_inst.get_fee_deposit_account()) and
                             config['EXTERNAL_DRAIN_CONFIG']):
                            add_transaction_to_db(transaction['hash'].hex(), 
                                                  transaction['to'], 
                                                  w3.fromWei(transaction["value"], "ether"), 
                                                  'ETH', "from_fee")
                
                for token in config['TOKENS'][config["CURRENT_ETH_NETWORK"]].keys():
                    token_instance  = Token(token)
                    transfers = token_instance.get_all_transfers(x, x)
                    for transaction in transfers:
                        if (token_instance.provider.toChecksumAddress(transaction['from']) in list_accounts or 
                            token_instance.provider.toChecksumAddress(transaction['to']) in list_accounts):
                            handle_event(transaction)
                            walletnotify_shkeeper.delay(token, transaction['txid'])
                            if ((token_instance.provider.toChecksumAddress(transaction['from']) not in list_accounts and 
                                token_instance.provider.toChecksumAddress(transaction['to']) in list_accounts) and 
                                token_instance.provider.toChecksumAddress(transaction['to']) != token_instance.get_fee_deposit_account() and
                                config['EXTERNAL_DRAIN_CONFIG']):
                                tx_amount = Decimal(transaction["amount"]) / Decimal(10** (token_instance.contract.functions.decimals().call()))
                                add_transaction_to_db(transaction['txid'], 
                                                             token_instance.provider.toChecksumAddress(transaction['to']), 
                                                             tx_amount, 
                                                             token_instance.symbol)
                                if ((w3.eth.block_number - x) < 40):
                                    #external_drain_account.delay(token_instance.symbol, transaction['to'], transaction['txid'])
                                    # check addresses and transactions no earlier than 5 minutes after the first transaction's confirmation to be sure the data is updated in AMLBot.
                                    external_drain_account.apply_async(args=[token_instance.symbol, transaction['to'], transaction['txid']], countdown=320)
                                #drain_account.delay(token, token_instance.provider.toChecksumAddress(transaction['to']))
                            elif ((token_instance.provider.toChecksumAddress(transaction['from']) not in list_accounts and 
                                token_instance.provider.toChecksumAddress(transaction['to']) in list_accounts) and 
                                token_instance.provider.toChecksumAddress(transaction['to']) != token_instance.get_fee_deposit_account() and
                                ((w3.eth.block_number - x) < 40) and
                                'EXTERNAL_DRAIN_CONFIG' not in config):
                                drain_account.delay(token, token_instance.provider.toChecksumAddress(transaction['to']))

                
                last_checked_block = x # TODO store this value in database

                pd = Settings.query.filter_by(name = "last_block").first()
                pd.value = x

                with app.app_context():
                    db.session.add(pd)
                    db.session.commit()
                    db.session.close()
    
        time.sleep(check_interval)

def events_listener():

    from app import create_app
    app = create_app()
    app.app_context().push()

    if (not Settings.query.filter_by(name = "last_block").first()) and (config['LAST_BLOCK_LOCKED'].lower() != 'true'):
        logger.warning(f"Changing last_block to a last block on a fullnode, because cannot get it in DB")
        with app.app_context():
            db.session.add(Settings(name = "last_block", 
                                         value = w3.eth.block_number))
            db.session.commit()
            db.session.close() 
            db.session.remove()
            db.engine.dispose()
    
    while True:
        try:
            pd = Wallets.query.all()
            pd2 = Accounts.query.all()
            if ((not pd) and pd2) or (config['FORCE_ADD_WALLETS_TO_DB'].lower() == 'true'):
                logger.warning(f"Wallets should be moved to the database. Creating task.")
                from .tasks import move_accounts_to_db
                move_accounts_to_db.delay()
            pd = Settings.query.filter_by(name = "last_block").first()
            last_checked_block = int(pd.value)

            log_loop(last_checked_block, int(config["CHECK_NEW_BLOCK_EVERY_SECONDS"]))
        except Exception as e:
            sleep_sec = 60
            logger.exception(f"Exception in main block scanner loop: {e}")
            logger.warning(f"Waiting {sleep_sec} seconds before retry.")           
            time.sleep(sleep_sec)



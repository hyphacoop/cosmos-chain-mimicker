"""
Mimics transactions from one chain into another.
See config.toml for required settings.
"""
import copy
import logging
import json
import asyncio
import websockets
import random
import toml
import os
from prometheus_client import start_http_server, Counter, Gauge
import threading
from src import utils, utils_cli, utils_tx

class ChainMimicker():
    def __init__(self, config_file: str='config.toml'):
        self.LEADER_RPC_ENDPOINT=''
        self.FOLLOWER_RPC_ENDPOINT=''
        self.BINARY='gaiad'
        self.DENOM = ''
        self.SIGNERS=[]
        self.RECIPIENT=''
        self.IBC = {
            'recipient': '',
            'channel': ''
        }
        self.WASM = {
            'contract': '',
            'command': {}
        }
        self.AUTHZ_GRANTS={}
        
        self.records_enabled = False
        self.records_filename=''
        self.prometheus_enabled = False
        self.prometheus_port = 8000
        self.records = {
            'total_messages': 0,
            'total_transactions': 0,
            'messages': {}
        }
        logging.basicConfig(
            filename=None,
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.load_config(config_file)
        
        self.tx_hashes = []

        self.counter_messages = Counter('messages', 'Total processed messages', ['msg_type'])
        self.counter_transactions = Counter('transactions', 'Total processed transactions')
        self.counter_gas = Counter('gas_total', 'Total consumed gas')
        self.gauge_signer_balances = Gauge('signer_balances', f'Signer balances in {self.DENOM}', ['address'])
        

        if self.records_enabled:
            self.load_records()

        if self.prometheus_enabled:
            threading.Thread(target=self.start_metrics_server, daemon=True).start()
        asyncio.run(self.monitor_block())


    def start_metrics_server(self):
        start_http_server(self.prometheus_port)

    def load_config(self, filename):
        """
        Populate settings
        """
        config = toml.load(filename)
        self.LEADER_RPC_ENDPOINT = config['leader_rpc_endpoint']
        self.FOLLOWER_RPC_ENDPOINT = config['follower_rpc_endpoint']
        self.records_filename = config['records_filename']
        self.records_enabled = config['save_records']

        self.BINARY=config['binary']
        self.DENOM=config['denom']
        self.SIGNERS = config['signers']
        self.IBC['recipient'] = config['ibc']['recipient']
        self.IBC['channel'] = config['ibc']['channel']
        self.WASM['contract'] = config['wasm']['contract']
        self.WASM['command'] = json.loads(config['wasm']['command'])
        # for index, grantee in enumerate(config['grantees']):
        #     self.AUTHZ_GRANTS[grantee] = config['granters'][index]
        self.AUTHZ_GRANTS = config['authz']
        self.prometheus_enabled = config['prometheus']['enable']
        self.prometheus_port = config['prometheus']['port']

    def load_records(self):
        """
        Populate records dictionary
        """
        if os.path.exists(self.records_filename):
            with open(self.records_filename, 'r') as infile:
                self.records = json.load(infile)
        else:
            self.save_records()
    
    def save_records(self):
        """
        Save records dictionary
        """
        with open(self.records_filename, 'w') as outfile:
            json.dump(self.records, outfile, indent = 4)


    def get_leader_block(self, height):
        """
        Returns a dictionary with the block height and a list of transactions.
        """
        if height == 0:
            height = int(utils.get_current_height(self.LEADER_RPC_ENDPOINT))
        txs = utils_cli.txs_query(urlRPC=self.LEADER_RPC_ENDPOINT, query=f"tx.height={height}", binary=self.BINARY)
        total_count = int(txs['total_count'])
        block = {'height': height, 'txs': []}
        if total_count > 0:
            for tx in txs['txs']:
                tx_entry = {'msgs': []}
                for message in tx['tx']['body']['messages']:
                    if '@type' in message:
                        msg_type = message['@type']
                        tx_entry['msgs'].append(msg_type)
                block['txs'].append(tx_entry)
        return block
    
    def tx_transform(self, tx, signer, height: int=0):
        """
        Returns a transaction assembled with the message types from the provided transaction.
        """
        ttx = None
        msgs = []
        for message in tx['msgs']:
            if '/cosmos.bank.v1beta1.MsgSend' in message:
                msg = utils_tx.send_message_json(
                    sender=signer,
                    recipient=self.SIGNERS[random.randint(0, len(self.SIGNERS) - 1)],
                    amount=1,
                    denom=self.DENOM
                )
                msgs.append(msg)
            elif '/cosmos.bank.v1beta1.MsgMultiSend' in message:
                msg = utils_tx.multisend_message_json(
                    sender=signer,
                    recipients=self.SIGNERS,
                    amount=1,
                    denom=self.DENOM
                )
                msgs.append(msg)
            elif '/cosmos.staking.v1beta1.MsgDelegate' in message:
                # Find a validator to delegate to
                validators = utils_cli.staking_validators_bonded(self.FOLLOWER_RPC_ENDPOINT, binary=self.BINARY)
                val = validators[random.randint(0, len(validators) - 1)]['operator_address']
                msg = utils_tx.delegate_message_json(
                    del_addr=signer,
                    val_addr=val,
                    amount=5,
                    denom=self.DENOM
                )
                msgs.append(msg)
            elif '/cosmos.staking.v1beta1.MsgBeginRedelegate' in message:
                # Check if the redelegation entries have reached the limit
                max_entries=int(utils_cli.staking_params(self.FOLLOWER_RPC_ENDPOINT, binary=self.BINARY)['max_entries'])
                validators = utils_cli.staking_validators_bonded(self.FOLLOWER_RPC_ENDPOINT, binary=self.BINARY)
                srcval = validators[random.randint(0, len(validators) - 1)]
                src = srcval['operator_address']
                if not utils_cli.staking_delegation(self.FOLLOWER_RPC_ENDPOINT, signer, src, binary=self.BINARY):
                    return

                validators.remove(srcval)
                dst = validators[random.randint(0, len(validators) - 1)]['operator_address']
                redels = utils_cli.staking_redelegations(self.FOLLOWER_RPC_ENDPOINT,signer,src=src,dst=dst, binary=self.BINARY)
                if redels and 'entries' in redels:
                    entries=redels['entries']
                    if len(entries) < max_entries:
                        msg = utils.tx_redelegate_message_json(
                            del_addr=signer,
                            src_addr=src,
                            dst_addr=dst,
                            amount=1,
                            denom=self.DENOM
                        )
                        msgs.append(msg)
                        return
                else:
                    msg = utils_tx.redelegate_message_json(
                        del_addr=signer,
                        src_addr=src,
                        dst_addr=dst,
                        amount=1,
                        denom=self.DENOM
                    )
                    msgs.append(msg)
                    return
                logging.info(f"{message} skipped: max entries reached.")
            elif '/cosmos.staking.v1beta1.MsgUndelegate' in message:
                # Check if the unbonding delegation entries have reached the limit
                max_entries=int(utils_cli.staking_params(self.FOLLOWER_RPC_ENDPOINT, binary=self.BINARY)['max_entries'])
                validators = utils_cli.staking_validators_bonded(self.FOLLOWER_RPC_ENDPOINT, binary=self.BINARY)
                val = validators[random.randint(0, len(validators) - 1)]['operator_address']
                if not utils_cli.staking_delegation(self.FOLLOWER_RPC_ENDPOINT, signer, val, binary=self.BINARY):
                    return
                
                unbondings = utils_cli.staking_unbonding(
                    self.FOLLOWER_RPC_ENDPOINT,
                    signer,
                    val,
                    binary=self.BINARY
                )
                if 'entries' in unbondings:
                    entries = unbondings['entries']
                    if len(entries) < max_entries:
                        msg = utils.undelegate_message_json(
                            del_addr=signer,
                            val_addr=VALIDATORS[0],
                            amount=1,
                            denom=self.DENOM
                        )
                        msgs.append(msg)
                        return
                logging.info(f"{message} skipped: max entries reached.")
            elif '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward' in message:
                validators = utils_cli.staking_validators_bonded(self.FOLLOWER_RPC_ENDPOINT, binary=self.BINARY)
                val = validators[random.randint(0, len(validators) - 1)]['operator_address']
                if not utils_cli.staking_delegation(self.FOLLOWER_RPC_ENDPOINT, signer, val, binary=self.BINARY):
                    return
                
                msg = utils_tx.withdraw_reward_message_json(
                    del_addr=signer,
                    val_addr=val
                )
                msgs.append(msg)
            elif '/cosmos.gov.v1beta1.MsgVote' in message or '/cosmos.gov.v1.MsgVote' in message:
                # Get proposals in voting period
                proposals = utils_cli.gov_proposals(
                    urlRPC=self.FOLLOWER_RPC_ENDPOINT,
                    status='voting-period',
                    binary=self.BINARY
                )
                if proposals:
                    logging.info(f"Proposals in voting period: {proposals}")
                    proposal_id = proposals[0]['id']
                    msg = utils.vote_message_json(
                        voter=signer,
                        proposal=proposal_id
                    )
                    msgs.append(msg)
                else:
                    logging.info(f"{message} skipped: no proposals in voting period.")
            elif '/cosmwasm.wasm.v1.MsgExecuteContract' in message:
                msg = utils_tx.wasm_execute_message_json(
                    sender=signer,
                    contract=self.WASM['contract'],
                    msg=self.WASM['command']
                )
                msgs.append(msg)
            elif '/cosmos.authz.v1beta1.MsgExec' in message:
                # logging.info(f"Processing authz message. Signer: {signer}, grantee: {signer}, delegator: {self.AUTHZ_GRANTS[signer]}")
                validators = utils_cli.staking_validators_bonded(self.FOLLOWER_RPC_ENDPOINT, binary=self.BINARY)
                val1 = validators[random.randint(0, len(validators) - 1)]['operator_address']
                val2 = validators[random.randint(0, len(validators) - 1)]['operator_address']
                val3 = validators[random.randint(0, len(validators) - 1)]['operator_address']
                vals = [val1, val2, val3]
                msg = utils_tx.authz_exec_message_json(
                    grantee=signer,
                    msgs=[utils_tx.withdraw_reward_message_json(self.AUTHZ_GRANTS[signer], val_addr) for val_addr in vals]
                )
                msgs.append(msg)
            elif '/gaia.liquid.v1beta1.MsgTokenizeShare' in message:
                validators = utils_cli.staking_validators_bonded(self.FOLLOWER_RPC_ENDPOINT, binary=self.BINARY)
                val = validators[random.randint(0, len(validators) - 1)]['operator_address']
                if not utils_cli.staking_delegation(self.FOLLOWER_RPC_ENDPOINT, signer, val, binary=self.BINARY):
                    return
                
                msg = utils_tx.liquid_tokenize_message_json(
                    delegator=signer,
                    validator=val,
                    owner=signer,
                    amount=2
                )
                msgs.append(msg)
            elif '/gaia.liquid.v1beta1.MsgRedeemTokensForShares' in message: 
                for balance in utils_cli.bank_balances(urlRPC=self.FOLLOWER_RPC_ENDPOINT, wallet=signer, binary=self.BINARY):
                    if 'cosmosvaloper' in balance['denom']:
                        msg = utils_tx.liquid_redeem_message_json(
                            delegator=signer,
                            denom=balance['denom'],
                            amount=balance['amount']
                        )
                        msgs.append(msg)
                        return
                logging.info(f"{message} skipped: signer account has no liquid tokens")
            elif '/gaia.liquid.v1beta1.MsgWithdrawAllTokenizeShareRecordReward' in message: 
                logging.info(f"Skipping unsupported message type: {message}")
            elif '/ibc.applications.transfer.v1.MsgTransfer' in message:
                msg = utils_tx.ibc_transfer_message_json(
                    sender=signer,
                    receiver=self.IBC['recipient'],
                    channel=self.IBC['channel'],
                    amount=1,
                    denom=self.DENOM
                )
                msgs.append(msg)
            elif 'cosmos.distribution.v1beta1.MsgWithdrawValidatorCommission' in message \
                or '/ibc.core.channel.v1.MsgAcknowledgement' in message \
                or '/ibc.core.channel.v1.MsgUpdateClient' in message \
                or '/ibc.core.client.v1.MsgUpdateClient' in message \
                or '/ibc.core.channel.v1.MsgRecvPacket' in message \
                or '/cosmos.authz.v1beta1.MsgGrant' in message \
                or '/cosmos.authz.v1beta1.MsgRevoke' in message \
                or '/cosmwasm.wasm.v1.MsgStoreCode' in message \
                or '/cosmwasm.wasm.v1.MsgInstantiateContract' in message:                    
                logging.info(f'{message} skipped: unsupported message type')
            else:
                logging.info(f'{message} skipped: unrecognized message type')
    
        if msgs:
            ttx = utils_tx.transaction_json(
                messages=msgs,
                memo='Mimic mainnet block ' + str(height) if height else 'Mimic mainnet block',
                fee_denom=self.DENOM
            )
        return ttx

    def mimic_transactions(self, height: int):
        """
        Mimic transactions from the leader chain to the follower chain.
        """
        block = self.get_leader_block(height)
        if not block['txs']:
            logging.info("No transactions to mimic.")
            return

        signers_bucket = copy.deepcopy(self.SIGNERS)
        txs = []
        tx_count=0
        msg_count=0
        for tx in block['txs']:
            if not signers_bucket: # No more signers available
                break
            signer = signers_bucket.pop(0)
            transformed_tx = self.tx_transform(tx, signer, block['height'])
            if not transformed_tx:
                continue
            json.dump(transformed_tx, open('unsigned.json', 'w'), indent=2)
            utils_cli.transaction_sign(unsigned_json='unsigned.json', signer=signer, signed_json='signed.json', binary=self.BINARY)
            response = utils_cli.transaction_broadcast(signed_json='signed.json', urlRPC=self.FOLLOWER_RPC_ENDPOINT, binary=self.BINARY)
            if response['code'] != 0:
                logging.info(f"Leader block {height} tx failed: {transformed_tx['body']['messages'][0]['@type']}: {response['raw_log']}")
            else:
                txhash=response['txhash']
                self.tx_hashes.append(txhash)
                msg_count += len(transformed_tx['body']['messages'])
                tx_count += 1
                for msg in transformed_tx['body']['messages']:
                    msg_type = msg['@type']
                    if self.prometheus_enabled:
                        self.counter_messages.labels(msg_type=msg_type).inc()
                    if self.records_enabled:
                        if msg_type in self.records['messages']:
                            self.records['messages'][msg_type] += 1
                        else:
                            self.records['messages'][msg_type] = 1

        # Update Prometheus metrics
        if self.prometheus_enabled:
            self.counter_transactions.inc(tx_count)
        if self.records_enabled:
            self.records['total_messages'] += msg_count
            self.records['total_transactions'] += tx_count
        # logging.info(f"Leader block {height} tx {tx_count} broadcasted") #{transformed_tx['body']['messages'][0]['@type']}")

    def calculate_gas(self):
        """
        Update gas metrics for available tx hashes
        """
        gas_amount = 0
        if not self.tx_hashes:
            return
        for hash in self.tx_hashes:
            hash_data = utils_cli.tx(self.FOLLOWER_RPC_ENDPOINT, hash, binary=self.BINARY)
            if hash_data:
                gas_used = int(hash_data['gas_used'])
                gas_amount += gas_used
        self.tx_hashes.clear()
        self.counter_gas.inc(gas_amount)

    def update_balances(self):
        """
        Update balance metrics for available signers
        """
        for signer in self.SIGNERS:
            balances = utils_cli.bank_balances(self.FOLLOWER_RPC_ENDPOINT, signer, binary=self.BINARY)
            for balance in balances:
                if self.DENOM in balance['denom']:
                    self.gauge_signer_balances.labels(address=signer).set(int(balance['amount']))


    async def monitor_block(self):
        """
        Subscribe to Tendermint RPC websocket NewBlock event to trigger mimic routine.
        """
        WS_NEWBLOCK_SUBSCRIPTION = \
        '{ "jsonrpc": "2.0", "method": "subscribe", \
            "params": ["tm.event=\'NewBlock\'"], "id": 1 }'
        
        ws_url = self.FOLLOWER_RPC_ENDPOINT.replace('http','ws')
        ws_url = ws_url.replace('https', 'wss')
        ws_url = ws_url + '/websocket'
        processing = False

        async for websocket in websockets.connect(ws_url):
            try:
                await websocket.send(WS_NEWBLOCK_SUBSCRIPTION)
                logging.info('Subscribed to NewBlock event.')
                # await websocket.recv()
                while True:
                    ws_data = json.loads(await websocket.recv())
                    if processing: # debounce
                        logging.info('Still processing previous block, skipping NewBlock event')
                        continue
                    processing = True
                    leader_height = int(utils.get_status(self.LEADER_RPC_ENDPOINT)['sync_info']['latest_block_height'])
                    if self.prometheus_enabled:
                        self.calculate_gas()
                        self.update_balances()
                    self.mimic_transactions(leader_height)
                    if self.records_enabled:
                        self.save_records()
                    processing = False
            except websockets.exceptions.ConnectionClosedError as cce:
                logging.info("subscription> Connection Closed Error: ", cce)

"""
Mimics transactions from one chain into another.
See config.toml for required settings.
"""
import copy
import logging
import json
import asyncio
import random
import os
import threading
import websockets
import toml
from prometheus_client import start_http_server, Counter, Gauge
from src import utils, utils_cli, utils_tx


class ChainMimicker():
    """
    Mimics transactions by reviewing each message in a block.
    """

    def __init__(self, config_file: str = 'config.toml'):
        self.config = {
        }
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

        self.counter_messages = Counter(
            'messages', 'Total processed messages', ['msg_type'])
        self.counter_transactions = Counter(
            'transactions', 'Total processed transactions')
        self.counter_gas = Counter('gas_total', 'Total consumed gas')
        self.gauge_signer_balances = Gauge(
            'signer_balances',
            f'Signer balances in {self.config['follower']['denom']}',
            ['address'])

        if self.config['records']['enable']:
            self.load_records()

        if self.config['prometheus']['enable']:
            threading.Thread(target=self.start_metrics_server,
                             daemon=True).start()
        asyncio.run(self.monitor_block())

    def start_metrics_server(self):
        """
        Run prometheus metrics server
        """
        start_http_server(self.config['prometheus']['port'])

    def load_config(self, filename):
        """
        Populate settings
        """
        config = toml.load(filename)
        self.config['leader'] = config['leader']
        self.config['follower'] = config['follower']
        self.config['ibc'] = config['ibc']
        self.config['wasm'] = config['wasm']
        self.config['authz'] = config['authz']
        self.config['records'] = config['records']
        self.config['prometheus'] = config['prometheus']

    def load_records(self):
        """
        Populate records dictionary
        """
        if os.path.exists(self.config['records']['filename']):
            with open(self.config['records']['filename'], 'r', encoding='utf-8') as infile:
                self.records = json.load(infile)
        else:
            self.save_records()

    def save_records(self):
        """
        Save records dictionary
        """
        with open(self.config['records']['filename'], 'w', encoding='utf-8') as outfile:
            json.dump(self.records, outfile, indent=4)

    def get_validator_addresses(self, url_rpc: str, amount: int = 1):
        """
        Return amount of validator addresses requested.
        """
        addresses = []
        validators = utils_cli.staking_validators_bonded(
            url_rpc, binary=self.config['follower']['binary'])
        while len(addresses) < amount:
            val = random.choice(validators)
            addresses.append(val['operator_address'])
            validators.remove(val)
        return addresses

    def get_leader_block(self, height):
        """
        Returns a dictionary with the block height and a list of transactions.
        """
        if height == 0:
            height = int(utils.get_current_height(
                self.config['leader']['rpc']))
        txs = utils_cli.txs_query(
            url_rpc=self.config['leader']['rpc'],
            query=f"tx.height={height}",
            binary=self.config['leader']['binary'])
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

    def tx_transform(self, tx, signer, height: int = 0):
        """
        Returns a transaction assembled with the message types from the provided transaction.
        """
        def handle_msg_send():
            return utils_tx.send_message_json(
                sender=signer,
                recipient=random.choice(self.config['follower']['signers']),
                amount=1,
                denom=self.config['follower']['denom']
            )

        def handle_msg_multisend():
            return utils_tx.multisend_message_json(
                sender=signer,
                recipients=self.config['follower']['signers'],
                amount=1,
                denom=self.config['follower']['denom']
            )

        def handle_msg_delegate():
            val = self.get_validator_addresses(
                self.config['follower']['rpc'])[0]
            return utils_tx.delegate_message_json(
                del_addr=signer,
                val_addr=val,
                amount=5,
                denom=self.config['follower']['denom']
            )

        def handle_msg_redelegate():
            max_entries = int(utils_cli.staking_params(
                self.config['follower']['rpc'],
                binary=self.config['follower']['binary'])['max_entries'])
            vals = self.get_validator_addresses(
                self.config['follower']['rpc'], amount=2)
            src = vals[0]
            dst = vals[1]
            if not utils_cli.staking_delegation(
                    self.config['follower']['rpc'],
                    signer,
                    src,
                    binary=self.config['follower']['binary']):
                return None
            redels = utils_cli.staking_redelegations(
                self.config['follower']['rpc'],
                signer,
                src=src,
                dst=dst,
                binary=self.config['follower']['binary'])
            if not redels:
                return None
            if 'entries' in redels and len(redels['entries']) >= max_entries:
                return None
            return utils_tx.redelegate_message_json(
                del_addr=signer,
                src_addr=src,
                dst_addr=dst,
                amount=1,
                denom=self.config['follower']['denom']
            )

        def handle_msg_undelegate():
            max_entries = int(utils_cli.staking_params(
                self.config['follower']['rpc'],
                binary=self.config['follower']['binary'])['max_entries'])
            val = self.get_validator_addresses(
                self.config['follower']['rpc'])[0]
            if not utils_cli.staking_delegation(
                    self.config['follower']['rpc'],
                    signer,
                    val,
                    binary=self.config['follower']['binary']):
                return None
            unbondings = utils_cli.staking_unbonding(
                self.config['follower']['rpc'],
                signer,
                val,
                binary=self.config['follower']['binary']
            )
            if 'entries' in unbondings and len(unbondings['entries']) >= max_entries:
                return None
            return utils_tx.undelegate_message_json(
                del_addr=signer,
                val_addr=val,
                amount=1,
                denom=self.config['follower']['denom']
            )

        def handle_msg_withdraw_reward():
            val = self.get_validator_addresses(
                self.config['follower']['rpc'])[0]
            if not utils_cli.staking_delegation(
                    self.config['follower']['rpc'],
                    signer,
                    val,
                    binary=self.config['follower']['binary']):
                return None
            return utils_tx.withdraw_reward_message_json(
                del_addr=signer,
                val_addr=val
            )

        def handle_msg_vote():
            proposals = utils_cli.gov_proposals(
                url_rpc=self.config['follower']['rpc'],
                status='voting-period',
                binary=self.config['follower']['binary']
            )
            if not proposals:
                logging.info(
                    '%s skipped: no proposals in voting period.', message)
                return None
            logging.info('Proposals in voting period: %s', proposals)
            return utils_tx.vote_message_json(
                voter=signer,
                proposal=proposals[0]['id']
            )

        def handle_msg_wasm_execute():
            return utils_tx.wasm_execute_message_json(
                sender=signer,
                contract=self.config['wasm']['contract'],
                msg=self.config['wasm']['command'],
                funds=[]
            )

        def handle_msg_authz_exec():
            vals = self.get_validator_addresses(
                self.config['follower']['rpc'], random.randint(1, 5))
            return utils_tx.authz_exec_message_json(
                grantee=signer,
                msgs=[utils_tx.withdraw_reward_message_json(
                    self.config['authz'][signer], val_addr) for val_addr in vals]
            )

        def handle_msg_liquid_tokenize():
            val = self.get_validator_addresses(
                self.config['follower']['rpc'])[0]
            if not utils_cli.staking_delegation(
                    self.config['follower']['rpc'],
                    signer,
                    val,
                    binary=self.config['follower']['binary']):
                return None

            return utils_tx.liquid_tokenize_message_json(
                delegator=signer,
                validator=val,
                owner=signer,
                amount=2
            )

        def handle_msg_liquid_redeem():
            for balance in utils_cli.bank_balances(
                    url_rpc=self.config['follower']['rpc'],
                    wallet=signer,
                    binary=self.config['follower']['binary']):
                if 'cosmosvaloper' in balance['denom']:
                    return utils_tx.liquid_redeem_message_json(
                        delegator=signer,
                        denom=balance['denom'],
                        amount=balance['amount']
                    )
            return None

        def handle_msg_ibc_transfer():
            ibc_recipient = {
                'receiver': self.config['ibc']['recipient'],
                'channel': self.config['ibc']['channel']
            }
            return utils_tx.ibc_transfer_message_json(
                sender=signer,
                recipient=ibc_recipient,
                amount=1,
                denom=self.config['follower']['denom']
            )
        dispatch = {
            '/cosmos.bank.v1beta1.MsgSend': handle_msg_send,
            '/cosmos.bank.v1beta1.MsgMultiSend': handle_msg_multisend,
            '/cosmos.staking.v1beta1.MsgDelegate': handle_msg_delegate,
            '/cosmos.staking.v1beta1.MsgBeginRedelegate': handle_msg_redelegate,
            '/cosmos.staking.v1beta1.MsgUndelegate': handle_msg_undelegate,
            '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward': handle_msg_withdraw_reward,
            '/cosmos.gov.v1beta1.MsgVote': handle_msg_vote,
            '/cosmos.gov.v1.MsgVote': handle_msg_vote,
            '/cosmwasm.wasm.v1.MsgExecuteContract': handle_msg_wasm_execute,
            '/cosmos.authz.v1beta1.MsgExec': handle_msg_authz_exec,
            '/gaia.liquid.v1beta1.MsgTokenizeShare': handle_msg_liquid_tokenize,
            '/gaia.liquid.v1beta1.MsgRedeemTokensForShares': handle_msg_liquid_redeem,
            '/ibc.applications.transfer.v1.MsgTransfer': handle_msg_ibc_transfer
        }

        msgs = []
        for message in tx['msgs']:
            handler = dispatch.get(message)
            if handler:
                msg = handler()
                if msg:
                    msgs.append(msg)
            elif message not in dispatch:
                logging.info('%s skipped: unsupported message type', message)
            else:
                logging.info('%s skipped: unrecognized message type', message)

        if msgs:
            return utils_tx.transaction_json(
                messages=msgs,
                memo=f'Mimic mainnet block {height}' if height else 'Mimic mainnet block',
                fee_denom=self.config['follower']['denom']
            )
        return None

    def mimic_transactions(self, height: int):
        """
        Mimic transactions from the leader chain to the follower chain.
        """
        block = self.get_leader_block(height)
        if not block['txs']:
            logging.info("No transactions to mimic.")
            return

        signers_bucket = copy.deepcopy(self.config['follower']['signers'])
        tx_count = 0
        msg_count = 0
        for tx in block['txs']:
            if not signers_bucket:  # No more signers available
                break
            signer = signers_bucket.pop(0)
            transformed_tx = self.tx_transform(tx, signer, block['height'])
            if not transformed_tx:
                continue
            with open('unsigned.json', 'w', encoding='utf-8') as outfile:
                json.dump(transformed_tx, outfile, indent=4)
            utils_cli.transaction_sign(
                unsigned_json='unsigned.json',
                signer=signer,
                signed_json='signed.json',
                binary=self.config['follower']['binary'])
            response = utils_cli.transaction_broadcast(
                signed_json='signed.json',
                url_rpc=self.config['follower']['rpc'],
                binary=self.config['follower']['binary'])
            if response['code'] != 0:
                logging.info(
                    'Leader block %s tx failed: %s: %s',
                    height,
                    transformed_tx['body']['messages'][0]['@type'],
                    response['raw_log'])
                continue
            txhash = response['txhash']
            self.tx_hashes.append(txhash)
            msg_count += len(transformed_tx['body']['messages'])
            tx_count += 1
            for msg in transformed_tx['body']['messages']:
                msg_type = msg['@type']
                if self.config['prometheus']['enable']:
                    self.counter_messages.labels(msg_type=msg_type).inc()
                if self.config['records']['enable']:
                    if msg_type in self.records['messages']:
                        self.records['messages'][msg_type] += 1
                    else:
                        self.records['messages'][msg_type] = 1

        # Update Prometheus metrics
        if self.config['prometheus']['enable']:
            self.counter_transactions.inc(tx_count)
        if self.config['records']['enable']:
            self.records['total_messages'] += msg_count
            self.records['total_transactions'] += tx_count

    def calculate_gas(self):
        """
        Update gas metrics for available tx hashes
        """
        gas_amount = 0
        if not self.tx_hashes:
            return
        for tx_hash in self.tx_hashes:
            hash_data = utils_cli.tx(
                self.config['follower']['rpc'], tx_hash, binary=self.config['follower']['binary'])
            if hash_data:
                gas_used = int(hash_data['gas_used'])
                gas_amount += gas_used
        self.tx_hashes.clear()
        self.counter_gas.inc(gas_amount)

    def update_balances(self):
        """
        Update balance metrics for available signers
        """
        for signer in self.config['follower']['signers']:
            balances = utils_cli.bank_balances(
                self.config['follower']['rpc'], signer, binary=self.config['follower']['binary'])
            for balance in balances:
                if self.config['follower']['denom'] in balance['denom']:
                    self.gauge_signer_balances.labels(
                        address=signer).set(int(balance['amount']))

    async def monitor_block(self):
        """
        Subscribe to Tendermint RPC websocket NewBlock event to trigger mimic routine.
        """
        ws_newblock_subscription = \
            '{ "jsonrpc": "2.0", "method": "subscribe", \
            "params": ["tm.event=\'NewBlock\'"], "id": 1 }'

        ws_url = self.config['follower']['rpc'].replace('http', 'ws')
        ws_url = ws_url.replace('https', 'wss')
        ws_url = ws_url + '/websocket'
        processing = False

        async for websocket in websockets.connect(ws_url):
            try:
                await websocket.send(ws_newblock_subscription)
                logging.info('Subscribed to NewBlock event.')
                while True:
                    await websocket.recv()
                    if processing:  # debounce
                        logging.info(
                            'Still processing previous block, skipping NewBlock event')
                        continue
                    processing = True
                    leader_height = int(utils.get_status(self.config['leader']['rpc'])[
                                        'sync_info']['latest_block_height'])
                    if self.config['prometheus']['enable']:
                        self.calculate_gas()
                        self.update_balances()
                    self.mimic_transactions(leader_height)
                    if self.config['records']['enable']:
                        self.save_records()
                    processing = False
            except websockets.exceptions.ConnectionClosedError as cce:
                logging.info('Websockets> Connection Closed Error: %s', cce)

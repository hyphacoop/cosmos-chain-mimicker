"""
Mimics transactions from one chain into another.
See config.toml for required settings.
"""
import copy
import logging
import json
import asyncio
import os
import threading
import websockets
import toml
from prometheus_client import start_http_server, Counter, Gauge
from src import utils, utils_cli, utils_tx, message_adapter


class ChainMimicker():
    """
    Mimics transactions by reviewing each message in a block.
    """

    def __init__(self, config_file: str = 'config.toml'):
        self.config = {
        }
        logging.basicConfig(
            filename=None,
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.load_config(config_file)

        self.counter_messages = Counter(
            'messages', 'Total processed messages', ['msg_type'])
        self.counter_transactions = Counter(
            'transactions', 'Total processed transactions')
        self.counter_gas = Counter('gas_total', 'Total consumed gas')
        self.gauge_signer_balances = Gauge(
            'signer_balances',
            f'Signer balances in {self.config['follower']['denom']}',
            ['address'])

        self.msg_adapter = message_adapter.MessageAdapter(self.config)
        self.records = {
            'total_messages': 0,
            'total_transactions': 0,
            'messages': {},
            'tx_hashes': []
        }

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
        start_http_server(self.config['prometheus']['port'], addr='127.0.0.1')

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

    def tx_transform(self, tx, signer, height):
        """
        Returns a transaction assembled with the message types from the provided transaction.
        """
        msgs = []
        self.msg_adapter.set_signer(signer)
        for message in tx['msgs']:
            msg = self.msg_adapter.adapt(message)
            if msg:
                msgs.append(msg)
        if not msgs:
            return None
        return utils_tx.transaction_json(
            messages=msgs,
            memo=f'Mimic mainnet block {height}' if height else 'Mimic mainnet block',
            fee_denom=self.config['follower']['denom']
        )

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
                # signers_bucket.append(signer)
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
            self.records['tx_hashes'].append(txhash)
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
        if not self.records['tx_hashes']:
            return
        for tx_hash in self.records['tx_hashes']:
            hash_data = utils_cli.tx(
                self.config['follower']['rpc'], tx_hash, binary=self.config['follower']['binary'])
            if hash_data:
                gas_used = int(hash_data['gas_used'])
                gas_amount += gas_used
        self.records['tx_hashes'].clear()
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

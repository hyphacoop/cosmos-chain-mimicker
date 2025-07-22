"""
Converts messages according to the config parameters.
"""

import logging
import random
from src import utils_cli, utils_tx


class MessageAdapter():
    """
    Assembles messages based on their type
    """

    def __init__(self, config: dict):
        self.dispatch_table = {
            '/cosmos.bank.v1beta1.MsgSend': self.handle_send,
            '/cosmos.bank.v1beta1.MsgMultiSend': self.handle_multisend,
            '/cosmos.staking.v1beta1.MsgDelegate': self.handle_delegate,
            '/cosmos.staking.v1beta1.MsgBeginRedelegate': self.handle_redelegate,
            '/cosmos.staking.v1beta1.MsgUndelegate': self.handle_undelegate,
            '/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward': self.handle_withdraw_reward,
            '/cosmos.gov.v1beta1.MsgVote': self.handle_vote,
            '/cosmos.gov.v1.MsgVote': self.handle_vote,
            '/cosmwasm.wasm.v1.MsgExecuteContract': self.handle_wasm_execute,
            '/cosmos.authz.v1beta1.MsgExec': self.handle_authz_exec,
            '/gaia.liquid.v1beta1.MsgTokenizeShare': self.handle_liquid_tokenize,
            '/gaia.liquid.v1beta1.MsgRedeemTokensForShares': self.handle_liquid_redeem,
            '/ibc.applications.transfer.v1.MsgTransfer': self.handle_ibc_transfer
        }
        self.config = config
        self.signer = ''

    def set_signer(self, new_signer: str):
        """
        Sets the signing account
        """
        self.signer = new_signer

    def get_validator_addresses(self, url_rpc: str, amount: int = 1):
        """
        Return amount of validator addresses requested
        """
        addresses = []
        validators = utils_cli.staking_validators_bonded(
            url_rpc, binary=self.config['follower']['binary'])
        while len(addresses) < amount:
            val = random.choice(validators)
            addresses.append(val['operator_address'])
            validators.remove(val)
        return addresses

    def handle_send(self):
        """
        bank send
        """
        return utils_tx.send_message_json(
            sender=self.signer,
            recipient=random.choice(self.config['follower']['signers']),
            amount=1,
            denom=self.config['follower']['denom']
        )

    def handle_multisend(self):
        """
        bank multi-send
        """
        return utils_tx.multisend_message_json(
            sender=self.signer,
            recipients=self.config['follower']['signers'],
            amount=1,
            denom=self.config['follower']['denom']
        )

    def handle_delegate(self):
        """
        staking delegate
        """
        val = self.get_validator_addresses(
            self.config['follower']['rpc'])[0]
        return utils_tx.delegate_message_json(
            del_addr=self.signer,
            val_addr=val,
            amount=5,
            denom=self.config['follower']['denom']
        )

    def handle_redelegate(self):
        """
        staking redelegate
        """
        max_entries = int(utils_cli.staking_params(
            self.config['follower']['rpc'],
            binary=self.config['follower']['binary'])['max_entries'])
        vals = self.get_validator_addresses(
            self.config['follower']['rpc'], amount=2)
        src = vals[0]
        dst = vals[1]
        if not utils_cli.staking_delegation(
                self.config['follower']['rpc'],
                self.signer,
                src,
                binary=self.config['follower']['binary']):
            return None
        redels = utils_cli.staking_redelegations(
            self.config['follower']['rpc'],
            self.signer,
            src=src,
            dst=dst,
            binary=self.config['follower']['binary'])
        if not redels:
            return None
        if 'entries' in redels and len(redels['entries']) >= max_entries:
            return None
        return utils_tx.redelegate_message_json(
            del_addr=self.signer,
            src_addr=src,
            dst_addr=dst,
            amount=1,
            denom=self.config['follower']['denom']
        )

    def handle_undelegate(self):
        """
        staking unbond
        """
        max_entries = int(utils_cli.staking_params(
            self.config['follower']['rpc'],
            binary=self.config['follower']['binary'])['max_entries'])
        val = self.get_validator_addresses(
            self.config['follower']['rpc'])[0]
        if not utils_cli.staking_delegation(
                self.config['follower']['rpc'],
                self.signer,
                val,
                binary=self.config['follower']['binary']):
            return None
        unbondings = utils_cli.staking_unbonding(
            self.config['follower']['rpc'],
            self.signer,
            val,
            binary=self.config['follower']['binary']
        )
        if 'entries' in unbondings and len(unbondings['entries']) >= max_entries:
            return None
        return utils_tx.undelegate_message_json(
            del_addr=self.signer,
            val_addr=val,
            amount=1,
            denom=self.config['follower']['denom']
        )

    def handle_withdraw_reward(self):
        """
        distribution withdraw-rewards
        """
        val = self.get_validator_addresses(
            self.config['follower']['rpc'])[0]
        if not utils_cli.staking_delegation(
                self.config['follower']['rpc'],
                self.signer,
                val,
                binary=self.config['follower']['binary']):
            return None
        return utils_tx.withdraw_reward_message_json(
            del_addr=self.signer,
            val_addr=val
        )

    def handle_vote(self):
        """
        gov vote
        """
        proposals = utils_cli.gov_proposals(
            url_rpc=self.config['follower']['rpc'],
            status='voting-period',
            binary=self.config['follower']['binary']
        )
        if not proposals:
            logging.info(
                '/cosmos.gov.v1beta1.MsgVote skipped: no proposals in voting period.')
            return None
        logging.info('Proposals in voting period: %s', proposals)
        return utils_tx.vote_message_json(
            voter=self.signer,
            proposal=proposals[0]['id']
        )

    def handle_wasm_execute(self):
        """
        wasm execute
        """
        return utils_tx.wasm_execute_message_json(
            sender=self.signer,
            contract=self.config['wasm']['contract'],
            msg=self.config['wasm']['command'],
            funds=[]
        )

    def handle_authz_exec(self):
        """
        authz exec
        """
        vals = self.get_validator_addresses(
            self.config['follower']['rpc'], random.randint(1, 5))
        return utils_tx.authz_exec_message_json(
            grantee=self.signer,
            msgs=[utils_tx.withdraw_reward_message_json(
                self.config['authz'][self.signer], val_addr) for val_addr in vals]
        )

    def handle_liquid_tokenize(self):
        """
        liquid tokenize-share
        """
        val = self.get_validator_addresses(
            self.config['follower']['rpc'])[0]
        if not utils_cli.staking_delegation(
                self.config['follower']['rpc'],
                self.signer,
                val,
                binary=self.config['follower']['binary']):
            return None
        return utils_tx.liquid_tokenize_message_json(
            delegator=self.signer,
            validator=val,
            owner=self.signer,
            amount=2
        )

    def handle_liquid_redeem(self):
        """
        liquid redeem-tokens
        """
        for balance in utils_cli.bank_balances(
                url_rpc=self.config['follower']['rpc'],
                wallet=self.signer,
                binary=self.config['follower']['binary']):
            if 'cosmosvaloper' in balance['denom']:
                return utils_tx.liquid_redeem_message_json(
                    delegator=self.signer,
                    denom=balance['denom'],
                    amount=balance['amount']
                )
        return None

    def handle_ibc_transfer(self):
        """
        ibc-transfer transfer
        """
        ibc_recipient = {
            'receiver': self.config['ibc']['recipient'],
            'channel': self.config['ibc']['channel']
        }
        return utils_tx.ibc_transfer_message_json(
            sender=self.signer,
            recipient=ibc_recipient,
            amount=1,
            denom=self.config['follower']['denom']
        )

    def adapt(self, message):
        """
        Handler dispatch
        """
        handler = self.dispatch_table.get(message)
        if not handler:
            return None
        return handler()

import time

def send_message_json(sender, recipient, amount, denom: str = "uatom"):
    return {
        "@type": "/cosmos.bank.v1beta1.MsgSend",
        "to_address": recipient,
        "from_address": sender,
        "amount": [{"denom": str(denom), "amount": str(amount)}],
    }


def multisend_message_json(sender, recipients, amount, denom: str = "uatom"):
    outputs = []
    for recipient in recipients:
        outputs.append(
            {
                "address": recipient,
                "coins": [{"denom": str(denom), "amount": str(amount)}],
            }
        )
    return {
        "@type": "/cosmos.bank.v1beta1.MsgMultiSend",
        "inputs": [
            {
                "address": sender,
                "coins": [
                    {"denom": str(denom), "amount": str(amount * len(recipients))}
                ],
            }
        ],
        "outputs": outputs
    }


def delegate_message_json(del_addr, val_addr, amount, denom: str = "uatom"):
    return {
        "@type": "/cosmos.staking.v1beta1.MsgDelegate",
        "delegator_address": del_addr,
        "validator_address": val_addr,
        "amount": {"denom": str(denom), "amount": str(amount)},
    }


def redelegate_message_json(
    del_addr, src_addr, dst_addr, amount, denom: str = "uatom"
):
    return {
        "@type": "/cosmos.staking.v1beta1.MsgBeginRedelegate",
        "delegator_address": del_addr,
        "validator_src_address": src_addr,
        "validator_dst_address": dst_addr,
        "amount": {"denom": str(denom), "amount": str(amount)},
    }


def undelegate_message_json(del_addr, val_addr, amount, denom: str = "uatom"):
    return {
        "@type": "/cosmos.staking.v1beta1.MsgUndelegate",
        "delegator_address": del_addr,
        "validator_address": val_addr,
        "amount": {"denom": str(denom), "amount": str(amount)},
    }


def withdraw_reward_message_json(del_addr, val_addr):
    return {
        "@type": "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward",
        "delegator_address": del_addr,
        "validator_address": val_addr,
    }

def vote_message_json(voter: str, proposal: str, option: str = "VOTE_OPTION_YES"):
    return {
        "@type": "/cosmos.gov.v1.MsgVote",
        "proposal_id": proposal,
        "voter": voter,
        "option": option,
        "metadata": ""
    }

def authz_exec_message_json(grantee: str, msgs: list):
    return {
        "@type": "/cosmos.authz.v1beta1.MsgExec",
        "grantee": grantee,
        "msgs": msgs
    }

def liquid_tokenize_message_json(
    delegator: str,
    validator: str,
    owner: str,
    amount: int,
    denom: str = "uatom"):
    return {
        "@type": '/gaia.liquid.v1beta1.MsgTokenizeShares',
        "delegator_address": delegator,
        "validator_address": validator,
        "amount": {
            "denom": denom,
            "amount": str(amount)
        },
        "tokenized_share_owner": owner
    }

def liquid_redeem_message_json(
    delegator: str,
    amount: str,
    denom: str):
    return {
        "@type": '/gaia.liquid.v1beta1.MsgRedeemTokensForShares',
        "delegator_address": delegator,
        "amount": {
            "denom": denom,
            "amount": amount
        }
    }
        

def ibc_transfer_message_json(sender: str, receiver: str, channel: str, amount, denom: str = "uatom", timeout_offset: int=3600000000000):
    return {
        "@type": "/ibc.applications.transfer.v1.MsgTransfer",
        "source_port": "transfer",
        "source_channel": channel,
        "token": {
          "denom": "uatom",
          "amount": "1"
        },
        "sender": sender,
        "receiver": receiver,
        "timeout_height": {
          "revision_number": "0",
          "revision_height": "0"
        },
        "timeout_timestamp": f'{time.time() * 1000000000 + timeout_offset:.0f}',
        "memo": "",
        "encoding": ""

    }

def wasm_execute_message_json(sender: str, contract: str, msg: dict, funds: list=[]):
    return {
        "@type": "/cosmwasm.wasm.v1.MsgExecuteContract",
        "sender": sender,
        "contract": contract,
        "msg": msg,
        "funds": funds
    }

def transaction_json(
    messages: list,
    gas_prices: float = 0.005,
    fee_denom: str = "uatom",
    gas_limit: int = 1000000,
    memo: str = "",
):
    fee_amount = int(gas_limit * gas_prices)
    return {
        "body": {
            "messages": messages,
            "memo": memo,
            "timeout_height": "0",
            "extension_options": [],
            "non_critical_extension_options": [],
        },
        "auth_info": {
            "signer_infos": [],
            "fee": {
                "amount": [{"denom": str(fee_denom), "amount": str(fee_amount)}],
                "gas_limit": str(gas_limit),
                "payer": "",
                "granter": "",
            },
        },
    }


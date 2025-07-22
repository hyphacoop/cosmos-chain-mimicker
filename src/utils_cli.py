"""
Helper functions that require a Cosmos binary in addition to an RPC endpoint
"""

import json
import subprocess


def bank_balances(url_rpc, wallet: str, binary: str = "gaiad"):
    """
    Returns the balances list.
    """
    p = subprocess.run(
        [binary, "q", "bank", "balances", wallet,
            "--output", "json", "--node", url_rpc],
        capture_output=True, check=False
    )
    response = p.stdout.decode("utf-8")
    response_json = json.loads(response)
    return response_json["balances"] if "balances" in response_json else []


def gov_proposals(url_rpc, status: str = 'unspecified', binary: str = "gaiad"):
    """
    Returns the proposals list.
    """
    p = subprocess.run(
        [binary, "q", "gov", "proposals", "--proposal-status",
            status, "--output", "json", "--node", url_rpc],
        capture_output=True, check=False
    )
    response = p.stdout.decode("utf-8")
    response_json = json.loads(response)
    return response_json["proposals"] if "proposals" in response_json else []


def staking_delegation(url_rpc, delegator: str, validator: str, binary: str = 'gaiad'):
    """
    Returns the unbonding delegation entries for the specified addresses.
    """
    p = subprocess.run(
        [binary, "q", "staking", "delegation", delegator,
            validator, "--output", "json", "--node", url_rpc],
        capture_output=True, check=False)
    out = p.stdout.decode("utf-8")
    err = p.stderr.decode("utf-8")
    if 'error' in err:
        return []
    res_json = json.loads(out)
    return res_json["delegation_response"] if "delegation_response" in res_json else []


def staking_params(url_rpc, binary: str = 'gaiad'):
    """
    Returns the staking module params.
    """
    p = subprocess.run(
        [binary, "q", "staking", "params", "--output", "json", "--node", url_rpc],
        capture_output=True, check=False
    )
    response = p.stdout.decode("utf-8")
    response_json = json.loads(response)
    return response_json["params"] if "params" in response_json else []


def staking_redelegations(url_rpc, delegator: str, src: str, dst: str, binary: str = 'gaiad'):
    """
    Returns the redelegation entries for the specified addresses.
    """
    p = subprocess.run(
        [binary, "q", "staking", "redelegation", delegator,
            src, dst, "--output", "json", "--node", url_rpc],
        capture_output=True, check=False
    )
    out = p.stdout.decode("utf-8")
    err = p.stderr.decode("utf-8")
    if 'error' in err:
        return []
    if not out:
        return []
    res_json = json.loads(out)
    return res_json["redelegation_responses"] if "redelegation_responses" in res_json else []


def staking_unbonding(url_rpc, delegator: str, validator: str, binary: str = 'gaiad'):
    """
    Returns the unbonding delegation entries for the specified addresses.
    """
    p = subprocess.run(
        [binary, "q", "staking", "unbonding-delegation", delegator,
            validator, "--output", "json", "--node", url_rpc],
        capture_output=True, check=False
    )
    out = p.stdout.decode("utf-8")
    err = p.stderr.decode("utf-8")
    if 'error' in err:
        return []
    res_json = json.loads(out)
    return res_json["unbond"] if "unbond" in res_json else []


def staking_validators(url_rpc, binary: str = 'gaiad'):
    """
    Returns the list of validators.
    """
    p = subprocess.run(
        [binary, "q", "staking", "validators", "--page-limit",
            "1000", "--output", "json", "--node", url_rpc],
        capture_output=True, check=False
    )
    response = p.stdout.decode("utf-8")
    response_json = json.loads(response)
    return response_json["validators"] if "validators" in response_json else []


def staking_validators_bonded(url_rpc, binary: str = 'gaiad'):
    """
    Returns the list of active validators.
    """
    validators = staking_validators(url_rpc, binary)
    bonded = [val for val in validators if val['status']
              == 'BOND_STATUS_BONDED']
    return bonded if validators else []


def tx(url_rpc, tx_hash: str, binary: str = "gaiad"):
    """
    Returns the tx query response
    """
    p = subprocess.run(
        [binary, "q", "tx", tx_hash, "--output", "json", "--node", url_rpc],
        capture_output=True, check=False)
    out = p.stdout.decode("utf-8")
    err = p.stderr.decode("utf-8")
    if 'error' in err:
        return []
    if not out:
        return []
    res_json = json.loads(out)
    return res_json


def txs_query(url_rpc, query, binary: str = "gaiad"):
    """
    Returns the txs --query response
    """
    p = subprocess.run(
        [binary, "q", "txs", "--query", query,
            "--output", "json", "--node", url_rpc],
        capture_output=True, check=False
    )
    response = p.stdout.decode("utf-8")
    response_json = json.loads(response)
    return response_json


def transaction_sign(
    unsigned_json: str, signer: str, signed_json: str, binary: str = "gaiad"
):
    """
    Signs the transaction with the signer key name.
    """
    subprocess.run(
        [
            binary,
            "tx",
            "sign",
            unsigned_json,
            "--from",
            signer,
            "--output-document",
            signed_json,
        ],
        check=False
    )


def transaction_broadcast(signed_json: str, url_rpc: str, binary: str = "gaiad"):
    """
    Broadcasts the transaction.
    """
    p = subprocess.run([binary,
        "tx",
        "broadcast",
        signed_json,
        "--node",url_rpc,
        "--output", "json"
        ],
        capture_output=True, check=False)
    response = p.stdout.decode("utf-8")
    return json.loads(response)

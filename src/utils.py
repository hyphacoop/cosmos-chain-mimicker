"""
Helper functions that only require an RPC endpoint.
"""

import requests


def get_current_height(url_rpc: str):
    """
    Returns the current block height from the RPC endpoint.
    """
    response = requests.get(f"{url_rpc}/status",timeout=5).json()
    return response["result"]["sync_info"]["latest_block_height"]


def get_status(url_rpc: str):
    """
    Returns the chain status from the RPC endpoint.
    """
    endpoint = f"{url_rpc}/status"
    response = requests.get(endpoint,timeout=5).json()["result"]
    return response

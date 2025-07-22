import json
import requests

def get_current_height(urlRPC: str):
    """
    Returns the current block height from the RPC endpoint.
    """
    response = requests.get(f"{urlRPC}/status").json()
    return response["result"]["sync_info"]["latest_block_height"]

def get_status(urlRPC: str):
    endpoint = f"{urlRPC}/status"
    response = requests.get(endpoint).json()["result"]
    return response

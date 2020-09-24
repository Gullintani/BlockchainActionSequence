# Powered by Etherscan.io APIs
import requests, os
import pandas as pd
import numpy as np


def get_external_transaction(address:str):
    api_link = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}"
    r = eval(requests.get(api_link).text)
    if r['status'] == '1':
        return r['status'], r['result']
    else:
        return r['status'], r['message']

def get_internal_transaction(address:str):
    api_link = f"https://api.etherscan.io/api?module=account&action=txlistinternal&address={address}&startblock=0&endblock=2702578&sort=asc&apikey={api_key}"
    r = eval(requests.get(api_link).text)
    if r['status'] == '1':
        return r['status'], r['result']
    else:
        return r['status'], r['message']


def get_ERC20_transaction(address:str):
    api_link = f"https://api.etherscan.io/api?module=account&action=tokentx&address={address}&startblock=0&endblock=999999999&sort=asc&apikey={api_key}"
    r = eval(requests.get(api_link).text)
    if r['status'] == '1':
        return r['status'], r['result']
    else:
        return r['status'], r['message']

def get_ERC721_transaction(address:str):
    api_link = f"https://api.etherscan.io/api?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999&sort=asc&apikey={api_key}"
    r = eval(requests.get(api_link).text)
    if r['status'] == '1':
        return r['status'], r['result']
    else:
        return r['status'], r['message']

def get_balance(address:str):
    api_link = f"https://api.etherscan.io/api?module=account&action=balance&address={address}&tag=latest&apikey={api_key}"
    r = eval(requests.get(api_link).text)
    if r['status'] == '1':
        return r['status'], r['result']
    else:
        return r['status'], r['message']

def get_mined_block(address:str):
    api_link = f"https://api.etherscan.io/api?module=account&action=getminedblocks&address={address}&blocktype=blocks&apikey={api_key}"
    r = eval(requests.get(api_link).text)
    if r['status'] == '1':
        return r['status'], r['result']
    else:
        return r['status'], r['message']

def collect_transaction(address:str, path:str):
    status, result = get_external_transaction(address)
    if status == '1':
        value_array = np.array([list(row.values()) for row in result])
        df = pd.DataFrame(value_array, columns=external_all)
        df = df[external_select]
        df['tokenName'] = 'NA'
        df['tokenDecimal'] = 'NA'
        df['type'] = 'external'
        df.to_csv(path + address + '.csv')
        print("External Saved")
    else:
        print("get external transaction failed: " + result)

    status, result = get_ERC20_transaction(address)
    if status == '1':
        value_array = np.array([list(row.values()) for row in result])
        df = pd.DataFrame(value_array, columns=erc20_all)
        df = df[erc20_select]
        df['type'] = 'ERC20'
        df.to_csv(path + address + '.csv', mode='a', header=False)
        print("ERC20 Saved")
    else:
        print("get ERC20 transaction failed: " + result)

    status, result = get_ERC721_transaction(address)
    if status == '1':
        value_array = np.array([list(row.values()) for row in result])
        df = pd.DataFrame(value_array, columns=erc721_all)
        df = df[erc721_select]
        df['type'] = 'ERC721'
        df.to_csv(path + address + '.csv', mode='a', header=False)
        print("ERC721 Saved")
    else:
        print("get ERC721 transaction failed: " + result)

    # Sort timeStamp, convert and set as Index
    df = pd.read_csv(path + address + '.csv').sort_values(by='timeStamp')
    df['timeStamp']=pd.to_datetime(df['timeStamp'],unit='s')
    df = df.set_index('timeStamp')
    df.fillna('NA')
    df.to_csv(path + address + '.csv')

if __name__ == '__main__':
    # ======================= Raw Attributes =======================
    external_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'transactionIndex', 'from', 'to', 'value', 'gas', 'gasPrice', 'isError', 'txreceipt_status', 'input', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'confirmations']
    erc20_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'from', 'contractAddress', 'to', 'value', 'tokenName', 'tokenSymbol', 'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'input', 'confirmations']
    erc721_all = ['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'from', 'contractAddress', 'to', 'tokenID', 'tokenName', 'tokenSymbol', 'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'input', 'confirmations']
    # ======================= Selected Attributes =======================
    external_select = ['blockNumber', 'timeStamp', 'from', 'to', 'value', 'gas', 'gasPrice', 'contractAddress', 'cumulativeGasUsed', 'gasUsed'] # tokenName, tokenDecimal
    erc20_select = ['blockNumber', 'timeStamp', 'from', 'to', 'value', 'gas', 'gasPrice', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'tokenName', 'tokenDecimal']
    erc721_select = ['blockNumber', 'timeStamp', 'from', 'to', 'tokenID', 'gas', 'gasPrice', 'contractAddress', 'cumulativeGasUsed', 'gasUsed', 'tokenName', 'tokenDecimal']

    # ======================= Const Variables =======================
    api_key = '39M8BBF53U6M7N2YS92M163RP3RCZF6GUK'

    # ======================= Start Program =======================
    address = '0x7891f796a5d43466fc29f102069092aef497a290'
    path = './data/by_address_raw/'
    collect_transaction(address, path)

    
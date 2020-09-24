import requests, os
import pandas as pd
import numpy as np

def profile(input_path:str, output_path:str, self_address:str):
    df = pd.read_csv(input_path)[['timeStamp', 'blockNumber', 'from', 'to', 'value', 'gas', 'gasPrice', 'contractAddress', 'cumulativeGasUsed',
       'gasUsed', 'tokenName', 'tokenDecimal', 'type', 'from_title', 'to_title', 'from_cate', 'to_cate']]
    store_array=[]

    send_all = len(df.loc[(df['from_title'] == 'self')])
    receive_all = len(df.loc[(df['to_title'] == 'self')])
    send_external = len(df.loc[(df['from_title'] == 'self') & (df['type'] == 'external')])
    receive_external = len(df.loc[(df['to_title'] == 'self') & (df['type'] == 'external')])
    send_ERC20 = len(df.loc[(df['from_title'] == 'self') & (df['type'] == 'ERC20')])
    receive_ERC20 = len(df.loc[(df['to_title'] == 'self') & (df['type'] == 'ERC20')])
    send_ERC721 = len(df.loc[(df['from_title'] == 'self') & (df['type'] == 'ERC721')])
    receive_ERC721 = len(df.loc[(df['to_title'] == 'self') & (df['type'] == 'ERC721')])

    time_list = [pd.to_datetime(x) for x in df['timeStamp'].values]
    first_date = time_list[0]
    last_date = time_list[-1]
    day_between = (time_list[-1] - time_list[0]).days
    daily_transaction = round(len(df)/day_between)

    

    store_array.append([self_address, len(df), send_all, receive_all, send_external, receive_external, send_ERC20, receive_ERC20, send_ERC721, receive_ERC721, daily_transaction, first_date, last_date])

    store_df = pd.DataFrame(np.array(store_array), index = None, columns=["address", "transaction_all", "send_all", "receive_all", "send_external", "receive_external", 
    "send_ERC20", "receive_ERC20", "send_ERC721", "receive_ERC721", "daily_transaction", "first_date", "last_date"]).fillna(0)
    
    store_df.to_csv(output_path)
    # store_df.to_csv(output_path, mode='a', header=False)

    return

if __name__ == '__main__':
    address = '0x7891f796a5d43466fc29f102069092aef497a290'
    input_path = './data/by_address_translated/' + address + '.csv'
    output_path = './data/profile.csv'
    profile(input_path, output_path, address)
import requests, os
import pandas as pd
import numpy as np

# columns in raw file: ['timeStamp', 'Unnamed: 0', 'blockNumber', 'from', 'to', 'value', 'gas', 
# 'gasPrice', 'contractAddress', 'cumulativeGasUsed', 'gasUsed',
# 'tokenName', 'tokenDecimal', 'type']
def match(row, self_address):
    if row[3] == self_address:
        from_title = 'self'
        from_cate = 'self'
        try:
            to_title = translate_df.query("address == @row[4]")['title'].values[0]
            to_cate = translate_df.query("address == @row[4]")['category'].values[0]
            return pd.Series([from_title, to_title, from_cate, to_cate])
        except:
            return pd.Series((from_title, 'unknown', from_cate, 'unknown')).values
            
    elif row[4] == self_address:
        to_title = 'self'
        to_cate = 'self'
        try:
            from_title = translate_df.query("address == @row[3]")['title'].values[0]
            from_cate = translate_df.query("address == @row[3]")['category'].values[0]
            return pd.Series([from_title, to_title, from_cate, to_cate])
        except:
            return pd.Series(('unknown', to_title, 'unknown', to_cate))

    else:
        return pd.Series((row[3], row[4], 'unknown', 'unknown'))

def translate(input_path:str, output_path:str, self_address:str):
    df = pd.read_csv(input_path)
    df[['from_title', 'to_title', 'from_cate', 'to_cate']]= df.apply(lambda row: match(row, self_address), axis=1)
    df.to_csv(output_path)
    return

if __name__ == '__main__':
    # ============ Constant ============
    translate_df = pd.read_csv('./data/database.csv')
    # columns in translate_df: ['Unnamed: 0', 'address', 'id', 'title', 'category', 'author', 'balance', 'contractsCount', 'ranking', 'Unnamed: 0.1']
    
    self_address = '0x7891f796a5d43466fc29f102069092aef497a290'
    input_path = './data/by_address_raw/' + self_address + '.csv'
    output_path = './data/by_address_translated/' + self_address + '.csv'

    translate(input_path, output_path, self_address)
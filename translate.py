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
            to_title, to_cate = tuple(translate_df.query("address == @row[4]")[['title', 'category']].values[0])
            return_list = [from_title, to_title, from_cate, to_cate]
        except:
            return_list = [from_title, 'unknown', from_cate, 'unknown']
            
    elif row[4] == self_address:
        to_title = 'self'
        to_cate = 'self'
        try:
            from_title, from_cate = tuple(translate_df.query("address == @row[3]")[['title', 'category']].values[0])
            return_list = [from_title, to_title, from_cate, to_cate]
        except:
            return_list = ['unknown', to_title, 'unknown', to_cate]

    else:
        return_list = [row[3], row[4], 'unknown', 'unknown']

    try:
        contract_title, contract_cate = tuple(translate_df.query("address == @row[8]")[['title', 'category']].values[0])
        return_list.append(contract_title)
        return_list.append(contract_cate)
        return pd.Series(return_list)
    except:
        return_list.append('NA')
        return_list.append('NA')
        return pd.Series(return_list)

def translate(input_path:str, output_path:str, self_address:str):
    df = pd.read_csv(input_path)
    df = df.fillna('NA')
    df[['from_title', 'to_title', 'from_cate', 'to_cate', 'contract_title', 'contract_cate']]= df.apply(lambda row: match(row, self_address), axis=1)
    df.to_csv(output_path)
    return

if __name__ == '__main__':
    # ============ Constant ============
    translate_df = pd.read_csv('./data/database.csv')
    # columns in translate_df: ['Unnamed: 0', 'address', 'id', 'title', 'category', 'author', 'balance', 'contractsCount', 'ranking', 'Unnamed: 0.1']
    file_names = os.listdir('./data/by_address_raw/')
    total = len(file_names)
    index = 0
    for file_name in file_names:
        try:
            self_address = file_name[:-4]
            input_path = './data/by_address_raw/' + self_address + '.csv'
            output_path = './data/by_address_translated/' + self_address + '.csv'
            translate(input_path, output_path, self_address)
            print(f"{index}/{total}")
            index+=1
        except:
            print("error happened")
            continue
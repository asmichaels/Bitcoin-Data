import requests
import json
import pandas as pd
import bitcoinrpc
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

api_1 = "https://blockstream.info/api/"


#the function used to iterate through every 100 blocks, scraping the data that i need, and 
#storing it in several lists
def blockchain_scrape(start):
    
    #GETS THE TOTAL NUMBER OF BLOCKS MINED UNTIL NOW
    tip_raw = requests.get(api_1 + "blocks/tip/height")
    tip = tip_raw.json()
    
    for height in range(start, tip, 100):
        
        #GET BLOCK HASH
        access = AuthServiceProxy("http://%s:%s@127.0.0.1:8332"%("{USER}", "{PASSWORD}"))
        block_hash = access.getblockhash(height)
        
        #GET 1st TXID IN THIS BLOCK
        txid_raw = requests.get(api_1 + "block/" + block_hash + "/txids")
        txids = txid_raw.json()
        txid_1 = txids[0]
        
        #GET INFO OF TXID#1
        tx_info_raw = requests.get(api_1 + "tx/" + txid_1)
        tx_info = tx_info_raw.json()
        
        #GET CONFIRMATION THAT THIS TX IS COINBASE
        vin = tx_info["vin"]
        is_coinbase = vin[0]["is_coinbase"]
        coinbase_list.append(is_coinbase)
        
        #GET VALUE OF COINBASE TX
        vout = tx_info["vout"]
        v_list = []
        
        for i in range(len(vout)):   #THERE ARE OCCASIONALLY SEVERAL OUTPUT ADDRESSES TO THE COINBASE TRANSACTION.
            value = vout[i]["value"] #THIS CODE ENSURES ALL THOSE OUTPUTS ARE INCLUDED IN THE TOTAL VALUE OF THE BLOCK REWARD
            v_list.append(value)

        total_value = sum(v_list)
        value_list.append(total_value)
        
        #GET BLOCK TIMESTAMP
        status = tx_info["status"]
        timestamp = status["block_time"]
        timestamp_list.append(timestamp)
        
        block_height_list.append(height)
        
        print(height, end = ' ')
        
        
block_height_list = []
timestamp_list = []
coinbase_list = []
value_list = []

#this starts the process from block height 0.
blockchain_scrape(0)


#DATA TO BE SAVED
data = {
    "Block_Height": block_height_list,
    "Block_Time": timestamp_list,
    "Is_Coinbase?": coinbase_list,
    "Value": value_list
       }


#CREATING A PANDAS DATAFRAME FROM THE DATA JUST COLLECTED
df = pd.DataFrame(data, columns = ["Block_Height", "Block_Time", "Is_Coinbase?", "Value"])
df


df.to_csv("file path")




#I FORGOT TO ADD BLOCK HASHES TO THE ABOVE DATASET. NOT A CRUCIAL METRIC, BUT USEFUL TO HAVE IF I NEED TO GO BACK TO THE BLOCKCHAIN
#FOR MORE ANALYSIS

hash_list = []

for height in range(0, tip, 100):
    access = AuthServiceProxy("http://%s:%s@127.0.0.1:8332"%("{USER}", "{PASSWORD}"))
    block_hash = access.getblockhash(height)
    hash_list.append(block_hash)
    
hash_list_dict = {
    "Block_Hash": hash_list
}

hash_list_df = pd.DataFrame(hash_list_dict, columns = ["Block_Hash"])
hash_list_df.to_csv("file path")

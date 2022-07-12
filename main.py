from pycoingecko import CoinGeckoAPI
import json
import sqlite3
import numpy as np
import pandas as pd

cg = CoinGeckoAPI()
SAVE_IN_DB = False
try:
    sqliteConnection = sqlite3.connect('bitcodb.db')
    cursor = sqliteConnection.cursor()
    print("Database created and Successfully Connected")
    query = "DROP TABLE IF EXISTS coins_list;"
    cursor.execute(query)
    query = """
    CREATE TABLE IF NOT EXISTS coins_list (
	id INTEGER PRIMARY KEY,
   	name TEXT NOT NULL,
	symbol TEXT NOT NULL);
    """
    cursor.execute(query)
    query = "DROP TABLE IF EXISTS quarter_data;"
    cursor.execute(query)
    query = """    
    CREATE TABLE IF NOT EXISTS quarter_data (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    vs_currency REAL NOT NULL); 
    """
    cursor.execute(query)
    query = "DROP TABLE IF EXISTS window_data;"
    cursor.execute(query)
    query = """    
    CREATE TABLE IF NOT EXISTS window_data (
    id INTEGER PRIMARY KEY,
    prev_price REAL NOT NULL,
    prev_vs_currency REAL NOT NULL); 
    """
    cursor.execute(query)

    coins = cg.get_coins()

    clean_coins = []
    for coin in coins:
        clean_coins.append({'id': coin['id'], 'name': coin['name'], 'symbol': coin['symbol']})
        if SAVE_IN_DB:
            query = """INSERT INTO coins_list 
            (id, name, symbol) VALUES 
            ({0}, '{1}', '{2}');""".format(coin['id'], coin['name'], coin['symbol'])
            cursor.execute(query)

    json_object = json.dumps(clean_coins, indent = 4)
    with open("coins.json", "w") as outfile:
        outfile.write(json_object)

    bitco = cg.get_coin_market_chart_by_id('bitcoin', 'usd', 210) # Q1 2022
    bitco = bitco['prices'][:90]
    json_object = json.dumps(bitco, indent = 4)
    with open("bitco.json", "w") as outfile:
        outfile.write(json_object)

    #bitcoin_data = {'bitcoin_price': [bitco['market_data']['current_price']['usd']], 'tag': ['Q1 2022']}
    print(bitco)
    if SAVE_IN_DB:
        for bit in bitco:
            query = """INSERT INTO quarter_data 
            (name, price, vs_currency) VALUES 
            ('{0}', {1}, {2});""".format('bitcoin', bit[0], bit[1])
            cursor.execute(query)

    # Spark or Pandas shift
    bitco_dt = pd.DataFrame.from_dict({'prices': bitco})
    bitco_shifted = bitco_dt.shift(5)
    print(bitco_shifted)

    json_object = bitco_shifted[5:].to_json()
    json_object = json.loads(json_object)
    json_object = json_object['prices']
    json_object = json.dumps(json_object, indent = 4)
    with open("bitco_shifted.json", "w") as outfile:
        outfile.write(json_object)

    if SAVE_IN_DB:
        # save shifted in db
        for k, shifted in enumerate(bitco_shifted):
            if k > 5:
                query = """INSERT INTO window_data 
                (prev_price, prev_vs_currency) VALUES 
                ({0}, {1})""".format(shifted[0], shifted[1])
    
    cursor.close()

except sqlite3.Error as error:
    print("Error while connecting to sqlite", error)
finally:
    if sqliteConnection:
        sqliteConnection.close()
        print("The SQLite connection is closed")
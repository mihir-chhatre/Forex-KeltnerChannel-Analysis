import requests
import time
import sqlite3
from pymongo import MongoClient
from datetime import datetime

def get_conversion_rate(pair, api_key):
    url = f"https://api.polygon.io/v1/conversion/{pair[0]}/{pair[1]}?amount=1&precision=4&apiKey={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['converted'], data['last']['timestamp']
    else:
        print(f"Error fetching data for {pair}: {response.status_code}")
        return None, None

def setup_sqlite(currency_pairs):
    conn_aux = sqlite3.connect('AuxiliaryForexData.db')
    conn_final = sqlite3.connect('FinalForexData.db')
    cur_aux = conn_aux.cursor()
    cur_final = conn_final.cursor()
    for pair in currency_pairs:
        table_name_aux = f"{pair[0]}_{pair[1]}"
        table_name_final = f"final_{pair[0]}_{pair[1]}"
        cur_aux.execute(f"CREATE TABLE IF NOT EXISTS {table_name_aux} (fx_rate REAL, fx_timestamp TIMESTAMP, entry_timestamp TIMESTAMP)")
        cur_final.execute(f"CREATE TABLE IF NOT EXISTS {table_name_final} (max_rate REAL, min_rate REAL, mean_rate REAL, vol REAL, fd REAL, data_timestamp TIMESTAMP, entry_timestamp TIMESTAMP)")
    conn_aux.commit()
    conn_final.commit()
    return conn_aux, conn_final

def setup_mongodb(currency_pairs):
    client = MongoClient()
    db_aux = client['AuxiliaryForexData']
    db_final = client['FinalForexData']
    for pair in currency_pairs:
        collection_name_aux = f"{pair[0]}_{pair[1]}"
        collection_name_final = f"final_{pair[0]}_{pair[1]}"
        db_aux[collection_name_aux]
        db_final[collection_name_final] 
    return client, db_aux, db_final


def insert_data_sqlite(conn, pair, rate, fx_timestamp):
    cur = conn.cursor()
    table_name = f"{pair[0]}_{pair[1]}"
    # Convert fx_timestamp from milliseconds to datetime
    fx_timestamp_dt = datetime.fromtimestamp(fx_timestamp / 1000)
    entry_timestamp_dt = datetime.now()
    cur.execute(f"INSERT INTO {table_name} (fx_rate, fx_timestamp, entry_timestamp) VALUES (?, ?, ?)", (rate, fx_timestamp_dt, entry_timestamp_dt))
    conn.commit()

def insert_data_mongodb(collections, pair, rate, fx_timestamp):
    collection_name = f"{pair[0]}_{pair[1]}"
    # Convert fx_timestamp from milliseconds to datetime
    fx_timestamp_dt = datetime.fromtimestamp(fx_timestamp / 1000)
    entry_timestamp_dt = datetime.now()
    collections[collection_name].insert_one({"fx_rate": rate, "fx_timestamp": fx_timestamp_dt, "entry_timestamp": entry_timestamp_dt})

def clear_database_data_sqlite(conn, pair):
    table_name = f"{pair[0]}_{pair[1]}"
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {table_name}")
    conn.commit()
    print("\nDeleted data from", table_name,"\n")

def clear_database_data_mongodb(collections, pair):
    collection_name = f"{pair[0]}_{pair[1]}"
    collections[collection_name].delete_many({})
    print("\nDeleted data from", collection_name, "\n")

def calculate_keltner_channels(mean_rate, vol):
    upper_bands = [(mean_rate + n * 0.025 * vol) for n in range(1, 101)]
    lower_bands = [(mean_rate - n * 0.025 * vol) for n in range(1, 101)]
    return upper_bands, lower_bands


def track_price_jumps_sqlite(conn, pair, bands):
    cur = conn.cursor()
    table_name = f"{pair[0]}_{pair[1]}"
    cur.execute(f"SELECT fx_rate FROM {table_name}")
    rates = [row[0] for row in cur.fetchall()]
    jumps = 0
    upper_bands, lower_bands = bands['upper'], bands['lower']   
    for rate in rates:
        for ub, lb in zip(upper_bands, lower_bands):
            if rate > ub or rate < lb:
                jumps += 1
                break
    return jumps

def track_price_jumps_mongodb(db, pair, bands):
    collection_name = f"{pair[0]}_{pair[1]}"
    collection = db[collection_name]
    rates_cursor = collection.find({}, {'fx_rate': 1, '_id': 0})
    rates = [doc['fx_rate'] for doc in rates_cursor]
    jumps = 0
    upper_bands, lower_bands = bands['upper'], bands['lower']   
    for rate in rates:
        for ub, lb in zip(upper_bands, lower_bands):
            if rate > ub or rate < lb:
                jumps += 1
                break
    return jumps


def update_final_sqlite(conn_final, pair, max_rate, min_rate, mean_rate, vol, fd, first_fx_timestamp):
    table_name = f"final_{pair[0]}_{pair[1]}"
    cur = conn_final.cursor()
    cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (max_rate REAL, min_rate REAL, mean_rate REAL, vol REAL, fd REAL, data_timestamp TIMESTAMP, entry_timestamp TIMESTAMP)")
    cur.execute(f"INSERT INTO {table_name} (max_rate, min_rate, mean_rate, vol, fd, data_timestamp, entry_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                (max_rate, min_rate, mean_rate, vol, fd, datetime.fromtimestamp(first_fx_timestamp / 1000), datetime.now()))
    conn_final.commit()

def update_final_mongodb(db_final, pair, max_rate, min_rate, mean_rate, vol, fd, first_fx_timestamp):
    collection_name = f"final_{pair[0]}_{pair[1]}"
    db_final[collection_name].insert_one({
        "max_rate": max_rate, 
        "min_rate": min_rate, 
        "mean_rate": mean_rate, 
        "vol": vol, 
        "fd": fd, 
        "data_timestamp": datetime.fromtimestamp(first_fx_timestamp / 1000), 
        "entry_timestamp": datetime.now()
    })

def update_statistics(stats, rate):
    stats['sum'] += rate
    stats['count'] += 1
    stats['max'] = max(stats['max'], rate)
    stats['min'] = min(stats['min'], rate)
    stats['mean'] = stats['sum'] / stats['count']
    stats['vol'] = (stats['max'] - stats['min']) / stats['mean'] if stats['mean'] else 0
    return stats


# Initialize
api_key = '<ADD KEY HERE>'
currency_pairs = [('EUR', 'USD'), ('GBP', 'INR'), ('CHF', 'JPY')]

# Setup databases
sqlite_conn_aux, sqlite_conn_final = setup_sqlite(currency_pairs)
mongodb_client, mongodb_aux, mongodb_final = setup_mongodb(currency_pairs)

# Main loop for data collection
duration_hours = 5
start_time = time.time()
end_time = start_time + duration_hours * 3600
last_calculation_time = start_time


# Initialize statistics
statistics = {pair: {'sum': 0, 'count': 0, 'max': float('-inf'), 'min': float('inf'), 'mean':0, 'vol':0, 'first_timestamp': None} for pair in currency_pairs}
bands = {pair: {'upper': [], 'lower': []} for pair in currency_pairs}

iteration = 0

while time.time() < end_time:
    start_call_time = time.time()
    for pair in currency_pairs:
        rate, fx_timestamp = get_conversion_rate(pair, api_key)
        #print(pair, "->", rate, ":", fx_timestamp)
        if rate is not None:
            insert_data_sqlite(sqlite_conn_aux, pair, rate, fx_timestamp)
            insert_data_mongodb(mongodb_aux, pair, rate, fx_timestamp)
            
            statistics[pair] = update_statistics(statistics[pair], rate)
            if statistics[pair]['first_timestamp'] is None and fx_timestamp is not None:
                statistics[pair]['first_timestamp'] = fx_timestamp
            print("Statistics for", pair, ":", statistics[pair])

    print("---\n")
    end_call_time = time.time()
    time.sleep(max(0, 1 - (end_call_time - start_call_time)))

    # Check if 6 minutes have passed
    if time.time() - last_calculation_time >= 6 * 60:
        print("*** 6 minutes DONE ***\n")
        
        iteration+=1
        print("Iteration:", iteration)
        print(last_calculation_time)
        if iteration == 1:
            for pair in currency_pairs:
                clear_database_data_sqlite(sqlite_conn_aux, pair)
                clear_database_data_mongodb(mongodb_aux, pair)
                bands[pair]['upper'], bands[pair]['lower'] = calculate_keltner_channels(statistics[pair]['mean'], statistics[pair]['vol'])      # {('EUR', 'USD'): {'upper': [], 'lower': []}, ('GBP', 'INR'): {'upper': [], 'lower': []}, ('CHF', 'JPY'): {'upper': [], 'lower': []}}
            statistics = {pair: {'sum': 0, 'count': 0, 'max': float('-inf'), 'min': float('inf'), 'mean':0, 'vol':0, 'first_timestamp': None} for pair in currency_pairs}
            last_calculation_time = time.time()       
        else:
            for pair in currency_pairs:
                # SQLite Calculations
                jumps_sqlite = track_price_jumps_sqlite(sqlite_conn_aux, pair, bands[pair])
                fd_sqlite = jumps_sqlite / (statistics[pair]['max'] - statistics[pair]['min']) if (statistics[pair]['max'] - statistics[pair]['min']) != 0 else 0
                fd_sqlite = fd_sqlite / 100000 if fd_sqlite > 100000 else fd_sqlite  # Normalizing

                # MongoDB Calculations
                jumps_mongodb = track_price_jumps_mongodb(mongodb_aux, pair, bands[pair])
                fd_mongodb = jumps_mongodb / (statistics[pair]['max'] - statistics[pair]['min']) if (statistics[pair]['max'] - statistics[pair]['min']) != 0 else 0
                fd_mongodb = fd_mongodb / 100000 if fd_mongodb > 100000 else fd_mongodb  # Normalizing

                # Update Final Databases (Note: data timestamp uses the first timestamp of the window)
                update_final_sqlite(sqlite_conn_final, pair, statistics[pair]['max'], statistics[pair]['min'], statistics[pair]['mean'], statistics[pair]['vol'], fd_sqlite, statistics[pair]['first_timestamp'])
                update_final_mongodb(mongodb_final, pair, statistics[pair]['max'], statistics[pair]['min'], statistics[pair]['mean'], statistics[pair]['vol'], fd_mongodb, statistics[pair]['first_timestamp'])

                # Clear Auxiliary Databases and Reset Price Tracking
                clear_database_data_sqlite(sqlite_conn_aux, pair)
                clear_database_data_mongodb(mongodb_aux, pair)
        
                bands[pair]['upper'], bands[pair]['lower'] = calculate_keltner_channels(statistics[pair]['mean'], statistics[pair]['vol'])      # {('EUR', 'USD'): {'upper': [], 'lower': []}, ('GBP', 'INR'): {'upper': [], 'lower': []}, ('CHF', 'JPY'): {'upper': [], 'lower': []}}
            
            statistics = {pair: {'sum': 0, 'count': 0, 'max': float('-inf'), 'min': float('inf'), 'mean':0, 'vol':0, 'first_timestamp': None} for pair in currency_pairs}
            last_calculation_time = time.time()
    
# Update Final Databases (Note: data timestamp uses the first timestamp of the window)
update_final_sqlite(sqlite_conn_final, pair, statistics[pair]['max'], statistics[pair]['min'], statistics[pair]['mean'], statistics[pair]['vol'], fd_sqlite, statistics[pair]['first_timestamp'])
update_final_mongodb(mongodb_final, pair, statistics[pair]['max'], statistics[pair]['min'], statistics[pair]['mean'], statistics[pair]['vol'], fd_mongodb, statistics[pair]['first_timestamp'])

# Clear Auxiliary Databases and Reset Price Tracking
clear_database_data_sqlite(sqlite_conn_aux, pair)
clear_database_data_mongodb(mongodb_aux, pair)

sqlite_conn_aux.close()
sqlite_conn_final.close()
mongodb_client.close()

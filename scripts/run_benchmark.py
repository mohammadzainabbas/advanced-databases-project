from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from collections import OrderedDict
from csv import DictReader, writer
from os import getcwd, listdir, makedirs
from os.path import join, exists, isfile, abspath, pardir
from time import sleep, time
from json import dump
from urllib3.util.retry import Retry

# Connection Configuration(s)
token = "RtWgHEkMiipthr5IkT1RpRed4ZXRH-9Q8YuEzZ3pdkjgelVA83D1SZ5tF9V_NY3u7W5-lAl-GuVUC3n3mS1cCQ=="
org = "ulb"
bucket = "advanced_db"
port=8086
url="http://localhost:{}".format(port)

# Directory structure
parent_dir = abspath(join(getcwd(), pardir))
data_dir = join(parent_dir, "data")
book_dir = join(data_dir, "data_books")
trade_dir = join(data_dir, "data_trades")
queries_dir = join(parent_dir, "queries")
result_dir = join(parent_dir, "results")
error_dir = join(parent_dir, "errors")

# Configuration for benchmarking
days = [1, 2, 3, 4, 5, 6, 7]
queries = sorted([x for x in listdir(queries_dir) if isfile(join(queries_dir, x)) and str(x).endswith(".flux")])

# Helper functions
def parse_order_book_row(row: OrderedDict):
    return Point("order_book").tag("symbol", "{}".format(row['symbol'])) \
        .field("ask_price_5", float(row['ask_price_5'])) \
        .field("ask_volume_5", float(row['ask_volume_5'])) \
        .field("ask_price_4", float(row['ask_price_4'])) \
        .field("ask_volume_4", float(row['ask_volume_4'])) \
        .field("ask_price_3", float(row['ask_price_3'])) \
        .field("ask_volume_3", float(row['ask_volume_3'])) \
        .field("ask_price_2", float(row['ask_price_2'])) \
        .field("ask_volume_2", float(row['ask_volume_2'])) \
        .field("ask_price_1", float(row['ask_price_1'])) \
        .field("ask_volume_1", float(row['ask_volume_1'])) \
        .field("bid_price_1", float(row['bid_price_1'])) \
        .field("bid_volume_1", float(row['bid_volume_1'])) \
        .field("bid_price_2", float(row['bid_price_2'])) \
        .field("bid_volume_2", float(row['bid_volume_2'])) \
        .field("bid_price_3", float(row['bid_price_3'])) \
        .field("bid_volume_3", float(row['bid_volume_3'])) \
        .field("bid_price_4", float(row['bid_price_4'])) \
        .field("bid_volume_4", float(row['bid_volume_4'])) \
        .field("bid_price_5", float(row['bid_price_5'])) \
        .field("bid_volume_5", float(row['bid_volume_5'])) \
        .time(datetime.fromtimestamp(float(row['time_stamp'])))

def parse_trade_row(row: OrderedDict):
    return Point("trades").tag("symbol", "{}".format(row['symbol'])) \
        .field("side", 1 if row['side'] == 'buy' else -1) \
        .field("price", float(row['price'])) \
        .field("amount", float(row['amount'])) \
        .time(datetime.fromtimestamp(float(row['time_stamp'])))

def load_data_file_influxdb(file, parse_data_func, is_return=False):
    data = list(map(parse_data_func, DictReader(open(file, 'r'))))
    retries = Retry(total=30, connect=10, read=30)
    with InfluxDBClient(url=url, token=token, org=org, timeout=60000, retries=retries) as client:
        with client.write_api(write_options=SYNCHRONOUS) as write_api:
            start_time = time()
            write_api.write(bucket=bucket, record=data)
            total_time = time() - start_time
            print("Took {} seconds to load {} records in {}".format( total_time, len(data), file))
            if is_return:
                return dict(time=total_time, _start=data[0]._time, _end=data[len(data) - 1]._time)
            else:
                return total_time

def remove_bucket(bucket_name):
    with InfluxDBClient(url=url, token=token, org=org) as client:
        buckets_api = client.buckets_api()
        buckets = buckets_api.find_buckets().buckets
        _bucket = None
        for bucket in buckets:
            if bucket.name == bucket_name:
                _bucket = bucket
        if _bucket is not None:
            start_time = time()
            buckets_api.delete_bucket(_bucket.id)
            total_time = time() - start_time
            print("Took {} seconds to delete '{}' bucket".format( total_time, bucket_name ))
            return total_time
        else:
            print("Unable to delete '{}' bucket".format( bucket_name ))
            return None

def create_bucket(bucket_name):
    created_bucket = None
    with InfluxDBClient(url=url, token=token, org=org) as client:
        buckets_api = client.buckets_api()
        start_time = time()
        created_bucket = buckets_api.create_bucket(bucket_name=bucket_name, org=org)
        total_time = time() - start_time
        print("Took {} seconds to create '{}' bucket".format( total_time, bucket_name ))
        return created_bucket is not None

def run_query_influxdb(query_file, param):
    retries = Retry(total=30, connect=10, read=30)
    with InfluxDBClient(url=url, token=token, org=org, timeout=60000, retries=retries) as client:
        query_api = client.query_api()
        with open(query_file, 'r') as file:
            query = file.read()
            start_time = time()
            _ = query_api.query(query, params=param)
            total_time = time() - start_time
            print("Took {} seconds to run '{}'".format( total_time, query_file ))
            return total_time

def main():

    benchmark_data = dict()
    timing = dict()
    failed_queries = dict()

    for day in days:
        
        benchmark = dict()
        
        trades_btc_file = join(trade_dir, "trades_btc_{}_day.csv".format(day))
        trades_eth_file = join(trade_dir, "trades_eth_{}_day.csv".format(day))
        orderbook_btc_file = join(book_dir, "orderbook_btc_{}_day.csv".format(day))
        
        # 1. Remove the old data (delete the bucket)
        timing["removal_{}_day".format(day)] = remove_bucket(bucket)
        
        # 2. Create new bucket
        bucket_created = create_bucket(bucket)
        if not bucket_created:
            print("Unable to create '{}' bucket".format(bucket))
            break
        
        # 3. Load new data
        timing["load_trade_btc_{}_day".format(day)] = load_data_file_influxdb(trades_btc_file, parse_trade_row)
        timing["load_trade_eth_{}_day".format(day)] = load_data_file_influxdb(trades_eth_file, parse_trade_row)
        load_data = load_data_file_influxdb(orderbook_btc_file, parse_order_book_row, is_return=True)
        timing["load_orderbook_btc_{}_day".format(day)] = load_data['time']
        
        param = dict(_start=load_data['_start'], _end=load_data['_end'])
        sleep(day * 10)

        failed = list()
        # 4. Run benchmark queries
        for query in queries:
            try:
                _time = run_query_influxdb(join(queries_dir, query), param)
                benchmark[query] = _time
            except Exception as e:
                print("Unable to run '{}' for {} day".format(query, day))
                failed.append(dict(day=day, query=query, param=param))
                if not exists(error_dir): makedirs(error_dir)            
                with open(join(error_dir, "{}_{}_day.log".format(query.split(".")[0], day)),'w') as f:
                    f.write(str(e))
        
        if len(failed): failed_queries[day] = failed

        # 5. Save the benchmark
        benchmark_data[day] = benchmark
        if not exists(result_dir): makedirs(result_dir)
        benchmark_file = join(result_dir, "result_{}_day.csv".format(day))
        with open(benchmark_file,'w') as f:
            w = writer(f)
            w.writerows(benchmark.items())
            print("Saved benchmark results to '{}'".format(benchmark_file))

    # Save all benchmark results    
    overall_benchmark_file = join(result_dir, "all_results.json")
    with open(overall_benchmark_file, 'w') as f:
        dump(benchmark_data, f, indent=2)
        print("Saved overall benchmark results to '{}'".format(overall_benchmark_file))
    
    # Save all timings    
    overall_timing_file = join(result_dir, "all_timings.json")
    with open(overall_timing_file, 'w') as f:
        dump(timing, f, indent=2)
        print("Saved overall timings to '{}'".format(overall_timing_file))

    # Save info about all the failed queries    
    failed_queries_file = join(error_dir, "all_failed.json")
    with open(failed_queries_file, 'w') as f:
        dump(failed_queries, f, indent=2)
        print("Saved failed queries details to '{}'".format(failed_queries_file))

if __name__ == "__main__":
    main()

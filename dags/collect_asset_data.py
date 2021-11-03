import datetime
import pytz
import airflow
import numpy as np
from airflow.operators.python import PythonOperator

ASSET_INFO_TABLE = 'asset_info'
ASSET_INFO_COLUMN_TYPES = {'exchange': str, 'symbol': str, 'category': str, 'currency': str, 'full_name': str,
                           'time_zone': str}
ASSET_DATA_TABLE = 'asset_data'
RAW_DATA_COLUMN_TYPES = {'symbol': str, 'start_time': 'datetime64[ns, UTC]', 'close_time': 'datetime64[ns, UTC]',
                         'open': np.float64, 'high': np.float64, 'low': np.float64, 'close': np.float64,
                         'volume': np.float64, 'nr_of_trades': np.int64}

# The currencies to collect
CRYPTO_CURRENCIES_TO_COLLECT = ['BNB', 'BTC', 'ETH', 'XRP', 'ADA', 'LINK', 'LTC', 'BUSD', 'TRX', 'DOGE']

UPDATE_INTERVAL = 60  # How often to collect the data (in minutes)

START_DATE = datetime.datetime(2021, 11, 1)  # From when to collect the data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}


dag = airflow.DAG(
    'asset_collection',
    start_date=datetime.datetime(2020, 1, 1),  # Not used when catchup is disabled
    default_args=default_args,
    schedule_interval=datetime.timedelta(minutes=UPDATE_INTERVAL),
    catchup=False,
    description='Collecting info on available assets and their data',
    max_active_runs=1,
)


def get_logger():
    import logging
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    return logger


def get_binance_client(binance_key_path="/opt/airflow/authentication_details/api_authentication_details.json"):
    from binance.client import Client
    import json
    with open(binance_key_path, 'r') as f:
        binance_auth_dict = json.load(f)
    if binance_auth_dict is None or binance_auth_dict.get('api_key', None) is None \
            or binance_auth_dict.get('api_secret', None) is None:
        raise ValueError("Binance authentication details missing or in the wrong format")
    return Client(api_key=binance_auth_dict['api_key'], api_secret=binance_auth_dict['api_secret'])


def get_sql_cnx(db_key_path = "/opt/airflow/authentication_details/db_authentication_details.json"):
    import json
    from sqlalchemy import create_engine
    with open(db_key_path, 'r') as f:
        db_auth_dict = json.load(f)
    if db_auth_dict is None or not {'user', 'password','address','port','db_name'}.issubset(db_auth_dict.keys()):
        raise ValueError("DB authentication details missing or in the wrong format")
    # Create the connection
    postgres_str = f"postgresql://{db_auth_dict['user']}:{db_auth_dict['password']}@{db_auth_dict['address']}:{db_auth_dict['port']}/{db_auth_dict['db_name']}"
    return create_engine(postgres_str)


def collect_asset_info():
    import pandas as pd
    logger = get_logger()
    logger.info("Running asset info collection")
    logger.info("-> Setup connections")
    # Setup binance connection
    client = get_binance_client()
    # Setup db connection
    cnx = get_sql_cnx()
    logger.info("-> Collecting data")
    info = client.get_exchange_info()
    symbol_details = pd.DataFrame(info['symbols'])
    symbol_details['exchange'] = 'Binance'
    symbol_details['category'] = symbol_details['baseAsset']
    symbol_details['currency'] = symbol_details['quoteAsset']
    symbol_details['full_name'] = symbol_details['symbol']
    symbol_details['time_zone'] = 'UTC'
    df = symbol_details[ASSET_INFO_COLUMN_TYPES.keys()]
    df['active'] = True
    try:
        old_table = pd.read_sql_table(ASSET_INFO_TABLE, cnx)
        old_table['active'] = False
        new_table = df.append(old_table).drop_duplicates(keep='first', subset=['exchange', 'symbol'])
    except ValueError:
        new_table = df
    logger.info(f"-> Writing info to db, {df.shape[0]} rows found")
    new_table.to_sql(ASSET_INFO_TABLE, cnx, if_exists='replace', index=False)
    logger.info("-> Done")


def collect_asset_data(crypto_currency: str):
    import datetime
    import pandas as pd
    from binance.client import Client

    def to_utc_milliseconds(dt: datetime.datetime):
        """ Convert a datetime object to milliseconds utc aka UNIX time """
        epoch = datetime.datetime.utcfromtimestamp(0)
        if dt.tzinfo is not None:
            epoch = epoch.replace(tzinfo=dt.tzinfo)
        return int((dt - epoch).total_seconds() * 1000)

    logger = get_logger()
    logger.info(f"Starting collecting data for {crypto_currency}")
    # Check which rows to collect
    cnx = get_sql_cnx()
    symbols_to_collect = pd.read_sql_query(
        f"SELECT * from {ASSET_INFO_TABLE} where category='{crypto_currency}' and active=True", cnx)
    # For each row, collect the data
    for index, row in symbols_to_collect.iterrows():
        # See if previous data exists
        # For now, just assume it exists
        #if sa.inspect(cnx.engine).has_table(ASSET_DATA_TABLE):
        start_time = pd.read_sql_query(
                f"SELECT max(start_time) from {ASSET_DATA_TABLE} where symbol='{row.symbol}'", cnx).iloc[0, 0]
        #else:
        #    start_time = None  # If the table does not exist
        if start_time is None:  # If the table does not exist or no row was returned
            start_time = pytz.timezone('UTC').localize(START_DATE)
        end_time = datetime.datetime.now(pytz.timezone('UTC'))
        client = get_binance_client()
        logger.info(f"Collecting data for {row.symbol} from {start_time} to {end_time} ")
        dat = client.get_historical_klines(row.symbol, interval=Client.KLINE_INTERVAL_1MINUTE,
                                           start_str=to_utc_milliseconds(start_time),
                                           end_str=to_utc_milliseconds(end_time), limit=1000)
        logger.info(f"Data collection complete: {len(dat)} new records found")
        df = pd.DataFrame(dat, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                                        'total_cost', 'nr_of_trades', 'taker_volume', 'taker_quote_asset_volume',
                                        'ignore'])
        df['start_time'] = pd.to_datetime(df.open_time, unit='ms')
        df['start_time'] = df['start_time'].dt.tz_localize('UTC')
        df['close_time'] = pd.to_datetime(df.close_time, unit='ms')
        df['close_time'] = df['close_time'].dt.tz_localize('UTC')
        df['symbol'] = row.symbol
        df = df.astype({key: val for key, val in RAW_DATA_COLUMN_TYPES.items() if key in df.columns})
        # Delete previous possible incomplete row
        cnx.execute(f"DELETE FROM {ASSET_DATA_TABLE} WHERE symbol='{row.symbol}' and start_time='{start_time}'")
        # And add new rows
        df[RAW_DATA_COLUMN_TYPES.keys()].set_index(['symbol', 'start_time']).to_sql(ASSET_DATA_TABLE, cnx,
                                                                                    if_exists='append', index=True)
    logger.info("Done collecting data")


def create_asset_data_table():
    logger = get_logger()
    cnx = get_sql_cnx()
    if len(list(cnx.execute(f"select * from  pg_catalog.pg_tables where tablename = '{ASSET_DATA_TABLE}'"))) == 0:
        logger.info("Table missing -> creating")
        cnx.execute(f"""
        create table {ASSET_DATA_TABLE} (
        symbol text, 
        start_time timestamp with time zone, 
        close_time timestamp with time zone, 
        open double precision, 
        high double precision, 
        low double precision, 
        close double precision, 
        volume int, 
        nr_of_trades int, 
        primary key (symbol, start_time))""")
    else:
        logger.info("Table already exists, skipping")



asset_collector = PythonOperator(
    task_id='collect_asset_info',
    python_callable=collect_asset_info,
    dag=dag,
)


datatable_constructor = PythonOperator(
    task_id='create_asset_data_table',
    python_callable=create_asset_data_table,
    dag=dag
)

data_collectors = [
    PythonOperator(
        task_id=f'collect_data_for_{crypto_currency}',
        python_callable=collect_asset_data,
        op_kwargs={'crypto_currency': crypto_currency},
        dag=dag)
    for crypto_currency in CRYPTO_CURRENCIES_TO_COLLECT]
asset_collector >> datatable_constructor >> data_collectors

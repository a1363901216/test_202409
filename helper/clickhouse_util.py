import time

import pandas as pd
from clickhouse_driver import Client

'''
pandahouse 是通过http url 链接，端口号是8123
'''
connection = dict(database="stock",
                  host="http://localhost:8123",
                  user='default',
                  password='')

'''
clickhouse_driver 是通过TCP链接，端口号是9000
'''

DB = 'stock'
# settings = {'max_threads': 5}
client = Client(database=f'{DB}',
                host='127.0.0.1',
                port='9000',
                user='default',
                password='',
                # settings=settings
                )
sql = 'SET max_partitions_per_insert_block = 200'
client.execute(sql)



# @timing_decorator
def to_table(data, table):
    if data.empty:
        return 0

    columns = ', '.join(data.columns)
    sql = f'INSERT INTO {table} ({columns}) VALUES'
    client.execute(sql, data.values.tolist())
    return data.shape[0]


# @timing_decorator
def from_table(sql) -> pd.DataFrame:
    last_time = time.time()
    try:
        result = client.query_dataframe(sql)
    except Exception as e:
        print(e)
        result = pd.DataFrame()
        print("from_table Exception: {}  sql: {}".format((time.time() - last_time) * 1000, sql, e))
    # print("db-> 耗时: {}  sql: {}".format((time.time() - last_time) * 1000, sql))
    return result


def optimize(table_name):
    """
    手动触发数据表去重操作
    场景: 在更新表后，由于重复的ReplacingMergeTree是不定时触发的，
    所以可以强制调用触发。
    :param table_name:
    :return:
    """
    sql = f'optimize table stock.{table_name}'
    client.execute(sql)



if __name__ == '__main__':
    print(1)
import os
import tushare as ts
from pandas.io import sql
from sqlalchemy import create_engine
import time

def get_hist_data(code):
    sql_url = os.environ['DATABASE_URL']

    if sql_url is None:
        sql_url = 'postgresql://localhost:5432/restapi'

    print(sql_url)

    sql_engine = create_engine(sql_url)

    all = ts.get_hist_data(code)
    if all is None:
        pass

    sql_db = sql.SQLDatabase(sql_engine)
    # Create the db table if necessary
    t_name = "quote_" + code
    if not sql_db.has_table(t_name):
        args = [t_name, sql_db]
        kwargs = {
            "frame" : all,
            "index" : True, 
            "index_label" : "date",
            "keys" : "date"
        }
        sql_table = sql.SQLTable(*args, **kwargs)
        sql_table.create()

    all.to_sql(t_name, sql_engine, if_exists='append', index=True)

    print("get_hist_data done...")

def get_index():
    sql_url = os.getenv('DATABASE_URL', 'postgresql://localhost:5432/restapi')

    if sql_url is None:
        sql_url = 'postgresql://localhost:5432/restapi'

    print(sql_url)

    try:
        sql_engine = create_engine(sql_url)

        all_index = ts.get_index()
        if all_index is None:
            pass

        currentTime = time.gmtime( time.time()) 
        formatDate = str(currentTime.tm_year) + str(currentTime.tm_mon).zfill(2) + str(currentTime.tm_mday).zfill(2)
        all_index['date'] = formatDate

        all_index.index = all_index.index.map(lambda x: formatDate + str(x).zfill(3))

        sql_db = sql.SQLDatabase(sql_engine)
        # Create the db table if necessary
        t_name = "index_data"
        if not sql_db.has_table(t_name):
            args = [t_name, sql_db]
            kwargs = {
                "frame" : all_index,
                "index" : True,
                "index_label" : "index",
                "keys" : "index"
            }
            sql_table = sql.SQLTable(*args, **kwargs)
            sql_table.create()

        all_index.to_sql(t_name, sql_engine, if_exists='append', index=True)
    except Exception as e:
        print(e)    
    print("get_index done...")
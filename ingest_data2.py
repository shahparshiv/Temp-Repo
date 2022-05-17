#!/usr/bin/env python
# coding: utf-8

import os
import requests
import argparse
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    #port = params.port
    db=params.db
    table_name=params.table_name
    url=params.url
    file_name = 'output.parquet'
    
    response=requests.get(url)
    open(file_name,"wb").write(response.content)
    #os.system(f"wget {url} -O {file_name}")

    df=pd.read_parquet(file_name)
    #df.head()
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    engine = create_engine('postgresql://{user}:{password}@{host}:5432/{db}')
    engine.connect()
    df.to_sql(name=table_name,con=engine,if_exists='replace')
    print(pd.io.sql.get_schema(df,name='yellow_taxi'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest parquet data to postgres')
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    #parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url of csv file')

    args = parser.parse_args()

    main(args)

#df.to_sql(name='yellow_taxi',con=engine,if_exists='replace')
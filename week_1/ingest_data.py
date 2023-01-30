import argparse
import pandas as pd
import os

from sqlalchemy import create_engine
from time import time


def cli():
    parser = argparse.ArgumentParser(description="ingest CSV data to Postgres")

    parser.add_argument("--user", "-u", type=str, help="username for postgres")
    parser.add_argument("--password", "-P", type=str, help="password for postgres")
    parser.add_argument("--hostname", "-hn", type=str, help="hostname for postgres")
    parser.add_argument("--port", "-p", type=str, help="port for postgres")
    parser.add_argument("--database", "-d", type=str, help="database name for postgres")
    parser.add_argument("--tablename", "-t", type=str, help="name of the table we will write the results to")
    parser.add_argument("--url", "-l", type=str, help="url of the csv file containing the data")

    args = parser.parse_args()
    return args


def main(args):

    user = args.user
    password = args.password
    hostname = args.hostname
    port = args.port
    database = args.database
    tablename = args.tablename
    url = args.url

    csv_name = 'output.csv'
    os.system(f"curl {url} -o {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{hostname}:{port}/{database}")
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=tablename, con=engine, if_exists="replace")

    while True:
        t_start = time()
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=tablename, con=engine, if_exists="append")

        t_end = time()

        print(f"Inserted another chunk, took {t_end - t_start:.3f} seconds")



if __name__ == "__main__":
    args = cli()
    main(args)
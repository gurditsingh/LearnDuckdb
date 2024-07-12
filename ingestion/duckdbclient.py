import duckdb
from ingestion.model import JobParameters
from deltalake import DeltaTable, write_deltalake
from duckdb.experimental.spark.sql import SparkSession as session
from duckdb.experimental.spark.sql.functions import lit, col
import pandas as pd

def read_data(params:JobParameters):
    with duckdb.connect() as db:
        data=db.sql(build_query(params))
        return data.df()

def write_parquet():
    with duckdb.connect() as db:
        data=db.sql("CREATE TABLE f_data AS select * from 'data\consumer_data.csv'")
        db.sql("COPY (SELECT * FROM f_data) TO 'item.parquet'")
        

def spark_test():
    spark = session.builder.getOrCreate()

    pandas_df = pd.DataFrame({
        'age': [34, 45, 23, 56],
        'name': ['Joan', 'Peter', 'John', 'Bob']
    })

    df = spark.createDataFrame(pandas_df)
    df = df.withColumn(
        'location', lit('Seattle')
    )
    res = df.select(
        col('age'),
        col('location')
    ).collect()

    print(res)

def build_query(params:JobParameters)->str:
    return f"""
    select count(*) from '{params.file_path}'
    """

def write_data(df):
    write_deltalake("transform/t_duckdb/seeds/consumer_delta_table",df)


def read_delta_data():
    with duckdb.connect() as db:
        
        db.install_extension('delta')
        db.load_extension('delta')
        
        data=db.sql("select * from delta_scan('transform/t_duckdb/seeds/consumer_delta_table')")
        data.show()
from ingestion.duckdbclient import read_data,write_data,read_delta_data,spark_test,write_parquet
from ingestion.model import JobParameters
import fire

def main(params:JobParameters):
    print("Pipeline running...")
    # df=read_data(params)
    # write_data(df)
    # read_delta_data()
    write_parquet()
    


if __name__ == '__main__':
    params= JobParameters(file_path="data/consumer_data.csv")
    main(params)
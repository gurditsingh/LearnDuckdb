from ingestion.model import JobParameters
from ingestion.duckdbclient import build_query

def test_build_query():
    params= JobParameters(file_path="data/consumer_data.csv")
    
    query=build_query(params)

    expected = "select count(*) from 'data/consumer_data.csv'"

    assert query.strip() == expected.strip()

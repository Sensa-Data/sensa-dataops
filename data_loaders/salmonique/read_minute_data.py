if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from mage_ai.data_preparation.shared.secrets import get_secret_value
from datetime import datetime, timedelta
from influxdb_client_3 import InfluxDBClient3
import pandas as pd

host = get_secret_value('influx_host')
org = get_secret_value('influx_salmonique_org')
token = get_secret_value('influx_salmonique_token')
source_bucket = 'aggregate_salmonique_long'


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    schedule_time = kwargs.get('execution_date')
    stop_time = datetime.fromisoformat(str(schedule_time)).replace(tzinfo=None)
    trigger_interval = kwargs.get('trigger_interval' , None)
    trigger_interval = 'hour'
    if trigger_interval is None or trigger_interval.startswith('var'):
        raise Exception("Set trigger Interval")
    if trigger_interval =='hour':
        start_time = stop_time - timedelta(hours=1)
    elif trigger_interval =='day':
        start_time = stop_time - timedelta(days=1)
    else:
        raise Exception("trigger_interval can be hour or day only")
    tags = []
    with InfluxDBClient3(
            host=host,
            org=org,
            token=token,
            database=source_bucket
        ) as client:
            query = f'''
                SELECT *
                FROM "{source_bucket}"
                WHERE
                time >= '{start_time}'
                AND time <= '{stop_time}'
            '''
            query_str_singleline = ' '.join(line.strip() for line in query.splitlines())
            print(f"Query on biofish_min: \"{query_str_singleline.strip()}\"")
            table = client.query(query, language="influxql")
            table = table.drop("iox::measurement")

            if table.num_rows < 1:
                print(f"Query: \"{query_str_singleline.strip()}\" returned empty table.")
                raise Exception(f"Query: \"{query_str_singleline.strip()}\" returned empty table.")

            # Get list of tags
            tags = []
            for column in table.schema:
                if column.metadata[b"iox::column::type"].endswith(b"tag"):
                    tags.append(column.name)
            df = table.to_pandas()
    return df, tags


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

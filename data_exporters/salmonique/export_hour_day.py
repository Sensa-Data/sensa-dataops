if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
from mage_ai.data_preparation.shared.secrets import get_secret_value
from datetime import datetime, timedelta
from influxdb_client_3 import InfluxDBClient3
import pandas as pd

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: downsampled minute data, tags
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here
    host = get_secret_value('influx_host')
    org = get_secret_value('influx_salmonique_org')
    token = get_secret_value('influx_salmonique_token')
    source_bucket = 'aggregate_salmonique_long'
    df = data[0]
    tags = data[1]
    target_bucket = 'aggregate_salmonique_long'
    trigger_interval = kwargs.get('trigger_interval' , None)
    data_frame_measurement_name = f"salmonique_{trigger_interval}"

    with InfluxDBClient3(
        host=get_secret_value('influx_host'),
        org=org,
        token=get_secret_value('influx_salmonique_token'),
        database=target_bucket
    ) as client:
        for tag in tags:
            client.write(
                    record=df,
                    data_frame_measurement_name=data_frame_measurement_name,
                    data_frame_timestamp_column="time",
                    data_frame_tag_columns=tag
            )

    print(f"Wrote {len(df.index)} rows to {target_bucket}")



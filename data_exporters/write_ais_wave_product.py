import json
import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

url = "https://api.sensadata.io/ingest/"

DB = {
        "customer_id": get_secret_value('sdm_customer_id'),
        "key": get_secret_value('sdm_customer_key'),
        "bucket": "AIS data"
}

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    records =[]
    for index, row in data.iterrows():
        records.append({
        "Table": "ais_wave_efficiency",
        "Tags": {
            "Name": row['name'],
            "Mmsi": row['mmsi'],
            "Shiptype": row['shipType']
        },
        "Fields": {
                "Latitude": row['latitude'],
                "Longitude": row['longitude'],
                "SpeedOverGround": row['speedOverGround'],
                "WaveHeight": row['Wave_height'],
                "FuelEfficiencyScore": row['fuelEfficencyScore']
            },
        "Time": row['msgtime']
        })
    body = json.dumps({"db": DB, "data":records})
    response = requests.post(url, data=body)
    return response.json()
import json
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(token, *args, **kwargs):
    """
    Template for loading data from API
    """
    #token=token['get_token'][0]['token']

    url = 'https://live.ais.barentswatch.no/v1/latest/combined'
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Bearer {token['get_ais_access_token'][0]}"
    }
    response = requests.get(url,headers=headers)
    response = pd.DataFrame(response.json())


    return response


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


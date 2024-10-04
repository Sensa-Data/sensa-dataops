if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import requests
import pandas as pd
import json



@transformer
def transform(token,data, *args, **kwargs):
    """
    ...
    """
    print (token)
    df = data
    df = df.head(10)
    token = token['get_bw_api_access_token'][0]
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": f"Bearer {token}"
    }
    respons_code = []
    wave_col=[]
    for index, row in df.iterrows():
        latitude= row['latitude']
        longitude= row['longitude']
        #url2 = 'https://www.barentswatch.no/bwapi/v1/waveforecastpoint/nearest?x=5.736098&y=60.073905'
        url = f"https://www.barentswatch.no/bwapi/v1/waveforecastpoint/nearest?x={longitude}&y={latitude}"
        response = requests.get(url,headers=headers)
        if response.status_code == 200:
            response = response.json()
            wave_data = response['totalSignificantWaveHeight']
        else:
            wave_data = None
        wave_col.append(wave_data)

    df['Wave_height']= wave_col

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

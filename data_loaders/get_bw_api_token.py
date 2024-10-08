import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.shared.secrets import get_secret_value



@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    url = "https://id.barentswatch.no/connect/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
    "client_id": get_secret_value('bw_api_client_id'),
    "scope": "api",
    "client_secret": get_secret_value('bw_api_client_secret'),
    "grant_type": "client_credentials"
}
    response = requests.post(url, headers=headers, data=data)
    token_data = response.json()
    
    return token_data['access_token']

    #return response.json()
    #pd.read_csv(io.StringIO(response.text), sep=',')

#test

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


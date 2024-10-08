if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
import asyncio
import httpx
import nest_asyncio

nest_asyncio.apply()


async def get_wave_data(row: dict, headers: dict) -> str:
    latitude = row["latitude"]
    longitude = row["longitude"]
    url = f"https://www.barentswatch.no/bwapi/v1/waveforecastpoint/nearest?x={longitude}&y={latitude}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code == 200:
            response = response.json()
            wave_data = response["totalSignificantWaveHeight"]
        else:
            wave_data = None
    return wave_data


async def process_df(df: pd.DataFrame, headers):
    wave_col = []
    # chunks = [df[i : i + 1000] for i in range(0, len(df), 1000)]
    chunks = [df[i : i + 1000] for i in range(0, len(df), 1000)]
    for chunk in chunks:

        tasks = [
            get_wave_data(row, headers) for index, row in chunk.iterrows()
        ]  # Create a list of tasks
        results = await asyncio.gather(*tasks)  # Await all tasks concurrently
        for result in results:
            wave_col.append(result)


@transformer
def transform(token: dict, df: pd.DataFrame, *args, **kwargs):
    """
    Block to enrich ais ship data with wave height data
    Args:
        token: Token for Barents Watch general API
        df: Data frame with ais data to be enriched with wave data
    Returns:
        Input data frame enriched with wave data
    """
    print(token)
    #df = df.head(100)
    #token = token["get_bw_api_access_token"]
    #token = token[0]
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Bearer {token}",
    }
    #
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # no event loop running:
        loop = asyncio.new_event_loop()
    result = loop.run_until_complete(
        process_df(df, headers)
    )  # Run async function until complete
    df["Wave_height"] = result
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, "The output is undefined"

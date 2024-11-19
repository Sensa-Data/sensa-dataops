if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
from pydantic import BaseModel, ValidationError, constr, validator
from pydantic_extra_types.coordinate import Latitude
from pydantic_extra_types.coordinate import Longitude
from datetime import date, datetime, time, timedelta


class AISModel(BaseModel):
    courseOverGround: float
    latitude: Latitude
    longitude: Longitude
    name: str
    rateOfTurn: float
    shipType: int
    speedOverGround: float
    trueHeading: float
    navigationalStatus: int
    mmsi: int
    msgtime: datetime

    @validator('name', pre=True, always=True)
    def capitalize_name(cls, value):
        if isinstance(value, str):
            return value.capitalize()
        return value

    @validator('msgtime')
    def ensure_timezone(cls, value):
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError("start_time must include a timezone.")
        return value


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    logger = kwargs.get('logger')
    url = 'https://gist.githubusercontent.com/moonstruck/cf66f076634b2cdbc2c63e3746a4f7d3/raw/barentswatch_ais_live.csv'
    df = pd.read_csv(url)

    response = []
    for index, row in df.iterrows():
        try:
            validated_row = AISModel(**row)
            logger.info(f"Row {index} is valid: {index}")
            response.append(row)
        except ValidationError as e:
            logger.warning(f"Row {index} validation error:\n{e}")

    return pd.DataFrame(response)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
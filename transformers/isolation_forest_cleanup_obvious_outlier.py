import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from typing import Optional
from datetime import datetime
from typing import Any

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


class WaterQualityModel(BaseModel):
    Bisulfide: Optional[float] = Field(None, description="Bisulfide concentration")
    CO2: Optional[float] = Field(None, description="CO2 concentration")
    Conductivity: Optional[float] = Field(None, description="Conductivity")
    Equipment: Optional[str] = Field(None, description="Equipment")
    H2S: Optional[float] = Field(None, description="H2S concentration")
    MeasuringUnit: Optional[str] = Field(None, description="Measuring unit")
    Nitrate: Optional[float] = Field(None, description="Nitrate concentration")
    Nitrite: Optional[float] = Field(None, description="Nitrite concentration")
    Oxygen: Optional[float] = Field(None, description="Oxygen concentration")
    PH: Optional[float] = Field(None, description="PH level")
    Quality: Optional[str] = Field(None, description="Quality")
    Section: Optional[str] = Field(None, description="Section")
    Subunit: Optional[str] = Field(None, description="Subunit")
    TOCeq: Optional[float] = Field(None, description="TOCeq")
    Temperature: Optional[float] = Field(None, description="Temperature")
    Turbidity: Optional[float] = Field(None, description="Turbidity")
    UV254f: Optional[float] = Field(None, description="UV254f")
    UV254t: Optional[float] = Field(None, description="UV254t")
    Unit: Optional[str] = Field(None, description="Unit")
    host: Optional[str] = Field(None, description="Host")
    id: Optional[str] = Field(None, description="ID")
    time: datetime = Field(..., description="Timestamp")


class FeedingSystemModel(BaseModel):
    AvgFeedHour: float = Field(..., description="Average feed hour")
    Equipment: str = Field(..., description="Equipment type")
    MeasuringUnit: str = Field(..., description="Measuring unit")
    Quality: Optional[str] = Field(None, description="Quality")
    Section: Optional[str] = Field(None, description="Section identifier")
    Sensor: Optional[str] = Field(None, description="Sensor information")
    Subunit: str = Field(..., description="Subunit name")
    Tank: Optional[str] = Field(None, description="Tank identifier")
    Unit: Optional[str] = Field(None, description="Unit")
    host: Optional[str] = Field(None, description="Host")
    id: Optional[str] = Field(None, description="ID")
    time: datetime = Field(..., description="Timestamp of the record")


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    logger = kwargs.get('logger')
    measurements=["WaterQuality", "feedingsystem"]
    measurements_models = [WaterQualityModel, FeedingSystemModel]
    cleaned_data = ()
    
    for idx, measurement in enumerate(data):
        df = measurement[0]
        tags = measurement[1]
        measurement_name = measurements[idx]
        measurement_model = measurements_models[idx]

        cleaned_measurement_data = []
        total_data_count = len(df)
        removed_data_count = 0

        for index, row in df.iterrows():
            try:
                validated_row = measurement_model(**row)
                # logger.info(f"Row {index} is valid: {index}")
                cleaned_measurement_data.append(row)
            except ValidationError as e:
                print(row)
                # logger.warning(f"Row {index} validation error:\n{e}")
                removed_data_count += 1
        
        
        cleaned_data_count = len(cleaned_measurement_data)
        print(cleaned_data_count)
        cleaned_data = cleaned_data + (
            (
                pd.DataFrame(cleaned_measurement_data), 
                tags, 
                total_data_count, 
                cleaned_data_count, 
                removed_data_count,
            ),
        )

    print(cleaned_data)
    return cleaned_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@transformer
def transform(data, *args, **kwargs):
    """
    Downsample raw sensor data by minute

    Args:
        data: minute raw data and tags
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        tuple(dataframe, tags)
    """
    # Specify your transformation logic here
    schedule_time = kwargs.get('execution_date')
    aggregated_data = ()
    for measurement in data:
        df = measurement[0]
        tags = measurement[1]
        aggregated = df.groupby(tags).mean().reset_index()
        aggregated["time"] = schedule_time
        print(f"Downsampled {len(df.index)} rows into {len(aggregated.index)} rows.")
        aggregated_data = aggregated_data + ((aggregated, tags),)
    
    return aggregated_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

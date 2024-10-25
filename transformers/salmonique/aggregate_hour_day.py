if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


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
        tuple(downsampled minute data, tags)
    """
    # Specify your transformation logic here
    schedule_time = kwargs.get('execution_date')
    df = data[0]
    tags = data[1]
    df_aggregated = df.groupby(tags).mean().reset_index()
    df_aggregated["time"] = schedule_time
    print(f"Downsampled {len(df.index)} rows into {len(df_aggregated.index)} rows.")

    return df, tags


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
import numpy as np
import random

@transformer
def transform(data, *args, **kwargs):
    """
    ...
    """
    df = pd.DataFrame(data)
    df['speedOverGround'] = np.where((df['speedOverGround'].isna()) | (df['speedOverGround'] == 0), random.uniform(0,1), df['speedOverGround'])
    df['Wave_height'] = np.where((df['Wave_height'].isna()) | (df['Wave_height'] == 0), random.uniform(0,1), df['Wave_height'])
    df['fuelEfficencyScore'] = df['speedOverGround'] * df['Wave_height']
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

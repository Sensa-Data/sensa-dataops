if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@transformer
def transform(data, *args, **kwargs):
    """
    ...
    """
    df = pd.DataFrame(data)
    #Remove rows where Wave_height is Nove
    df = df[df['Wave_height'].notna()]
    df['fuelEfficencyScore']=df['speedOverGround']*df['Wave_height']
    
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

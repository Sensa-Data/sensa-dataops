import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from default_repo.utils.models.kmeans.kmeans_utils import anomaly_predict_kmeans, anomaly_predict_kmeans_single


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

    measurements=["WaterQuality", "feedingsystem"]
    measurements_field_columns = {
        "WaterQuality": ['Bisulfide', 'CO2', 'Conductivity', 'H2S', 'Nitrate', 'Nitrite', 'Oxygen', 'PH', 'TOCeq', 'Temperature', 'Turbidity', 'UV254f', 'UV254t'],
        "feedingsystem": ['AvgFeedHour'],
    }
    measurements_tag_columns = {
        "WaterQuality": ['Equipment', 'Section', 'Subunit', 'Unit', 'Origin'],
        "feedingsystem": ['Equipment', 'Section', 'Subunit', 'Unit', 'Origin'],
    }

    TIME_COL = 'time'
    MEASURING_UNIT_COL = 'MeasuringUnit'

    schedule_time = kwargs.get('execution_date')
    aggregated_data = ()

    for idx, measurement in enumerate(data):
        df, tags, total_data_count, cleaned_data_count, removed_data_count, *_ = measurement
        if MEASURING_UNIT_COL in tags:
            tags.remove(MEASURING_UNIT_COL)
            
        measurement_name = measurements[idx]
        measurement_columns = measurements_field_columns.get(measurement_name)
        tag_columns = measurements_tag_columns.get(measurement_name)

        aggregated_row_columns = measurement_columns + tag_columns + [TIME_COL, MEASURING_UNIT_COL]

        aggregated_rows = []
        groups = df.groupby(tag_columns)
        for (equipment, section, subunit, unit, origin), group_data_df in groups:
            
            # Following existing Influx table naming pattern, e.g. Total_Data_Count
            field_column_unit_aggregation = {
                "Total_Data_Count": total_data_count,
                "Cleaned_Data_Count": cleaned_data_count,
                "Removed_Data_Count": removed_data_count,
            }

            field_column_unit_aggregation[TIME_COL] = pd.to_datetime(group_data_df[TIME_COL]).mean()
            field_column_unit_aggregation['Equipment'] = equipment
            field_column_unit_aggregation['Section'] = section
            field_column_unit_aggregation['Subunit'] = subunit
            field_column_unit_aggregation['Unit'] = unit
            field_column_unit_aggregation['Origin'] = origin

            for field_column_name in measurement_columns:
                field_column_unit_aggregation[field_column_name] = group_data_df[field_column_name].mean()
                fc_unique_values = group_data_df[[field_column_name, MEASURING_UNIT_COL]].dropna(subset=[field_column_name])[MEASURING_UNIT_COL].unique()
                assert len(fc_unique_values) <= 1

                field_column_unit_aggregation[f"{field_column_name}_Unit"] = fc_unique_values[0] if len(fc_unique_values) == 1 else ""

            aggregated_rows.append(field_column_unit_aggregation)
        
        aggregated_single_row_df = pd.DataFrame(aggregated_rows)

        aggregated_data = aggregated_data + ((aggregated_single_row_df, tags),)

    return aggregated_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    # assert output is not None, 'The output is undefined'
    assert output is not None, 'The output is undefined'
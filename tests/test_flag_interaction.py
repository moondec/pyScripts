
import pandas as pd
import numpy as np
import pytest

# Import the functions to be tested
from unified_script import apply_value_range_flags, apply_quality_flags, _ensure_flag_columns_exist

def test_flag_interaction_prevents_overwrite(monkeypatch):
    """
    Tests the interaction between flag-related functions, ensuring overwrite is prevented.
    1. Verifies that apply_value_range_flags sets an initial flag.
    2. Verifies that apply_quality_flags does NOT overwrite the existing flag.
    3. Verifies that _ensure_flag_columns_exist does not cause further overwrites.
    """
    # 1. Setup
    df = pd.DataFrame({
        'TIMESTAMP': pd.to_datetime(['2023-01-15']),
        'temp_air': [100.0],  # Out of range, will be flagged with 4
        'pressure': [950.0]  # Numeric column that needs a flag column created
    })

    mock_value_range_flags = {'temp': {'min': -50, 'max': 50}}
    mock_station_mapping_qc = {'TEST_GROUP': 'test_rules'}
    mock_quality_flags = {
        'test_rules': {
            'temp_air': [{'start': '2023-01-01', 'end': '2023-01-31', 'flag_value': 2}]
        }
    }
    
    monkeypatch.setattr('unified_script.VALUE_RANGE_FLAGS', mock_value_range_flags)
    monkeypatch.setattr('unified_script.STATION_MAPPING_FOR_QC', mock_station_mapping_qc)
    monkeypatch.setattr('unified_script.QUALITY_FLAGS', mock_quality_flags)

    # 2. Execution
    
    # Step 1: Apply range flags
    df1 = apply_value_range_flags(df)
    assert df1['temp_air_flag'].iloc[0] == 4
    assert 'pressure_flag' not in df1.columns

    # Step 2: Apply quality flags
    config = {'file_id': 'TEST_GROUP'}
    df2 = apply_quality_flags(df1, config)
    # Assert that the flag was NOT overwritten and remains 4
    assert df2['temp_air_flag'].iloc[0] == 4

    # Step 3: Ensure flag columns exist
    df3 = _ensure_flag_columns_exist(df2)

    # 3. Final Assertion
    
    # Assert that the flag is still unchanged
    assert df3['temp_air_flag'].iloc[0] == 4
    
    # Assert that _ensure_flag_columns_exist correctly created the missing flag column
    assert 'pressure_flag' in df3.columns
    assert df3['pressure_flag'].iloc[0] == 0

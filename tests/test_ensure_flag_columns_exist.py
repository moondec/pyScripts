import pandas as pd
import numpy as np

from unified_script import _ensure_flag_columns_exist


def test_adds_missing_flag_columns_values_and_dtype():
    df = pd.DataFrame({
        'a': [1.0, np.nan, 3.5, np.nan],
        'b': [0, 1, np.nan, 2],
    })

    out = _ensure_flag_columns_exist(df)

    # New flag columns created
    assert 'a_flag' in out.columns
    assert 'b_flag' in out.columns

    # Values: 0 for notna, 99 for NaN
    assert out['a_flag'].tolist() == [0, 99, 0, 99]
    assert out['b_flag'].tolist() == [0, 0, 99, 0]

    # Dtype int8
    assert out['a_flag'].dtype == 'int8'
    assert out['b_flag'].dtype == 'int8'


def test_preserves_existing_flag_columns():
    df = pd.DataFrame({
        'c': [np.nan, 2, 3],
        'c_flag': [7, 7, 7],  # already exists, should not be recreated/modified
    })

    out = _ensure_flag_columns_exist(df)

    assert 'c_flag' in out.columns
    # unchanged
    assert out['c_flag'].tolist() == [7, 7, 7]


def test_skips_non_numeric_and_cols_to_skip():
    df = pd.DataFrame({
        'name': ['x', 'y', 'z'],            # non-numeric -> no flag
        'latitude': [1.0, np.nan, 3.0],     # in cols_to_skip -> no flag
        'longitude': [np.nan, 2.0, 3.0],    # in cols_to_skip -> no flag
        'TIMESTAMP': pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03']),  # in cols_to_skip -> no flag
        'value': [1.0, np.nan, 5.0],        # numeric -> flag should be created
    })

    out = _ensure_flag_columns_exist(df)

    # No flags for skipped/non-numeric columns
    assert 'name_flag' not in out.columns
    assert 'latitude_flag' not in out.columns
    assert 'longitude_flag' not in out.columns
    assert 'TIMESTAMP_flag' not in out.columns

    # Flag created for numeric column not skipped
    assert 'value_flag' in out.columns
    assert out['value_flag'].tolist() == [0, 99, 0]
    assert out['value_flag'].dtype == 'int8'



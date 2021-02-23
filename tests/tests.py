# -*- coding: utf-8 -*-
from datagun import DataSet
from tests.new_tests import wrapper_data


def test_get_errors():
    data = [
        [1, 2, 3],
        ["error_value", 20, 30],
        ["error_value", "error_value", 300],
        [[1000], [2000], [3000]],
        [[[10000]], [[20000]], [[3000]]],
    ]
    data2 = [
        ["error_value", 5, 6],
        [40, 50, 60],
        [400, 500, 600],
        [[4000], [5000], ["error_value"]],
        [[[40000]], [[50000]], [[6000]]],
    ]
    ds1 = DataSet(data,
                  [{"name": "a", "dtype": "int", "dt_format": None},
                   {"name": "b", "dtype": "int", "dt_format": None},
                   {"name": "c", "dtype": "float", "dt_format": None},
                   {"name": "d", "dtype": "float", "dt_format": None, "depth": 1},
                   {"name": "e", "dtype": "float", "dt_format": None, "depth": 2}],
                  orient="columns")
    ds2 = DataSet(data2,
                  [{"name": "a", "dtype": "int", "dt_format": None},
                   {"name": "b", "dtype": "int", "dt_format": None},
                   {"name": "c", "dtype": "float", "dt_format": None},
                   {"name": "d", "dtype": "float", "dt_format": None, "depth": 1},
                   {"name": "e", "dtype": "float", "dt_format": None, "depth": 2}],
                  orient="columns")
    ds3 = ds1.append(ds2)

    for ds in [ds1, ds2, ds3]:
        ds_error = ds.get_errors()
        for ds_error in ds_error.to_values():
            index_cols, row = ds_error
            for index_col in index_cols:
                assert row[index_col] in ["error_value", ["error_value"]]


def test_to_data():
    data = [
        [1, 2, 3],
        [10, 20, 30],
        [100, 200, 300]
    ]
    schema = [{"name": "a", "dtype": "string", "default_value": "", "is_array": "False", "dt_format": None},
              {"name": "b", "dtype": "string", "default_value": "", "is_array": "False", "dt_format": None},
              {"name": "c", "dtype": "string", "default_value": "", "is_array": "False", "dt_format": None}]

    new_data = DataSet(data, schema=schema, orient="columns")
    assert [('1', '10', '100'), ('2', '20', '200'), ('3', '30', '300')] == new_data.to_values()
    assert [['1', '2', '3'], ['10', '20', '30'], ['100', '200', '300']] == new_data.to_list()
    assert [{'b': '10', 'a': '1', 'c': '100'}, {'b': '20', 'a': '2', 'c': '200'},
            {'b': '30', 'a': '3', 'c': '300'}] == new_data.to_dict()
    assert [('1', '10'), ('2', '20')] == new_data[["a", "b"]][:2].to_values()
    assert ['1', '2', '3'] == new_data["a"].data()
    assert [('1', '10'), ('2', '20'), ('3', '30')] == new_data[["a", "b"]].to_values()


@wrapper_data
def test_filter(data_shot):
    series = data_shot[data_shot.columns[1]]
    series_filter_value = series == 30
    print(series)
    print(series_filter_value)
    print(data_shot.filter(series_filter_value))
    assert series.filter(series_filter_value).data() == [30]
    assert data_shot.filter(series_filter_value).to_values() == [('3', 30, 0.0)]


@wrapper_data
def test_rename_columns(data_shot):
    data_shot.rename_columns({k: k + k for k in data_shot.columns})
    assert set(data_shot.columns).issubset({'aa', 'bb', 'cc'})

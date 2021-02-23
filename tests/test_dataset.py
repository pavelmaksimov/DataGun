# -*- coding: utf-8 -*-
import pytest

from datagun import DataSet


def test_transpont():
    data = [[1, 2, 3], [10, 20, 30], [100, 200, 300], [1000, 2000, 3000]]
    dataset1 = DataSet(data, orient="columns")

    assert dataset1.T().to_dict() == [
        {"0": 1, "1": 2, "2": 3},
        {"0": 10, "1": 20, "2": 30},
        {"0": 100, "1": 200, "2": 300},
        {"0": 1000, "1": 2000, "2": 3000},
    ]

    dataset2 = DataSet(data, orient="values")
    assert dataset2.T().to_dict() == [
        {"0": 1, "1": 10, "2": 100, "3": 1000},
        {"0": 2, "1": 20, "2": 200, "3": 2000},
        {"0": 3, "1": 30, "2": 300, "3": 3000},
    ]


def test_clear_values():
    data = [[0]]

    schema = [{"dtype": "string", "allow_null": True, "clear_values": [0]}]
    ds = DataSet(data=data, schema=schema, orient="columns")
    assert ds["0"][0] == ""

    schema = [{"dtype": "string", "allow_null": False, "clear_values": [0]}]
    ds = DataSet(data=data, schema=schema, orient="columns")
    assert ds["0"][0] == ""

    schema = [{"dtype": "string", "allow_null": False, "null_value": "NULL", "clear_values": [0]}]
    ds = DataSet(data=data, schema=schema, orient="columns")
    assert ds["0"][0] == ""


def test_null_values():
    data = [[None]]

    schema = [{"dtype": "string", "allow_null": True}]
    ds = DataSet(data=data, schema=schema, orient="columns")
    assert ds["0"][0] == None

    schema = [{"dtype": "string", "allow_null": False}]
    ds = DataSet(data=data, schema=schema, orient="columns")
    assert ds["0"][0] == ""

    schema = [{"dtype": "string", "allow_null": True, "null_value": "NULL"}]
    ds = DataSet(data=data, schema=schema, orient="columns")
    assert ds["0"][0] == "NULL"

    schema = [{"dtype": "string", "allow_null": False, "null_value": "NULL"}]
    ds = DataSet(data=data, schema=schema, orient="columns")
    assert ds["0"][0] == ""


@pytest.mark.parametrize("orient", ["columns", "values", "dict"])
@pytest.mark.parametrize("data", [None, [], [[]], [{}], ])
def test_empty_data_with_schema(data, orient):
    ds = DataSet(data, schema=[{"name": "col"}], orient=orient)
    assert ds.to_values() == []
    assert ds.to_dict() == []
    assert ds.to_list() == [[]]
    assert ds.to_series() == []
    assert ds.to_text() == "col\n"
    assert ds.get_errors().to_values() == []


@pytest.mark.parametrize(
    "data,orient",
    [
        ([[1], [20], [300]], "columns"),
        ([[1, 20, 300]], "values"),
        ([{"col1": 1, "col2": 20, "col3": 300}], "dict"),
    ]
)
def test_empty_data_without_schema(data, orient):
    ds = DataSet(data, orient=orient)
    assert ds.to_values() == [(1, 20, 300)]


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
                  [{"name": "a", "dtype": "int"},
                   {"name": "b", "dtype": "int"},
                   {"name": "c", "dtype": "float"},
                   {"name": "d", "dtype": "float", "depth": 1},
                   {"name": "e", "dtype": "float", "depth": 2}],
                  orient="columns")
    ds2 = DataSet(data2,
                  [{"name": "a", "dtype": "int"},
                   {"name": "b", "dtype": "int"},
                   {"name": "c", "dtype": "float"},
                   {"name": "d", "dtype": "float", "depth": 1},
                   {"name": "e", "dtype": "float", "depth": 2}],
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
    schema = [{"name": "a", "dtype": "string", "default_value": "", "is_array": "False"},
              {"name": "b", "dtype": "string", "default_value": "", "is_array": "False"},
              {"name": "c", "dtype": "string", "default_value": "", "is_array": "False"}]

    new_data = DataSet(data, schema=schema, orient="columns")
    assert [('1', '10', '100'), ('2', '20', '200'), ('3', '30', '300')] == new_data.to_values()
    assert [['1', '2', '3'], ['10', '20', '30'], ['100', '200', '300']] == new_data.to_list()
    assert [{'b': '10', 'a': '1', 'c': '100'}, {'b': '20', 'a': '2', 'c': '200'},
            {'b': '30', 'a': '3', 'c': '300'}] == new_data.to_dict()
    assert [('1', '10'), ('2', '20')] == new_data[["a", "b"]][:2].to_values()
    assert ['1', '2', '3'] == new_data["a"].data()
    assert [('1', '10'), ('2', '20'), ('3', '30')] == new_data[["a", "b"]].to_values()


def test_filter():
    data = [
        ["a0", 2, 3],
        [10, "b1", 30],
        [100, 200, "c3"]
    ]
    data_shot = DataSet(data, [{"name": "a", "dtype": "string"},
                               {"name": "b", "dtype": "int"},
                               {"name": "c", "dtype": "float"}], orient="columns")

    series = data_shot[data_shot.columns[1]]
    series_filter_value = series == 30
    assert series.filter(series_filter_value).data() == [30]
    assert data_shot.filter(series_filter_value).to_values() == [('3', 30, 0.0)]


def test_rename_columns():
    data = [
        ["a0", 2, 3],
        [10, "b1", 30],
        [100, 200, "c3"]
    ]
    data_shot = DataSet(data, [{"name": "a", "dtype": "string"},
                               {"name": "b", "dtype": "int"},
                               {"name": "c", "dtype": "float"}], orient="columns")

    data_shot.rename_columns({k: k + k for k in data_shot.columns})
    assert set(data_shot.columns).issubset({'aa', 'bb', 'cc'})

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

# -*- coding: utf-8 -*-
from data_transformer import Data
import pytest
import datetime as dt


@pytest.mark.parametrize(
    "schema,data,standart",
    [
        (
                [{"name": "name1", "type": "uint", "errors": "default"},
                 {"name": "name2", "type": "uint", "errors": "default"},
                 {"name": "name3", "type": "int", "errors": "default"},
                 {"name": "name4", "type": "int", "errors": "default"},
                 {"name": "name5", "type": "float", "errors": "default"},
                 {"name": "name6", "type": "float", "errors": "default"},
                 {"name": "name7", "type": "string", "errors": "default"},
                 {"name": "name8", "type": "string", "errors": "default"},
                 {"name": "name9", "type": "string", "errors": "default"},
                 {"name": "name10", "type": "string", "errors": "default"},],
                [["-1", 1, -1, "-1", -1, "-1", "[str]", -1, None, False]],
                [[0, 1, -1, -1, -1.0, -1.0, "[str]", "-1", str(None), str(False)]],
        ),
    ],
)
def test_errors1(schema, data, standart):
    data = Data(data, schema)
    data.transform()
    data.filtered()
    print(data.to_json())
    assert data.to_json() == standart


@pytest.mark.parametrize(
    "schema,dataset,standart,errors",
    [
        (
                [{"name": "name1", "type": "uint", "errors": "default"},],
                [{"name1": "-1"}, [-1], [0], [1], [1,1]],
                [{'name1': 0}, [0], [0], [1]],
                {'name1': ['-1', -1]}
        ),
    ],
)
def test_errors2(schema, dataset, standart, errors):
    dataset = Data(dataset, schema)
    dataset.transform()
    dataset.filtered()
    print(dataset.error_values)
    print(dataset.error_rows)
    print("dataset", dataset.to_json())
    assert dataset.error_values == errors
    assert dataset.to_json() == standart


@pytest.mark.parametrize(
    "schema,dataset,standart,errors",
    [
        (
                [{"name": "name1", "type": "uint", "errors": "default", "is_json": True},],
                [["[-1]"], [[-1]], [[0]], [[1]], [[1], [1]]],
                [[[0]], [[0]], [[0]], [[1]]],
                {'name1': [-1, -1]}
        ),
    ],
)
def test_errors3(schema, dataset, standart, errors):
    dataset = Data(dataset, schema)
    dataset.transform()
    dataset.filtered()
    print("error_values", dataset.error_values)
    print("error_rows", len(dataset.error_rows))
    print(dataset.error_rows)
    assert dataset.error_values == errors
    assert dataset.to_json() == standart


@pytest.mark.parametrize(
    "schema,dataset,standart,errors",
    [
        (
                [{"name": "name1", "type": "string", "errors": "default", "is_json": False},],
                [{"name1": "sdf"}],
                [{"name1": "sdf"}],
                {'name1': []}
        ),
    ],
)
def test_errors3(schema, dataset, standart, errors):
    dataset = Data(dataset, schema)
    dataset.transform()
    dataset.filtered()
    print("error_values", dataset.error_values)
    print("error_rows", len(dataset.error_rows))
    print(dataset.error_rows)
    assert dataset.error_values == errors
    assert dataset.to_json() == standart

# -*- coding: utf-8 -*-
import datetime as dt
from importlib.util import find_spec

import pytest

from datagun import Series, DataSet, read_text

DT_NOW = dt.datetime.now()


def wrapper_data(func):
    def wrapper():
        data = [
            ["a0", 2, 3],
            [10, "b1", 30],
            [100, 200, "c3"]
        ]
        data_shot = DataSet(data, [{"name": "a", "dtype": "string", "dt_format": None},
                                   {"name": "b", "dtype": "int", "dt_format": None},
                                   {"name": "c", "dtype": "float", "dt_format": None}], orient="columns")
        func(data_shot)

    return wrapper


@pytest.mark.parametrize(
    "dtype,values,depth,result",
    [
        ["int", ['a', 2, 3, 'a'], 0, ['1', '2', '3']],
        ["int", [[1], ['a', 1, 'a'], [3, 'a']], 1, ['1', '2', '3']],
        ["int", [[[0], [0]], [['b'], [0]]], 2, ['1', '2', '3']],
    ],
)
def test_error_values(dtype, values, depth, result):
    # TODO: непонятно как работает в случае вложеных данных
    print(dtype, values)
    series = Series(data=values, dtype=dtype, errors="coerce", dt_format="%Y-%m-%d %H:%M:%S.%f", depth=depth)
    r = series()
    print(r)
    # assert result == r
    print(series.error_values)


def test_dataframe():
    """
    :return to_values:
    1,10,100
    2,20,200
    3,30,300
    """
    if find_spec("pandas"):
        data = [
            [1, 2, 3],
            [10, 20, 30],
            [100, 200, 300]
        ]
        new_data = DataSet(data,
                           [{"name": "a", "dtype": "string", "default_value": "", "is_array": "False",
                             "dt_format": None},
                            {"name": "b", "dtype": "int", "default_value": 0, "is_array": "False", "dt_format": None},
                            {"name": "c", "dtype": "float", "default_value": 0.0, "is_array": "False",
                             "dt_format": None}],
                           orient="columns")
        df = new_data.to_dataframe()
        print(df)
        print(df.dtypes)


# TODO: проверка, чтобы после разный функций, всегда наследовались ошибки error_values
# TODO: проверка кол-во столбцов и кол-во столбцов в строках(или предупреждение, когда таких строк много)


def test_read_text():
    print(read_text("a\t\n"))
    print(read_text("a\n"))
    # print(read_text("a\t"))


@wrapper_data
def test_str(data_shot):
    print(data_shot.to_text())


@wrapper_data
def test_other_series(data_shot):
    series = data_shot[data_shot.columns[0]]
    print()
    print("size", series.size)
    print("null_count", series.null_count())
    print("error_count", series.error_count())


@wrapper_data
def test_other(data_shot):
    print()
    print("size", data_shot.size)
    print("num_rows", data_shot.num_rows)
    print("error_count", data_shot.error_count())


@wrapper_data
def test_magic_method_series(data_shot):
    series = data_shot[data_shot.columns[1]]
    series_filter_value = series == 30
    print(series_filter_value)
    print(series - 100)
    print(series + 100)
    print(series * 100)
    print(series / 100)
    print(series // 100)
    print(series % 100)
    print(series ** 100)
    print()
    print(series == 0)
    print(series != 0)
    print(series < 0)
    print(series > 0)
    print(series >= 0)
    print(series <= 0)
    print()
    print(series)
    print(series_filter_value)
    print()
    print(series == series_filter_value)
    print(~(series == series_filter_value))
    print((series | series_filter_value))
    print((series & series_filter_value))
    print()
    print(reversed(series))
    for value in series:
        print(value)


@wrapper_data
def test_transform_and_filter_schema_func(data_shot):
    data = data_shot.to_list()
    schema = [{**d, **dict(dtype=None,
                           transform_func="lambda x: str(x)*10",
                           filter_func="lambda x: True")
               }
              for d in data_shot.schema]
    ds = DataSet(data=data, schema=schema, orient="columns")
    print(ds)


@wrapper_data
def test_to_list(data_shot):
    print(data_shot.to_list())


def test_read_dict():
    data = [{i: i} for i in range(10)]
    ds = DataSet(data=data, schema=None, orient="dict")
    print(ds)
    schema = [{"name": i}
              for i in range(int(len(data) / 2))]
    # Проверяется, что парметры задаанные в DataSet,
    # пробрасываются в схему каждой колонки, во всех ориентациях данных. СДелать отдельно.
    ds = DataSet(data=data, schema=schema, orient="dict", null_value="NULL", allow_null=True)
    print(ds['0'].data())
    assert ds['0'].allow_null == True
    assert ds['0'].null_value == "NULL"
    # TODO: проверка всем параметров, проходят ли и ставятся верно


@wrapper_data
def test_1(data_shot):
    for i in data_shot:
        print(i)

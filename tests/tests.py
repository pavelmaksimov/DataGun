# -*- coding: utf-8 -*-
from data_transformer import Column
import pytest
import datetime as dt


@pytest.mark.parametrize(
    "schema,value,standart",
    [
        ({"type": "int", "errors": "default"}, None, 0),
        ({"type": "int", "errors": "default", "custom_default": 100}, None, 100),
        ({"type": "int", "errors": "ignore"}, None, None),
        ({"type": "int", "errors": "coerce"}, None, None),
        ({"type": "int", "errors": "raise"}, None, TypeError),
    ],
)
def test_errors(schema, value, standart):
    column = Column(**schema)
    v = column.transform_value(value)
    assert column.error_count == 1
    assert column.error_values == [None]
    assert standart == v


@pytest.mark.parametrize(
    "schema,value",
    [
        ({"type": "int", "errors": "raise"}, None),
    ],
)
def test_raise(schema, value):
    column = Column(**schema)
    try:
        column.transform_value(value)
        assert False
    except TypeError:
        assert True


@pytest.mark.parametrize(
    "schema,value,standart",
    [
        ({"type": "int", "is_json": True}, [1, -2], [1, -2]),
        ({"type": "int", "is_json": True}, "[1, -2]", [1, -2]),
        ({"type": "int", "is_json": True}, [None, None], [0,0]),
        ({"type": "int", "is_json": True}, ["[]"], [0]),

        ({"type": "uint", "is_json": True}, [1, -2], [1, 0]),
        ({"type": "uint", "is_json": True}, "[1, -2]", [1, 0]),
        ({"type": "uint", "is_json": True}, [None, None], [0, 0]),
        ({"type": "uint", "is_json": True}, ["[]"], [0]),

        ({"type": "float", "is_json": True}, [1, -2], [1.0, -2.0]),
        ({"type": "float", "is_json": True}, "[1, -2]", [1.0, -2.0]),
        ({"type": "float", "is_json": True}, [None, None], [0.0, 0.0]),
        ({"type": "float", "is_json": True}, ["[]"], [0.0]),

        ({"type": "string", "is_json": True}, [1, -2], ["1", "-2"]),
        ({"type": "string", "is_json": True}, "['82202']", ["82202"]),
        ({"type": "string", "is_json": True}, "[1, -2]", ["1", "-2"]),
        ({"type": "string", "is_json": True}, [None, None], ["None", "None"]),
        ({"type": "string", "is_json": True}, ["[]"], ["[]"]),
        ({"type": "string", "is_json": True}, [[]], ["[]"]),

        ({"type": "datetime", "is_json": True, "dt_format": "timestamp"}, [0,1], [dt.datetime.fromtimestamp(i) for i in range(2)]),
        ({"type": "datetime", "is_json": True, "dt_format": "timestamp"}, ["0","1"], [dt.datetime.fromtimestamp(i) for i in range(2)]),
        ({"type": "datetime", "is_json": True, "dt_format": "%Y"}, ["2019", "2019"], [dt.datetime(2019, 1, 1) for i in range(2)]),

        ({"type": "datetime", "is_json": True, "dt_format": "timestamp"}, str([0,1]), [dt.datetime.fromtimestamp(i) for i in range(2)]),
        ({"type": "datetime", "is_json": True, "dt_format": "timestamp"}, str(["0","1"]), [dt.datetime.fromtimestamp(i) for i in range(2)]),
        ({"type": "datetime", "is_json": True, "dt_format": "%Y"}, str(["2019", "2019"]), [dt.datetime(2019, 1, 1) for i in range(2)]),

        ({"type": "timestamp", "is_json": True}, ["1", "1"], [1, 1]),
        ({"type": "timestamp", "is_json": True}, [1,1], [1,1]),
        ({"type": "timestamp", "is_json": True, "dt_format": "%Y"}, ["2019", "2019"], [dt.datetime(2019, 1, 1).timestamp() for i in range(2)]),

        ({"type": "timestamp", "is_json": True}, str(["1", "1"]), [1, 1]),
        ({"type": "timestamp", "is_json": True}, str([1,1]), [1,1]),
        ({"type": "timestamp", "is_json": True, "dt_format": "%Y"}, str(["2019", "2019"]), [dt.datetime(2019, 1, 1).timestamp() for i in range(2)]),

        ({"type": "date", "is_json": True, "dt_format": "timestamp"}, [0, 1], [dt.datetime.fromtimestamp(i).date() for i in range(2)]),
        ({"type": "date", "is_json": True, "dt_format": "timestamp"}, ["0", "1"], [dt.datetime.fromtimestamp(i).date() for i in range(2)]),
        ({"type": "date", "is_json": True, "dt_format": "%Y"}, ["2019", "2019"], [dt.datetime(2019, 1, 1).date() for i in range(2)]),

        ({"type": "date", "is_json": True, "dt_format": "timestamp"}, str([0, 1]), [dt.datetime.fromtimestamp(i).date() for i in range(2)]),
        ({"type": "date", "is_json": True, "dt_format": "timestamp"}, str(["0", "1"]), [dt.datetime.fromtimestamp(i).date() for i in range(2)]),
        ({"type": "date", "is_json": True, "dt_format": "%Y"}, str(["2019", "2019"]), [dt.datetime(2019, 1, 1).date() for i in range(2)]),

    ],
)
def test_json(schema, value, standart):
    column = Column(**schema)
    print(column.transform_value(value), type(column.transform_value(value)))
    assert standart == column.transform_value(value)
    assert type(standart) == type(column.transform_value(value))


@pytest.mark.parametrize(
    "schema,value,standart",
    [
        ({"type": "int"}, -1, -1),
        ({"type": "int"}, 1, 1),
        ({"type": "int"}, "-1", -1),
        ({"type": "int"}, "1", 1),
        ({"type": "int"}, None, 0),
        ({"type": "int"}, "", 0),

        ({"type": "int", "is_json": True}, [1, -2], [1, -2]),
        ({"type": "int", "is_json": True}, "[1, -2]", [1, -2]),
        ({"type": "int", "is_json": True}, [None, None], [0,0]),
        ({"type": "int", "is_json": True}, ["[]"], [0]),

        ({"type": "uint"}, -1, 0),
        ({"type": "uint"}, 1, 1),
        ({"type": "uint"}, "-1", 0),
        ({"type": "uint"}, "1", 1),
        ({"type": "uint"}, None, 0),
        ({"type": "uint"}, "", 0),

        ({"type": "uint", "is_json": True}, [1, -2], [1, 0]),
        ({"type": "uint", "is_json": True}, "[1, -2]", [1, 0]),
        ({"type": "uint", "is_json": True}, [None, None], [0, 0]),
        ({"type": "uint", "is_json": True}, ["[]"], [0]),

        ({"type": "float"}, -1, -1.0),
        ({"type": "float"}, 1, 1.0),
        ({"type": "float"}, "-1", -1.0),
        ({"type": "float"}, "1", 1.0),
        ({"type": "float"}, None, 0.0),
        ({"type": "float"}, "", 0.0),

        ({"type": "float", "is_json": True}, [1, -2], [1.0, -2.0]),
        ({"type": "float", "is_json": True}, "[1, -2]", [1.0, -2.0]),
        ({"type": "float", "is_json": True}, [None, None], [0.0, 0.0]),
        ({"type": "float", "is_json": True}, ["[]"], [0.0]),

        ({"type": "string"}, -1, "-1"),
        ({"type": "string"}, "-1", "-1"),
        ({"type": "string"}, False, "False"),
        ({"type": "string"}, None, "None"),
        ({"type": "string"}, [], "[]"),

        ({"type": "string", "is_json": True}, [1, -2], ["1", "-2"]),
        ({"type": "string", "is_json": True}, "[1, -2]", ["1", "-2"]),
        ({"type": "string", "is_json": True}, [None, None], ["None", "None"]),
        ({"type": "string", "is_json": True}, ["[]"], ["[]"]),
        ({"type": "string", "is_json": True}, [[]], ["[]"]),

        ({"type": "datetime", "dt_format": "timestamp"}, 0, dt.datetime.fromtimestamp(0)),
        ({"type": "datetime", "dt_format": "timestamp"}, 1, dt.datetime.fromtimestamp(1)),
        ({"type": "datetime", "dt_format": "timestamp"}, "1", dt.datetime.fromtimestamp(1)),
        ({"type": "datetime", "dt_format": "%Y"}, "2019", dt.datetime(2019,1,1,0,0)),

        ({"type": "timestamp"}, 1, 1),
        ({"type": "timestamp", "dt_format": "%Y"}, "2019", dt.datetime(2019,1,1,0,0).timestamp()),

        ({"type": "date", "dt_format": "%Y"}, "2019", dt.datetime(2019,1,1).date()),
        ({"type": "date", "dt_format": "timestamp"}, 1, dt.datetime.fromtimestamp(1).date()),
        ({"type": "date", "dt_format": "timestamp"}, "1", dt.datetime.fromtimestamp(1).date()),

    ],
)
def test_1(schema, value, standart):
    column = Column(**schema)
    assert standart == column.transform_value(value)
    assert type(standart) == type(column.transform_value(value))


@pytest.mark.parametrize(
    "schema,value,standart",
    [
        ({"type": "string", "is_json": True}, "['', '1']", ['', '1']),
        ({"type": "string", "is_json": True}, "[\'\', \'1\']", ['', '1']),
        ({"type": "string", "is_json": True}, "[\\'\\', \\'1\\']", ['', '1']),
        ({"type": "string", "is_json": True}, '''["""", "'", ""'"", "''", "", "'"]''', ['""', "'", '"\'"', "''", "", "'"]),
        ({"type": "string", "is_json": True},
            '''[\'{""__ym"":{""user_id"":""""}}\',\'{""__ym"":{""user_id"":""""}}\',\'{""__ym"":{""user_id"":""""}}\']''',
            ['{""__ym"":{""user_id"":""""}}','{""__ym"":{""user_id"":""""}}','{""__ym"":{""user_id"":""""}}']),
    ],
)
def test_novalid_json(schema, value, standart):
    column = Column(**schema)
    assert standart == column.transform_value(value)
    assert type(standart) == type(column.transform_value(value))

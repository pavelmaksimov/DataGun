# -*- coding: utf-8 -*-
import datetime as dt

import pytest
import pytz

from datagun import Series


@pytest.mark.parametrize("timezone", [pytz.timezone("Europe/Moscow"), "Europe/Moscow"])
def test_to_datetime_timezone(timezone):
    data_shot = Series(data=["2020-01-01"], dtype="date", timezone=timezone, errors="raise")

    assert data_shot[0] == dt.date(2020, 1, 1)
    assert data_shot.to_datetime()[0] == dt.datetime(2020, 1, 1)
    assert data_shot.to_date()[0] == dt.date(2020, 1, 1)
    assert data_shot.to_timestamp()[0] == dt.datetime(2020, 1, 1).timestamp()


@pytest.mark.parametrize(
    "dtype,data,result",
    [
        ["string", [1, [], {}, (1,)], ['1', '[]', '{}', '[1]']],
        ["int", ['1', 2.0], [1, 2]],
        ["uint", ['1', 2.0, -1], [1, 2, 0]],
        ["float", ['1', 2], [1.0, 2.0]],
    ],
)
def test_deserialize(dtype, data, result):
    print(dtype, data)
    series = Series(data=data, dtype=dtype, allow_null=False, null_values=[])
    assert result == series.data()


@pytest.mark.parametrize(
    "dtype,data,depth,result",
    [
        ("string", [[], [1], [1, 1]], 1, [[], ['1'], ['1', '1']]),
        ("string", [[[2]], ], 2, [[['2']]]),
        ("string", [[1, 1, [2, 2, [3]]]], 1, [['1', '1', '[2, 2, [3]]']]),
        ("string", [[[2]], [[2, 2]], [[[3]]]], 2, [[['2']], [['2', '2']], [['[3]']]]),

    ],
)
def test_depth(dtype, data, depth, result):
    series = Series(data=data, dtype=dtype, depth=depth)
    assert result == series.data()

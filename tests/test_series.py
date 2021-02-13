# -*- coding: utf-8 -*-
import pytz
from datagun import Series
import pytest
import datetime as dt


@pytest.mark.parametrize("timezone", [pytz.timezone("Europe/Moscow"), "Europe/Moscow"])
def test_to_datetime_timezone(timezone):
    data_shot = Series(data=["2020-01-01"], dtype="date", timezone=timezone, errors="raise")

    assert data_shot[0] == dt.date(2020, 1, 1)
    assert data_shot.to_datetime()[0] == dt.datetime(2020, 1, 1)
    assert data_shot.to_date()[0] == dt.date(2020, 1, 1)
    assert data_shot.to_timestamp()[0] == dt.datetime(2020, 1, 1).timestamp()

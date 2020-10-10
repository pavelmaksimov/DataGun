# -*- coding: utf-8 -*-
import ast
import datetime as dt
import json
import logging
import re

from dateutil import parser as dt_parser

logging.basicConfig(level=logging.INFO)


def list_loads(value):
    def _to_list(x):
        x = re.sub(r"^\[", "", x)
        x = re.sub(r"\]$", "", x)
        x = x.replace("\\'", "'")
        # This lexer takes a JSON-like 'array' string and converts single-quoted array items into escaped double-quoted items,
        # then puts the 'array' into a python list
        # Issues such as  ["item 1", '","item 2 including those double quotes":"', "item 3"] are resolved with this lexer
        items = []  # List of lexed items
        item = ""  # Current item container
        dq = True  # Double-quotes active (False->single quotes active)
        bs = 0  # backslash counter
        in_item = (
            False
        )  # True if currently lexing an item within the quotes (False if outside the quotes; ie comma and whitespace)
        for i, c in enumerate(x):  # Assuming encasement by brackets
            if (
                    c == "\\"
            ):  # if there are backslashes, count them! Odd numbers escape the quotes...
                bs += 1
                continue
            if ((dq and c == '"') or (not dq and c == "'")) and (
                    not in_item or i + 1 == len(x) or x[i + 1] == ","
            ):  # quote matched at start/end of an item
                if (
                        bs & 1 == 1
                ):  # if escaped quote, ignore as it must be part of the item
                    continue
                else:  # not escaped quote - toggle in_item
                    in_item = not in_item
                    if item != "":  # if item not empty, we must be at the end
                        items += [item]  # so add it to the list of items
                        item = ""  # and reset for the next item
                    else:
                        if not in_item:
                            items.append("")
                    continue
            if not in_item:  # toggle of single/double quotes to enclose items
                if dq and c == "'":
                    dq = False
                    in_item = True
                elif not dq and c == '"':
                    dq = True
                    in_item = True
                continue
            if in_item:  # character is part of an item, append it to the item
                if not dq and c == '"':  # if we are using single quotes
                    item += bs * "\\" + '"'  # escape double quotes for JSON
                else:
                    item += bs * "\\" + c
                bs = 0
                continue
        return items

    def _to_json(x):
        try:
            return ast.literal_eval(x)
        except SyntaxError:
            return _to_list(x)

    return _to_json(value)


def get_schema_from_clickhouse_describe_table(describe_table, errors="default"):
    dtypes = {
        "String": "string",
        "UInt": "uint",
        "Int": "int",
        "Float": "float",
        "Decimal": "float",
        "DateTime": "datetime",
        "Date": "date",
    }
    schema = []
    for col in describe_table:
        if col[2] in ("MATERIALIZED", "ALIAS"):
            continue
        depth = col[1].count("Array")
        dtype = [v for k, v in dtypes.items()
                 if col[1].find(k) > -1]
        dtype = dtype[0] if dtype else None
        d = {
            "name": col[0],
            "type": dtype,
            "errors": errors
        }
        if dtype in ("date", "dateTime"):
            d["dt_format"] = None
        if depth > 0:
            d["depth"] = depth
        schema.append(d)
    return schema


class NormValue:
    pass


# TODO: при повторной десереализации будет ошибки выдавать,
#  чет придумать или выводить ошибку специальнцю чтоб было понятно
class ValueType:
    def __init__(self, func, errors, default_value=None, **kwargs):
        self.default_value = default_value
        self.errors = errors
        self.error_values = []
        self.func = func

    def _process_error(self, value, except_):
        if self.errors == "default":
            return self.default_value
        elif self.errors == "raise":
            raise except_
        elif self.errors == "ignore":
            return value
        elif self.errors == "coerce":
            return None

    def _check_isinstance(self, value):
        return False

    def apply(self, func, value, *args, **kwargs):
        if self._check_isinstance(value):
            return value
        else:
            try:
                result = func(value, *args, **kwargs)
            except (ValueError, TypeError) as e:
                self.error_values.append(value)
                return self._process_error(value, e)
            else:
                self.error_values.append(NormValue)
                return result

    def __call__(self, value, *args, **kwargs):
        return self.apply(self.func, value, *args, **kwargs)


class Series:
    def __init__(
            self,
            data,
            dtype=None,
            default=type,
            errors="default",
            dt_format=None,
            depth=0,
            **kwargs
    ):
        """

        :param data: str, list
        :param dtype: str
        :param default: any
        :param errors: str, coerce|raise|ignore|default
        :param is_array: bool
        :param dt_format: str, timestamp|auto|формат даты
        :param depth: int
        """
        if dtype not in (None, "string", "array", "int", "uint",
                         "float", "date", "datetime", "timestamp"):
            raise ValueError("{} = неверный dtype".format(dtype))
        if errors not in ("coerce", "raise", "ignore", "default"):
            raise ValueError("{} = неверный errors".format(errors))
        if dtype in ("date", "datetime", "timestamp") and dt_format is None:
            raise ValueError("dt_format обязателен для типа даты и/или времени ")

        self.errors = errors
        self.default = default
        self.dt_format = dt_format
        self.error_values = kwargs.get("error_values", [])
        self.depth = depth
        self._data = data
        self._dtype = dtype

        self._deserialize(data)

    def count_errors(self):
        return len(self.error_values)

    def _deserialize(self, data):
        if not isinstance(data, list):
            self._data = list_loads(data)
            if not isinstance(self._data, list):
                raise TypeError("Не удалось получить массив")
        else:
            self._data = data

        if self._dtype is not None:
            method = getattr(Series, "to_{}".format(self._dtype))
            if self.default == type:
                result = method(self, errors=self.errors, depth=self.depth)
            else:
                result = method(self, errors=self.errors, default_value=self.default, depth=self.depth)
            self._data = result.data()
            self.error_values = result.error_values

    def applymap(self, func, errors="raise", default_value=None, depth=None):
        depth = depth or self.depth
        if depth == 0:
            func_with_wrap = ValueType(func, errors, default_value)
            _data = list(map(func_with_wrap, self._data))
            error_values = func_with_wrap.error_values
        else:
            _data = []
            error_values = []
            for array in self._data:
                series = Series(array, dtype=self._dtype, errors=errors, depth=depth - 1, error_values=error_values)
                _data.append(series.data())
                error_values.append(series.error_values)
        return Series(data=_data, error_values=error_values)

    def apply(self, func, errors="raise", default_value=None):
        return self.applymap(func=func, errors=errors, default_value=default_value, depth=0)

    def to_string(self, errors="raise", default_value="", **kwargs):
        return self.applymap(func=str, errors=errors, default_value=default_value, **kwargs)

    def to_int(self, errors="raise", default_value=0, **kwargs):
        return self.applymap(func=int, errors=errors, default_value=default_value, **kwargs)

    def to_uint(self, errors="raise", default_value=0, **kwargs):
        def to_uint_func(obj):
            x = int(obj)
            return 0 if x < 0 else x

        return self.applymap(func=to_uint_func, errors=errors, default_value=default_value, **kwargs)

    def to_float(self, errors="raise", default_value=0.0, **kwargs):
        return self.applymap(func=float, errors=errors, default_value=default_value, **kwargs)

    def to_array(self, errors="raise", default_value=list, **kwargs):
        if default_value == list:
            default_value = []

        return self.applymap(func=list_loads, errors=errors, default_value=default_value, **kwargs)

    def to_datetime(self, dt_format=None, errors="raise", default_value=dt.datetime, **kwargs):
        dt_format = dt_format or self.dt_format

        if dt_format is None:
            raise ValueError("Введите параметр format")

        if default_value == dt.datetime:
            default_value = dt.datetime(1970, 1, 1, 0, 0, 0)

        def to_datetime_func(obj):
            if self.dt_format == "timestamp":
                x = int(obj)
                return dt.datetime.fromtimestamp(x)
            else:
                if dt_format == "auto":
                    return dt_parser.parse(obj)
                else:
                    return dt.datetime.strptime(obj, dt_format)

        return self.applymap(func=to_datetime_func, errors=errors, default_value=default_value, **kwargs)

    def to_date(self, dt_format=None, errors="raise", default_value=dt.date, **kwargs):
        series = self.to_datetime(dt_format=dt_format, errors=errors)
        func = lambda dt_: dt_.date()
        return series.applymap(func=func, errors=errors, default_value=default_value, **kwargs)

    def to_timestamp(self, dt_format=None, errors="raise", default_value=0, **kwargs):
        """Может десериализовать только из datetime."""
        series = self.to_datetime(dt_format=dt_format, errors=errors)
        to_timestamp_func = lambda dt_: dt_.timestamp()
        return series.applymap(func=to_timestamp_func, errors=errors, default_value=default_value, **kwargs)

    def data(self):
        return self._data

    def __add__(self, series):
        if not isinstance(series, Series):
            raise TypeError
        self._data += series._data
        self.error_values += series.error_values
        return self

    def __len__(self):
        return len(self._data)

    def __getitem__(self, key):
        data = self._data[key]
        if isinstance(key, slice):
            return Series(
                data=data,
                dtype=None,
                default=self.default,
                errors=self.errors,
                dt_format=self.dt_format,
                depth=self.depth,
                error_values=self.error_values
            )
        else:
            return data

    def __setitem__(self, key, value):
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]

    def __repr__(self):
        return self()

    def __str__(self):
        return str(self())

    def __call__(self):
        return self.data()


class DataData:
    def __init__(self, data, schema, orient="columns", **kwargs):
        """

        :param schema: [{"name": "n", "type": "int", "default": "default", "is_array": "False", "dt_format": None}]
        :param data: list, tuple
        """
        self.error_values = []
        self.error_rows = []
        self.columns = [s.get("name", i) for i, s in enumerate(schema)]
        self.schema = schema
        self._schema = {col_schema.get("name", i): col_schema
                        for i, col_schema in enumerate(schema)}
        self._orient = orient
        self._is_from_text = kwargs.get("is_from_text", False)
        self._sep = kwargs.get("sep", False)
        self.dtypes = {sch.get("name", i): sch.get("type", None)
                       for i, sch in enumerate(schema)}
        self._series = {}
        if kwargs.get("series"):
            self._series = kwargs.get("series")
        else:
            self._deserialize(data, schema, orient)

    # TODO: Отключить поддержку чтения из данных с ориентацией columns

    def _deserialize(self, data, schema, orient):
        if not isinstance(data, list):
            data = list_loads(data)
            if not isinstance(data, list):
                raise TypeError("Не удалось получить нужный тип входных данных")
            if not data:
                raise ValueError(
                    "После преобразования входных данных в нужный формат, "
                    "был получен пустой массив."
                )
        else:
            if not data:
                raise ValueError("Получен пустой массив")

        if orient == "rows":
            count_columns = len(self.columns)
            data_ = [[] for i in range(count_columns)]
            for row in data:
                if len(row) > count_columns:
                    self.error_rows.append(row)
                else:
                    for col_index, value in enumerate(row):
                        data_[col_index].append(value)
            data = data_

        for col_name, values, series_schema in zip(self.columns, data, schema):
            series = Series(values, dtype=series_schema.get("type", None))  # TODO: rename type to dtype
            self._series[col_name] = series

    def _check_count_columns_in_rows(self):
        """Выкидывание строк, в которых столбцов больше, чем объявлено"""
        pass

    def count_error_rows(self):
        return len(self.get_error_logs())

    def get_error(self):
        error_values_by_columns = self.get_error_as_dict()
        # Индексы строк, в которых есть ошибки преобразования.
        index_error_rows = set()
        for col_name in self.columns:
            index_error_rows.update(set(error_values_by_columns[col_name].keys()))

        data_orient_values = [[self._series[col_name][i] for col_name in self.columns]
                              for i in sorted(list(index_error_rows))]
        return DataData(data_orient_values, schema=self.schema, orient="rows")

    def get_error_logs(self):
        # Индексы строк, в которых есть ошибки преобразования.
        error_data = []
        for index_row in range(len(self)):
            col_names_with_error = []
            value_names_with_error = []
            dtype_with_error = []
            row = []
            for col_name in self.columns:
                series = self._series[col_name]
                value = series[index_row]
                is_error_value = series.error_values[index_row] != NormValue
                row.append(value)
                if is_error_value:
                    col_names_with_error.append(col_name)
                    value_names_with_error.append(value)
                    dtype_with_error.append(self._schema[col_name]["type"])
            if value_names_with_error:
                if self._is_from_text:
                    row = self._sep.join(row)
                else:
                    row = json.dumps(row)
                error_data.append(
                    [
                        col_names_with_error,
                        value_names_with_error,
                        dtype_with_error,
                        row,
                    ]
                )

        for i in self.error_rows:
            error_data.append(["", "", "", i])

        schema = [
            {"name": "column"},
            {"name": "value"},
            {"name": "dtype"},
            {"name": "row"},
        ]

        return DataData(error_data, schema=schema, orient="rows")

    def get_error_as_dict(self):
        error_data = {}
        for col_name in self.columns:
            error_data[col_name] = {i: v
                                    for i, v in enumerate(self._series[col_name].error_values)
                                    if v is not NormValue}
        return error_data

    def to_list(self):
        return [self._series[col_name].data() for col_name in self.columns]

    def to_values(self):
        return list(zip(*self.to_list()))

    def to_dict(self):
        return [dict(zip(self.columns, v)) for v in self.to_values()]

    def to_dataframe(self, **kwargs):
        from pandas import DataFrame
        data = {col_name: self._series[col_name].data()
                for col_name in self.columns}
        return DataFrame(data, **kwargs)

    def __add__(self, other):
        if not isinstance(other, DataData):
            raise TypeError
        if self.columns != other.columns:
            raise ValueError("Не совпадают столбцы")
        for col_name in self.columns:
            self[col_name] += other[col_name]
            self.error_rows += other.error_rows
            self.error_values += other.error_values
        return self

    def __len__(self):
        """Count rows."""
        return len(list(self._series.values())[0])

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._series[key]
        elif isinstance(key, list):
            new_series = {}
            new_schema = []
            for col in key:
                series = self[col]
                new_series[col] = series
                new_schema.append(self._schema[col])
            return DataData(data=None, schema=new_schema, series=new_series)
        elif isinstance(key, slice):
            series = {col_name: self._series[col_name][key]
                      for col_name in self.columns}
            return DataData(data=None, schema=self.schema, series=series)
        else:
            raise TypeError

    def __setitem__(self, key, value):
        if isinstance(key, str):
            try:
                self._series[key] = value
            except KeyError:
                # Новый столбец.
                self.columns.append(key)
                self._series[key] = Series(data=value)
        else:
            raise TypeError("Метод принимает название или индекс столбца")

    def __delitem__(self, key):
        del self._series[key]

    def __repr__(self):
        return self()

    def __str__(self):
        return str(self())

    def __call__(self, *args, **kwargs):
        return self.to_values()


def read_text(text, sep, schema, newline="\n"):
    data = [i.split(sep) for i in text.split(newline)]
    return DataData(data=data, schema=schema, orient="rows")

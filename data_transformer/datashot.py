# -*- coding: utf-8 -*-
import ast
import datetime as dt
import json
import logging
import re

from dateutil import parser as dt_parser

logging.basicConfig(level=logging.INFO)


def deserialize_list(text):
    """Вытащит массив из строки"""

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
            if c == "\\":
                # if there are backslashes, count them! Odd numbers escape the quotes...
                bs += 1
                continue
            if (
                    ((dq and c == '"') or (not dq and c == "'"))
                    and (not in_item or i + 1 == len(x) or x[i + 1] == ",")
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

    return _to_json(text)


# TODO: параметр пропуска строк, если в первом строке, названия например
def read_text(text, sep="\t", schema=None, newline="\n", skip_begin_lines=0, skip_end_lines=0):
    data = [i.split(sep) for i in text.split(newline)]
    data = data[skip_begin_lines:-1-skip_end_lines]

    if schema is None:
        schema = [{}] * len(data[0])

    return DataShot(data=data, schema=schema, orient="rows")


class FunctionWrapper:
    def __init__(self, func, errors, default_value=None):
        self.default_value = default_value
        self.errors = errors
        self.error_values = []
        self.func = func
        self.run_number = 0

    def _process_error(self, value, except_):
        if self.errors == "default":
            return self.default_value
        elif self.errors == "raise":
            raise except_
        elif self.errors == "ignore":
            return value
        elif self.errors == "coerce":
            return None

    def apply(self, func, value, *args, **kwargs):
        try:
            result = func(value, *args, **kwargs)
        except (ValueError, TypeError) as e:
            self.error_values.append(
                (self.run_number, value)
            )
            return self._process_error(value, e)
        else:
            return result
        finally:
            self.run_number += 1

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
        self._dtype = dtype
        self._data = data

        self._deserialize(data)

    def count_errors(self):
        return len(self.error_values)

    def _deserialize(self, data):
        if not isinstance(data, list):
            raise TypeError("Параметр data должен быть массивом")

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
            func_with_wrap = FunctionWrapper(func=func, errors=errors, default_value=default_value)
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
        to_int_func = lambda obj: default_value if obj in ("", None) else int(obj)
        return self.applymap(func=to_int_func, errors=errors, default_value=default_value, **kwargs)

    def to_uint(self, errors="raise", default_value=0, **kwargs):
        def to_uint_func(obj):
            obj = default_value if obj == "" else obj
            x = int(obj)
            return 0 if x < 0 else x

        return self.applymap(func=to_uint_func, errors=errors, default_value=default_value, **kwargs)

    def to_float(self, errors="raise", default_value=0.0, **kwargs):
        to_float_func = lambda obj: default_value if obj in ("", None) else float(obj)

        return self.applymap(func=to_float_func, errors=errors, default_value=default_value, **kwargs)

    def to_array(self, errors="raise", default_value=list, **kwargs):
        if default_value == list:
            default_value = []

        def func(obj):
            if obj in ("", None):
                return default_value
            elif not isinstance(obj, list):
                return deserialize_list(obj)
            else:
                return obj

        return self.applymap(func=func, errors=errors, default_value=default_value, **kwargs)

    def to_datetime(self, dt_format=None, errors="raise", default_value=dt.datetime, **kwargs):
        """

        :param dt_format: str
            - "timestamp" = преобразует число или число строке в дату и время
            - "auto" = спарсит автоматически из строки
            - format из strptime
        :param errors:
        :param default_value:
        :param kwargs:
        :return:
        """
        dt_format = dt_format or self.dt_format

        if dt_format is None:
            raise ValueError("Введите параметр dt_format")

        if default_value == dt.datetime:
            default_value = dt.datetime(1970, 1, 1, 0, 0, 0)

        def to_datetime_func(obj):
            if obj in ("", None):
                return default_value
            elif isinstance(obj, dt.datetime):
                return obj
            elif dt_format == "timestamp":
                x = int(obj)
                return dt.datetime.fromtimestamp(x)
            elif dt_format == "auto":
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

    def replace(self, old, new, count=None, errors="raise", **kwargs):
        # TODO: тест
        replace_func = lambda obj: str(obj).replace(old, new, count)
        return self.applymap(func=replace_func, errors=errors, **kwargs)

    def has(self, sub, start=None, end=None, errors="raise", **kwargs):
        has_func = lambda obj: str(obj).count(sub, start, end) > 0
        return self.applymap(func=has_func, errors=errors, **kwargs)

    def isin(self, l, errors="raise", **kwargs):
        isin_func = lambda obj: obj in l
        return self.applymap(func=isin_func, errors=errors, **kwargs)

    def is_identical(self, value, errors="raise", **kwargs):
        # TODO: тест
        is_identical_func = lambda obj: obj == value
        return self.applymap(func=is_identical_func, errors=errors, **kwargs)

    def is_instance(self, A_tuple, errors="raise", **kwargs):
        # TODO: тест
        has_func = lambda obj: isinstance(obj, A_tuple)
        return self.applymap(func=has_func, errors=errors, **kwargs)

    def is_not_instance(self, A_tuple, errors="raise", **kwargs):
        # TODO: тест
        has_func = lambda obj: not isinstance(obj, A_tuple)
        return self.applymap(func=has_func, errors=errors, **kwargs)

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

    def __str__(self):
        if len(self) > 20:
            return str(self.data()[:10] + ["..."] + self.data()[-10:])
        return str(self.data())

    def __call__(self):
        return self.data()


class DataShot:
    def __init__(self, data, schema, orient="columns", **kwargs):
        """
        TODO: Придумать как, обойтись без schema. Типа посчитать медиану столбцов, исходя из этого получить столбцы.
        TODO: Атрибут size размер данных, чтоб в статистике печатать.
        TODO: Подача на вход в виде строк словарей. Тогда и схема не нужна.
        TODO: Проверка, или названия столбцов должны быть у всех или ни у кого, иначе ошибка. Проверка схемы должна быть
        TODO: Отключить поддержку чтения из данных с ориентацией columns
        TODO: Какая-то проверка нужна на согласование данных со схемой
        TODO: Метод получения статистики по ошибкам
        TODO: Когда применяется стандартная фнукция преобразования, то добавлять тип данных в схему,
         чтобы потом по ней десериализовать и напротив когда применяется неизвестная функция удалять из схемы тип

        :param schema: [{"name": "n", "type": "int", "default": "default", "is_array": "False", "dt_format": None}]
        :param data: list, tuple
        """
        self.error_values = []
        self.error_rows = []
        self.columns = [s.get("name", i) for i, s in enumerate(schema)]
        self._schema = schema
        self._schema_dict = {col_schema.get("name", i): col_schema
                             for i, col_schema in enumerate(schema)}
        self._orient = orient
        self.dtypes = {sch.get("name", i): sch.get("type", None)
                       for i, sch in enumerate(schema)}
        if kwargs.get("series"):
            self._series = kwargs.get("series")
        else:
            self._series = {}
            self._deserialize(data, schema, orient)

    def _deserialize(self, data, schema, orient):
        if not isinstance(data, list):
            raise TypeError("Параметр data должен быть массивом")

        if not data:
            raise ValueError("Пустой массив принять пока не могу")

        if orient == "rows":
            data = self._values_from_rows_to_columns(data)

        for col_name, values, series_schema in zip(self.columns, data, schema):
            series = Series(values, dtype=series_schema.get("type", None))  # TODO: rename type to dtype
            self[col_name] = series

    def _values_from_rows_to_columns(self, data):
        count_columns = len(self.columns)
        data_orient_column = [[] for i in range(count_columns)]
        for row in data:
            if len(row) != count_columns:
                # Выкидывание строки, в которой кол-во столбцов отличается.
                self.error_rows.append(row)
            else:
                for col_index, value in enumerate(row):
                    data_orient_column[col_index].append(value)
        return data_orient_column

    def count_error_rows(self):
        return len(self.get_errors())

    def _get_index_error_rows(self):
        """Возвращает список из индексов строк, в которых были ошибки преобразования."""
        index_error_rows = set()
        for series in self._series.values():
            index_error_rows.update(
                {i[0] for i in series.error_values}
            )
        return sorted(list(index_error_rows))

    def get_errors(self):
        index_error_rows = self._get_index_error_rows()
        data = []
        for i in index_error_rows:
            index_columns = []
            row = list(self[i:i + 1].to_values()[0])
            for col_index, col_name in enumerate(self.columns):
                error_value = dict(self[col_name].error_values).get(i)
                if error_value:
                    index_columns.append(col_index)
                    row[col_index] = error_value
            data.append([index_columns, row])

        for row in self.error_rows:
            data.append([[], row])
        # TODO: когда ошибок нет, выходит ошибка при создании DataShot т.к. он не умеет принимать пустой
        return DataShot(data, schema=[{"name": "col_index"}, {"name": "data"}], orient="rows")

    def to_list(self):
        return [self[col_name].data() for col_name in self.columns]

    def to_values(self):
        return list(zip(*self.to_list()))

    def to_dict(self):
        return [dict(zip(self.columns, v)) for v in self.to_values()]

    def to_dataframe(self, **kwargs):
        from pandas import DataFrame
        data = {col_name: self[col_name].data()
                for col_name in self.columns}
        return DataFrame(data, **kwargs)

    def to_text(self, sep="\t", new_line="\n"):
        # TODO: тут надо сериализовать, а не просто в текст выводить
        func = lambda row: sep.join(map(str, row))
        string_rows = list(map(func, self.to_values()))
        data = new_line.join(string_rows)
        return data

    def __add__(self, other):
        if not isinstance(other, DataShot):
            raise TypeError
        if self.columns != other.columns:
            raise ValueError("Не совпадают столбцы")
        for col_name in self.columns:
            self[col_name] += other[col_name]
            self.error_rows += other.error_rows
            self.error_values += other.error_values
        return self

    def append(self, data):
        return self + DataShot(data, schema=self._schema, orient="rows")

    def __len__(self):
        """Count rows."""
        for series in self._series.values() or [[]]:
            return len(series)

    def __getitem__(self, key):
        if isinstance(key, (str, int)):
            return self._series[key]
        elif isinstance(key, list):
            new_series = {}
            new_schema = []
            for col in key:
                new_series[col] = self[col]
                new_schema.append(self._schema_dict[col])
            return DataShot(data=None, schema=new_schema, series=new_series)
        elif isinstance(key, slice):
            series = {col_name: self[col_name][key]
                      for col_name in self.columns}
            return DataShot(data=None, schema=self._schema, series=series)
        else:
            raise TypeError

    def __setitem__(self, key, value):
        if isinstance(key, (str, int)):
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

    def __str__(self):
        cols = "\t".join(map(str, self.columns))
        numbers = 10
        if len(self) > numbers*2:
            return "{}\n{}\n...\n{}".format(cols, str(self[:numbers]), str(self[-numbers:]))
        else:
            return "{}\n{}".format(cols, self.to_text())

    def _repr_html_(self):
        """
        Return a html representation for a particular DataShot.

        Mainly for IPython notebook.
        """
        return str(self)

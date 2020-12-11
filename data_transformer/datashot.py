# -*- coding: utf-8 -*-
import ast
import datetime as dt
import json
import sys
import logging
import re

from dateutil import parser as dt_parser

logging.basicConfig(level=logging.INFO)

# TODO: формирование схема из строки в данных, как кликхаус читает столбцы и типы из csv
# TODO: Замена значений, через указание в конфиге столбца

ONLY_SERIES_ERROR = "Только Series"
NULL_VALUES = {None, "", "NULL", "none", "None"}


class dtype_default_value:
    def __repr__(self):
        return "dtype_default_value"

    def __str__(self):
        return "dtype_default_value"


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

    return _to_json(text)


def read_text(
    text,
    sep="\t",
    schema=None,
    newline="\n",
    skip_blank_lines=True,
    skip_begin_lines=0,
    skip_end_lines=0,
):
    data = [i.split(sep) for i in text.split(newline)]
    data = data[skip_begin_lines : -1 - skip_end_lines]
    if skip_blank_lines:
        data = [i for i in data if i]

    return DataShot(data=data, schema=schema, orient="rows")


class DataGun:
    # TODO: remove dtype_default_value
    def __init__(
        self,
        func,
        errors,
        null=False,
        null_value=None,
        default_value=dtype_default_value,
        null_values=None,
        clear_values=None,
        **kwargs
    ):
        self.default_value = default_value
        self.null = null
        self.null_value = null_value
        self.errors = errors
        self.error_values = {}
        self.func = func
        self._run_number = 0
        self.null_values = null_values or NULL_VALUES
        self.clear_values = clear_values or {}

    def _process_error(self, value, except_):
        if self.errors == "default":
            if self.default_value == dtype_default_value:
                raise NotImplementedError(
                    "При параметре errors='default', требуется параметр default_value"
                )
            return self.default_value
        elif self.errors == "raise":
            raise except_
        elif self.errors == "ignore":
            return value
        elif self.errors == "coerce":
            return None

    def apply(self, func, obj, *args, **kwargs):
        if not isinstance(obj, list) and obj in self.clear_values:
            if self.default_value != dtype_default_value:
                return self.default_value
            else:
                obj = self.null_value

        if self.null and obj in self.null_values:
            return self.null_value

        try:
            result = func(obj, *args, **kwargs)
        except Exception as e:
            self.error_values[self._run_number] = obj
            return self._process_error(obj, e)
        else:
            return result
        finally:
            self._run_number += 1

    def __call__(self, value, *args, **kwargs):
        return self.apply(self.func, value, *args, **kwargs)


class SeriesMagicMethod:
    _schema = None

    def data(self):
        return []

    def __add__(self, obj):
        "Сложение."
        if isinstance(obj, Series):
            data = [val1 + val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value + obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __sub__(self, obj):
        "Вычитание."
        if isinstance(obj, Series):
            data = [val1 - val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value - obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __mul__(self, obj):
        "Умножение."
        if isinstance(obj, Series):
            data = [val1 * val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value * obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __floordiv__(self, obj):
        "Целочисленное деление, оператор //."
        if isinstance(obj, Series):
            data = [val1 // val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value // obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __truediv__(self, obj):
        "Деление, оператор /."
        if isinstance(obj, Series):
            data = [val1 / val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value / obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __mod__(self, obj):
        "Остаток от деления, оператор %."
        if isinstance(obj, Series):
            data = [val1 % val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value % obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __pow__(self, obj):
        "Возведение в степень, оператор **."
        if isinstance(obj, Series):
            data = [val1 ** val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value ** obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __and__(self, obj):
        "Двоичное И, оператор &."
        if isinstance(obj, Series):
            data = [all([val1, val2]) for val1, val2 in zip(self.data(), obj.data())]
        else:
            raise ValueError(ONLY_SERIES_ERROR)
        return Series(**self._schema, data=data)

    def __or__(self, obj):
        "Двоичное ИЛИ, оператор |"
        if isinstance(obj, Series):
            data = [any([val1, val2]) for val1, val2 in zip(self.data(), obj.data())]
        else:
            raise ValueError(ONLY_SERIES_ERROR)
        return Series(**self._schema, data=data)

    def __invert__(self):
        "Определяет поведение для инвертирования оператором ~."
        data = [not value for value in self.data()]
        return Series(**self._schema, data=data)

    def __eq__(self, obj):
        """Определяет поведение оператора равенства, ==."""
        if isinstance(obj, Series):
            data = [val1 == val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value == obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __ne__(self, obj):
        """Определяет поведение оператора неравенства, !=."""
        if isinstance(obj, Series):
            data = [val1 != val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value != obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __lt__(self, obj):
        """Определяет поведение оператора меньше, <."""
        if isinstance(obj, Series):
            data = [val1 < val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value < obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __gt__(self, obj):
        """Определяет поведение оператора больше, >."""
        if isinstance(obj, Series):
            data = [val1 > val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value > obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __le__(self, obj):
        """Определяет поведение оператора меньше или равно, <=."""
        if isinstance(obj, Series):
            data = [val1 <= val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value <= obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __ge__(self, obj):
        """Определяет поведение оператора больше, >=."""
        if isinstance(obj, Series):
            data = [val1 >= val2 for val1, val2 in zip(self.data(), obj.data())]
        else:
            data = [value >= obj for value in self.data()]
        return Series(**self._schema, data=data)

    def __reversed__(self):
        return Series(**self._schema, data=list(reversed(self.data())))

    def __iter__(self):
        self._it = (i for i in self.data())
        return self

    def __next__(self):
        return next(self._it)


class Series(SeriesMagicMethod):
    def __init__(
        self,
        data=None,
        dtype=None,
        errors="default",
        null=False,
        null_value=None,
        null_values=None,
        dt_format=None,
        depth=0,
        default=dtype_default_value,
        name=None,
        transform_func=None,
        filter_func=None,
        clear_values=None,
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
        if dtype in ("date", "datetime", "timestamp", "auto") and dt_format is None:
            raise ValueError("dt_format обязателен для типа даты и/или времени")

        self.null_value = null_value
        self.null = null or ("nullable" in dtype.lower() if dtype else False)
        self.null_values = null_values or NULL_VALUES.union({null_value})
        self.errors = errors
        self.depth = depth or (dtype.lower().count("array") if dtype else depth)
        self.name = name
        self._default_value = default
        self._dtype = self._parse_dtype(dtype)
        self._dt_format = dt_format
        self._data = data
        self._clear_values = clear_values
        self.error_values = kwargs.pop("error_values", {})  # TODO: wrap property

        if transform_func is None:
            self._transform_func = None
        elif isinstance(transform_func, list):
            self._transform_func = [
                eval(f) if isinstance(f, str) else f for f in transform_func
            ]
        elif isinstance(transform_func, str):
            self._transform_func = [eval(transform_func)]
        else:
            self._transform_func = [transform_func]

        if filter_func is None:
            self._filter_func = None
        elif isinstance(filter_func, list):
            self._filter_func = [
                eval(f) if isinstance(f, str) else f for f in filter_func
            ]
        elif isinstance(filter_func, str):
            self._filter_func = [eval(filter_func)]
        else:
            self._filter_func = [filter_func]

        self._deserialize(data)

    @staticmethod
    def _parse_dtype(dtype):
        if dtype and "(" in dtype:
            index_open = "".join(reversed(dtype)).find("(")
            index_close = dtype.find(")")
            return dtype[-index_open:index_close]
        else:
            return dtype

    @property
    def _schema(self):
        return {
            "errors": self.errors,
            "depth": self.depth,
            "null": self.null,
            "null_value": self.null_value,
            "null_values": self.null_values,
            "name": self.name,
        }

    def get_schema(self, **kwargs):
        return {**self._schema, **kwargs}

    def _deserialize(self, data):
        if data is None:
            return
        elif not isinstance(data, list):
            raise TypeError("Параметр data должен быть массивом")
        elif not data:
            self._data = []
            return

        if self._dtype is not None:
            method = getattr(Series, "to_{}".format(self._dtype))
            if self._default_value == dtype_default_value:
                series = method(self, errors=self.errors, depth=self.depth)
            else:
                series = method(
                    self,
                    errors=self.errors,
                    depth=self.depth,
                    default_value=self._default_value,
                )
            self._data = series.data()
            self.error_values = series.error_values
        else:
            self._data = data
            if self.null or self._clear_values:
                repalce_values = set()
                if self.null:
                    repalce_values.update(set(self.null_values))
                if self._clear_values:
                    repalce_values.update(set(self._clear_values))
                self._data = [
                    self.null_value if obj in repalce_values else obj for obj in self
                ]

        if self._transform_func is not None:
            for func in self._transform_func:
                self._data = self.applymap(func).data()

        if self._filter_func is not None:
            for func in self._transform_func:
                self._data = self.filter(self.applymap(func)).data()

    def applymap(
        self, func, errors=None, default_value=dtype_default_value, depth=None
    ):
        if depth is None:
            depth = self.depth

        if errors is None:
            errors = self.errors

        if default_value == dtype_default_value:
            default_value = self._default_value

        # TODO: Вынести логику в FunctionWrapper.
        if depth == 0:
            func_with_wrap = DataGun(
                **self.get_schema(
                    func=func,
                    errors=errors,
                    default_value=default_value,
                    clear_values=self._clear_values,
                )
            )
            self._data = list(map(func_with_wrap, self))
            error_values = {**func_with_wrap.error_values, **self.error_values}
        else:
            error_values = {}
            for i, array in enumerate(self):
                series = Series(
                    **self.get_schema(
                        data=array,
                        dtype=self._dtype,
                        default=default_value,
                        errors=errors,
                        depth=depth - 1,
                        error_values=self.error_values,
                        clear_values=self._clear_values,
                    )
                )
                self._data[i] = series.data()
                if series.error_values:
                    error_values[i] = array

        return Series(
            **self.get_schema(
                data=self._data, depth=depth, errors=errors, error_values=error_values
            )
        )

    def apply(self, func, errors=None, default_value=None):
        return self.applymap(
            func=func, errors=errors, default_value=default_value, depth=0
        )

    def to_string(self, errors=None, default_value="", **kwargs):
        to_str_func = lambda obj: json.dumps(obj)
        return self.applymap(
            func=to_str_func, errors=errors, default_value=default_value, **kwargs
        )

    def to_int(self, errors=None, default_value=0, **kwargs):
        to_int_func = lambda obj: int(obj)
        return self.applymap(
            func=to_int_func, errors=errors, default_value=default_value, **kwargs
        )

    def to_uint(self, errors=None, default_value=0, **kwargs):
        def to_uint_func(obj):
            x = int(obj)
            if x < 0:
                raise ValueError("Число {} меньше 0".format(x))
            return x

        return self.applymap(
            func=to_uint_func, errors=errors, default_value=default_value, **kwargs
        )

    def to_float(self, errors=None, default_value=0.0, **kwargs):
        to_float_func = lambda obj: float(obj)
        return self.applymap(
            func=to_float_func, errors=errors, default_value=default_value, **kwargs
        )

    def to_array(self, errors=None, default_value=list, **kwargs):
        default_value = [] if default_value == list else default_value

        def func(obj):
            if not isinstance(obj, list):
                return deserialize_list(obj)
            else:
                return obj

        return self.applymap(
            func=func, errors=errors, default_value=default_value, **kwargs
        )

    def to_datetime(
        self, dt_format=None, errors=None, default_value=dt.datetime, **kwargs
    ):
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
        dt_format = dt_format or self._dt_format

        if dt_format is None:
            raise ValueError("Введите параметр dt_format")

        if default_value == dt.datetime:
            default_value = dt.datetime(1970, 1, 1, 0, 0, 0)

        def to_datetime_func(obj):
            if isinstance(obj, dt.datetime):
                return obj
            elif dt_format == "timestamp":
                x = int(obj)
                return dt.datetime.fromtimestamp(x)
            elif dt_format == "auto":
                return dt_parser.parse(obj)
            else:
                return dt.datetime.strptime(obj, dt_format)

        return self.applymap(
            func=to_datetime_func, errors=errors, default_value=default_value, **kwargs
        )

    def to_date(self, dt_format=None, errors=None, default_value=dt.date, **kwargs):
        if default_value == dt.datetime:
            default_value = dt.datetime(1970, 1, 1)

        series = self.to_datetime(dt_format=dt_format, errors=errors)
        func = lambda dt_: dt_.date()
        return series.applymap(
            func=func, errors=errors, default_value=default_value, **kwargs
        )

    def to_timestamp(self, dt_format=None, errors=None, default_value=0, **kwargs):
        """Может десериализовать только из datetime."""
        series = self.to_datetime(dt_format=dt_format, errors=errors)
        to_timestamp_func = lambda dt_: dt_.timestamp()
        return series.applymap(
            func=to_timestamp_func, errors=errors, default_value=default_value, **kwargs
        )

    def replace_str(self, old, new, count=None, **kwargs):
        # TODO: тест
        replace_func = lambda obj: str(obj).replace(old, new, count)
        return self.applymap(func=replace_func, **kwargs)

    def replace_values(self, old_values, new_value, **kwargs):
        # TODO: тест
        replace_func = lambda obj: new_value if obj in old_values else obj
        return self.applymap(func=replace_func, **kwargs)

    def has(self, value, **kwargs):
        has_func = lambda obj: value in obj
        return self.applymap(func=has_func, **kwargs)

    def isin(self, value, **kwargs):
        has_func = lambda obj: obj in value
        return self.applymap(func=has_func, **kwargs)

    def filter(self, series):
        return Series(
            **series.get_schema(
                data=[i for i, f in zip(self.data(), series.data()) if f],
                error_values=series.error_values,
            )
        )

    def error_count(self):
        return len(self.error_values)

    def null_count(self):
        return len(self.filter(self == None))

    def size(self):
        return sys.getsizeof(self.data())

    def append(self, series):
        if isinstance(series, Series):
            data = self._data + series._data
            # TODO: правильно складывать ошибки
            error_values = self.error_values + series.error_values
            return Series(
                **self.get_schema(
                    data=data, dtype=self._dtype, error_values=error_values
                )
            )
        else:
            raise TypeError

    def data(self):
        return self._data

    def __len__(self):
        return len(self._data)

    def __getitem__(self, key):
        data = self._data[key]
        if isinstance(key, slice):
            return Series(**self.get_schema(data=data, error_values=self.error_values))
        else:
            return data

    def __setitem__(self, key, value):
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]

    def __str__(self):
        return json.dumps(self.data())

    def __repr__(self):
        if len(self) > 20:
            return str(self.data()[:10] + ["..."] + self.data()[-10:])
        return str(self.data())

    def _repr_html_(self):
        """
        Return a html representation for a particular DataShot.

        Mainly for IPython notebook.
        """
        return self.__repr__()

    def __iter__(self):
        self._it = (value for value in self._data)
        return self

    def __next__(self):
        return next(self._it)

    def __call__(self):
        return self.data()


class DataShot:
    def __init__(self, data=None, schema=None, orient="rows", **kwargs):
        self.error_rows = []

        if data is None and schema:
            data = [[] for i in range(len(schema))]
        elif data is None:
            data = []
        elif orient == "series":
            pass
        elif not isinstance(data, list):
            raise TypeError("Параметр data должен быть массивом")

        if schema:
            self._schema = schema
        else:
            self._schema = []
            if data:
                if orient == "columns":
                    self._schema = [{} for i in range(len(data))]
                elif orient == "rows":
                    self._schema = [{} for i in range(len(data[0]))]
        # Set params in all series schema.
        series_params = {"errors", "depth", "null", "null_value", "null_values", "clear_values"}
        have_series_params = set(kwargs.keys()).intersection(series_params)
        for s in self._schema:
            for series_param in have_series_params:
                s[series_param] = kwargs[series_param]

        self._series = []
        self._deserialize(data, orient)

    @property
    def schema(self):
        return [series.get_schema() for series in self]

    @property
    def columns(self):
        return [series.name for series in self]

    def _get_error_index_rows(self):
        """Возвращает список из индексов строк, в которых были ошибки преобразования."""
        index_error_rows = set()
        for series in self:
            index_error_rows.update(set(series.error_values.keys()))
        return sorted(list(index_error_rows))

    def _dict_orient_data_to_columns(self, data):
        if self._schema:
            try:
                column_names = [i["name"] for i in self._schema]
            except KeyError:
                raise KeyError(
                    "Если данные находятся в словаре и есть схема, "
                    "то в схеме должны быть имена столбцов."
                )
            data_orient_column = [[] for i in range(len(column_names))]
            for row in data:
                for col_index, col_name in enumerate(column_names):
                    data_orient_column[col_index].append(row.get(col_name, None))
        else:
            data_orient_column = []
            column_names = []
            for row_index, row in enumerate(data):
                for col_name, col_value in row.items():
                    # Появление нового столбца.
                    if col_name not in column_names:
                        # Добавление названия нового столбца.
                        column_names.append(col_name)
                        # Добавление пустых данных в предыдущие строки.
                        data_orient_column.append(
                            [None for i in range(row_index)] or []
                        )
                    col_index = column_names.index(col_name)
                    data_orient_column[col_index].append(col_value)
                # Добавление пустых значений в столбцы, которые отсутствуют в строке.
                for col_index, col_name in enumerate(column_names):
                    if col_name not in row.keys():
                        data_orient_column[col_index].append(None)

            # TODO: Вынести из этой функции.
            self._schema = [{"name": col_name} for col_name in column_names]

        return data_orient_column

    def _rows_orient_data_to_columns(self, data):
        count_columns = len(self._schema)
        data_orient_column = [[] for i in range(count_columns)]
        for row in data:
            if len(row) != count_columns:
                # Выкидывание строки, в которой кол-во столбцов отличается.
                self.error_rows.append(row)
            else:
                for col_index, value in enumerate(row):
                    data_orient_column[col_index].append(value)

        return data_orient_column

    def _deserialize(self, data, orient):
        if orient == "series":
            for i, series in enumerate(data):
                series.name = series.name or str(i)
                self._series.append(series)
            return
        # TODO: а что если ли не перекладывать данные в столбцы? даже ограничив функциональность?
        elif data and orient == "rows":
            data = self._rows_orient_data_to_columns(data)
        elif data and orient == "dict":
            data = self._dict_orient_data_to_columns(data)

        col_index_list = range(len(self._schema))
        for col_index, values, series_schema in zip(col_index_list, data, self._schema):
            series_schema["name"] = str(series_schema.get("name", col_index))
            series_schema["dtype"] = series_schema.get("dtype", None)
            self.add_series(Series(values, **series_schema))

        self.print_stats(print_zero=False)

    def rename_columns(self, new_columns):
        """
        Переименование столбцов.

        :param new_columns: dict : {..., "old_name": "new_name"}
        :return: None
        """
        for series in self:
            if series.name in new_columns.keys():
                series.name = new_columns[series.name]

    def print_stats(self, print_zero=True):
        if print_zero or len(self.error_rows) > 0:
            logging.warning(
                "Не вошло строк из-за того, что кол-во столбцов в строке отличается: {}".format(
                    len(self.error_rows)
                )
            )
        for series in self:
            if len(series.error_values) > 0:
                logging.warning(
                    "Кол-во значений преобразованных в значение по умолчанию: {}={}".format(
                        series.name, len(series.error_values)
                    )
                )

    def error_count(self):
        return len(self.get_errors())

    def get_errors(self):
        index_error_rows = self._get_error_index_rows()
        data = []
        for i in index_error_rows:
            index_columns = []
            row = list(self[i : i + 1].to_values()[0])
            for col_index, col_name in enumerate(self.columns):
                error_value = dict(self[col_name].error_values).get(i)
                if error_value:
                    index_columns.append(col_index)
                    row[col_index] = error_value
            data.append([index_columns, row])

        for row in self.error_rows:
            data.append([[], row])

        return DataShot(
            data,
            schema=[{"name": "error_column_index"}, {"name": "row_data"}],
            orient="rows",
        )

    def to_list(self):
        return [series.data() for series in self]

    def to_values(self):
        return list(zip(*self.to_list()))

    def to_dict(self):
        return [dict(zip(self.columns, v)) for v in self.to_values()]

    def to_series(self):
        return list(self)

    def to_dataframe(self, **kwargs):
        from pandas import DataFrame

        data = {series.name: series.data() for series in self}
        return DataFrame(data, **kwargs)

    def to_text(self, sep="\t", new_line="\n", add_column_names=True):
        func = lambda row: sep.join(map(json.dumps, row))
        text = new_line.join(map(func, self.to_values()))

        if add_column_names:
            columns = "\t".join(self.columns)
            return "{}\n{}".format(columns, text)
        else:
            return text

    def filter(self, filter_series):
        ds = DataShot()
        for series in self:
            ds[series.name] = series.filter(filter_series).data()
        return ds

    def append(self, other):
        return self + other

    def size(self):
        return sys.getsizeof(self._series)

    def num_rows(self):
        return len(self)

    def add_series(self, series):
        col_name = len(self.columns) if series.name is None else series.name
        self[col_name] = series

    def add_column(self, data, **kwargs):
        if not isinstance(data, list):
            raise Exception
        elif len(data) != len(self):
            raise Exception

        self.add_series(Series(data=data, **kwargs))

    def __add__(self, other_DataShot):
        if not isinstance(other_DataShot, DataShot):
            raise TypeError

        if self.columns != other_DataShot.columns:
            raise ValueError("Не совпадают столбцы")

        ds = DataShot()
        for series in self:
            ds.add_series(series.append(other_DataShot[series.name]))
        ds.error_rows = self.error_rows + other_DataShot.error_rows

        return ds

    def __len__(self):
        """Count rows."""
        for series in self:
            return len(series)
        return 0

    def __getitem__(self, key):
        if isinstance(key, list):
            if not set(self.columns).issuperset(set(key)):
                raise ValueError()
            ds = DataShot()
            for col_name in key:
                ds.add_series(self[col_name])
            return ds
        elif isinstance(key, slice):
            ds = DataShot()
            for series in self:
                ds.add_series(series[key])
            return ds

        try:
            return self._series[self.columns.index(key)]
        except ValueError:
            raise ValueError("Столбец с таким именем отсутствует")

    def __setitem__(self, key, value):
        if len(self) == 0 or len(self) == len(value):
            if not isinstance(value, Series):
                value = Series(data=value, name=key)

            if key in self.columns:
                self._series[self.columns.index(key)] = value
            else:
                self._series.append(value)

        else:
            raise Exception(
                "Кол-во строк не совпадает. Добавить новый столбец можно, только той же длины."
            )

    def __delitem__(self, key):
        # TODO: test
        del self[key]

    def iter_rows(self):
        for row in zip(*self._series):
            yield row

    def __iter__(self):
        self._it = (series for series in self._series)
        return self

    def __next__(self):
        return next(self._it)

    def __str__(self):
        return self.to_text()

    def __repr__(self):
        cols = "\t".join(map(str, self.columns))
        numbers = 10
        if len(self) > numbers * 2:
            return "{}\n{}\n...\n{}".format(
                cols, str(self[:numbers]), str(self[-numbers:])
            )
        else:
            if cols:
                return "{}\n{}".format(cols, self.to_text())
            else:
                return "(empty)"

    def _repr_html_(self):
        """
        Return a html representation for a particular DataShot.

        Mainly for IPython notebook.
        """
        return self.__repr__()

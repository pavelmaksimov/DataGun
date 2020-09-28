# -*- coding: utf-8 -*-
import ast
import datetime as dt
import logging
import re


logging.basicConfig(level=logging.INFO)


class ValueType:
    def __init__(self, default_value, errors, **kwargs):
        self.default_value = default_value
        self.errors = errors
        self.error_values = []
        self.func = NotImplementedError

    def _process_error(self, value, except_):
        if self.errors == "default":
            self.error_values.append(value)
            return self.default_value
        elif self.errors == "raise":
            logging.error("Ошибка преобразования")
            raise except_
        elif self.errors == "ignore":
            self.error_values.append(value)
            return value
        elif self.errors == "coerce":
            self.error_values.append(value)
            return None

    def _check_isinstance(self, value):
        return False

    def _deserialize(self, x, *args, **kwargs):
        if self._check_isinstance(x):
            return x
        else:
            try:
                result = self.func(x, *args, **kwargs)
            except (ValueError, TypeError) as e:
                return self._process_error(x, e)
            else:
                return result

    def __call__(self, value, *args, **kwargs):
        return self._deserialize(value, *args, **kwargs)


class String(ValueType):
    def __init__(self, default_value="", errors="raise", **kwargs):
        super().__init__(default_value=default_value, errors=errors, **kwargs)
        self.func = str


class Integer(ValueType):
    def __init__(self, default_value=0, errors="raise", **kwargs):
        super().__init__(default_value=default_value, errors=errors, **kwargs)
        self.func = int


class Floating(ValueType):
    def __init__(self, default_value=0, errors="raise", **kwargs):
        super().__init__(default_value=default_value, errors=errors, **kwargs)
        self.func = float


class Boolean(ValueType):
    def __init__(self, default_value=0, errors="raise", **kwargs):
        super().__init__(default_value=default_value, errors=errors, **kwargs)
        self.func = bool


class Array(ValueType):
    _default = []
    def __init__(self, errors="raise", default_value=list, **kwargs):
        if default_value == list:
            default_value = self._default
        super().__init__(default_value=default_value, errors=errors, **kwargs)
        self.func = self._to_array

    def _to_list(self, x):
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

    def _to_array(self, x):
        try:
            return ast.literal_eval(x)
        except SyntaxError:
            return self._to_list(x)

    def _check_isinstance(self, value):
        if isinstance(value, list):
            return True


class Series:
    data_type_transform_methods = {
        "string": String,
        "array": Array
    }

    def __init__(
            self,
            data,
            dtype,
            default="auto",
            errors="default",
            dt_format=None,
            is_array=False,
            depth=1,
            **kwargs
    ):
        """

        :param type:
        :param errors: coerce|raise|ignore|default #TODO: Вызов исключения, если что то другое подано
        :param is_array:
        :param dt_format: timestamp|формат даты
        """
        self.errors = errors
        self.default = default
        self.dt_format = dt_format
        self.dtype = dtype
        self.error_values = []
        self.is_array = is_array
        self.depth = depth
        self._depth = depth + int(is_array)
        if kwargs.get("_data"):
            self._data = kwargs.get("_data")
        else:
            self._data = self._deserialize(data)

    def count_errors(self):
        return len(self.error_values)

    def _get_class(self, data_type, errors, default, **kwargs):
        return self.data_type_transform_methods[data_type](default, errors, **kwargs)

    def _to_array(self, values):
        if not isinstance(values, list):
            array = self._get_class("array",
                                    errors="raise" if self._depth == self.depth else self.errors,
                                    default="auto")
            values = array(values)
            if not isinstance(values, list):
                raise TypeError("Не смог преобразовать в массив значений")
        self._depth -= 1
        return values

    def _deserialize(self, values):
        print("вход", self._depth, values)
        value_list = self._to_array(values)
        print("после входа", self._depth)

        if self._depth == 0:
            print("значение вход", value_list)
            value_type = self._get_class(self.dtype, self.errors, self.default)
            result = list(map(value_type, value_list))
            # TODO: с индексом строки добавлять значения ошибки
            self.error_values += value_type.error_values
            print("значение выход", result)
            return result
        else:
            # Преобразование значения в словарях.
            print("массив вход", value_list)
            result = []
            for _list in value_list:
                series = Series(_list, self.dtype, errors=self.errors, is_array=True, depth=self._depth - 1)
                print("Рекурсия")
                print(self._depth, series._depth, "is_array", self._depth > 1)
                result.append(series())
            print("массив выход", self._depth, result)
            return result

    def to_list(self):
        return self._data

    def __len__(self):
        return len(self._data)

    def __getitem__(self, key):
        data = self._data[key]
        return Series(
            _data=data,
            data=None,
            dtype=self.dtype,
            default=self.default,
            errors=self.errors,
            dt_format=self.dt_format,
            is_array=self.is_array,
            depth=self.depth,
        )

    def __repr__(self):
        return self()

    def __str__(self):
        return str(self())

    def __call__(self, *args, **kwargs):
        return self.to_list()


class Column:
    def __init__(
        self,
        type,
        errors="default",
        custom_default="_",
        is_json=False,
        dt_format=None,
        **kwargs
    ):
        """

        :param type:
        :param errors: coerce|raise|ignore|default #TODO: Вызов исключения, если что то другое подано
        :param is_json:
        :param dt_format: timestamp|формат даты
        """
        self.errors = errors
        self.custom_default = custom_default
        self.dt_format = dt_format
        self.is_json = is_json
        self.name = kwargs.get("name", None)
        self.data_type = type
        self.array_sizes = []
        self.error_values = []
        self.data_type_default_values = {
            "string": "",
            "float": 0.0,
            "int": 0,
            "uint": 0,
            "datetime": 0,
            "date": 0,
            "timestamp": 0,
        }
        self.data_type_transform_methods = {
            "string": self.to_string,
            "float": self.to_float,
            "int": self.to_int,
            "uint": self.to_uint,
            "datetime": self.to_datetime,
            "date": self.to_date,
            "timestamp": self.timestamp,
        }

    @property
    def min_array_size(self):
        if self.is_json:
            return min(self.array_sizes)

    @property
    def count_errors(self):
        return len(self.error_values)

    def _process_error(self, x, except_):
        self.error_values.append(x)
        if self.errors == "default":
            if self.custom_default != "_":
                return self.custom_default
            return self.data_type_default_values[self.data_type]
        elif self.errors == "raise":
            logging.error(
                "Ошибка преобразования значения {} в тип {}".format(x, self.data_type)
            )
            raise except_
        elif self.errors == "ignore":
            return x
        elif self.errors == "coerce":
            return None

    def _to(self, func, x, *args, **kwargs):
        try:
            result = func(x, *args, **kwargs)
        except (ValueError, TypeError) as e:
            return self._process_error(x, e)
        else:
            return result

    def to_string(self, x):
        return self._to(str, x)

    def to_float(self, x):
        return self._to(float, x)

    def to_int(self, x):
        return self._to(int, x)

    def to_uint(self, x):
        n = self.to_int(x)
        if n and n < 0:
            return self._process_error(x, ValueError("less than 0"))
        else:
            return n

    def timestamp(self, x):
        if self.dt_format:
            dt_ = self._to(self.to_datetime, x)
            return dt_.timestamp()
        else:
            return self.to_int(x)

    def to_datetime(self, x):
        if self.dt_format == "timestamp":
            n = self.to_int(x)
            return self._to(dt.datetime.fromtimestamp, n)
        else:
            return self._to(dt.datetime.strptime, x, self.dt_format)

    def to_date(self, x):
        return self.to_datetime(x).date()

    def _to_list(self, x):
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

    def to_json(self, x):
        if x is not None:
            try:
                x = ast.literal_eval(x)
            except SyntaxError:
                return self._to_list(x)
        return x

    def _deserialize(self, data):
        # TODO: не преобразовывать, если тип уже соответствует.
        return self.data_type_transform_methods[self.data_type](data)

    def transform_value(self, data):
        if self.is_json and isinstance(data, str):
            data = self.to_json(data)

        if self.is_json:
            if not isinstance(data, (list, tuple)):
                raise TypeError
            self.array_sizes.append(len(data))
            return list(map(self._deserialize, data))

        return self._deserialize(data)


class NewData:
    def __init__(self, data, schema, **kwargs):
        """

        :param schema: [{"name": "n", "type": "int", "default": "default", "is_array": "False", "dt_format": None}]
        :param data: list, tuple
        """
        self.error_rows = []
        self.columns = [s.get("name", i) for i, s in enumerate(schema)]
        self._col_name_by_index_dict = {col_name: i for i, col_name in enumerate(self.columns)}
        self._schema = schema
        self.dtypes = {sch.get("name", i): sch["type"] for i, sch in enumerate(schema)}
        self._series = {}
        if kwargs.get("series"):
            self._series = kwargs.get("series")
        else:
            self._deserialize(data, schema)

    def _deserialize(self, data, schema):
        col_index = range(len(data))
        for i, values, series_schema in zip(col_index, data, schema):
            series = Series(values, dtype=series_schema["type"])
            self._series[i] = series

    def _get_col_index(self, key):
        if isinstance(key, str):
            try:
                index = self._col_name_by_index_dict[key]
            except KeyError:
                raise KeyError("Такого столбца нет")
        else:
            if key > len(self.columns):
                raise KeyError("Такого столбца нет")
            index = key
        return index

    def to_list(self):
        data = []
        for col_index in range(len(self.columns)):
            value_list = self._series[col_index].to_list()
            data.append(value_list)
        return data

    def to_values(self):
        return list(zip(*self.to_list()))

    def to_dict(self):
        return [dict(zip(self.columns,v)) for v in self.to_values()]

    def __len__(self):
        """Count rows."""
        return len(self._series[0])

    def __getitem__(self, key):
        if isinstance(key, (str, int)):
            col_index = self._get_col_index(key)
            return self._series[col_index]
        elif isinstance(key, list):
            series = {}
            schema = []
            for col in key:
                col_index = self._get_col_index(col)
                schema.append(self._schema[col_index])
                series[col_index] = self._series[col_index]
            return NewData(data=None, schema=schema, series=series)
        elif isinstance(key, slice):
            series = {i: s[key] for i, s in enumerate(self._series.values())}
            return NewData(data=None, schema=self._schema, series=series)
        else:
            raise TypeError

    def __setitem__(self, key, value):
        col_index = self._get_col_index(key)
        self._series[col_index] = value

    def __delitem__(self, key):
        col_index = self._get_col_index(key)
        del self._series[col_index]

    def __repr__(self):
        return self()

    def __str__(self):
        return str(self())

    def __call__(self, *args, **kwargs):
        return self.to_values()


class Data:
    def __init__(self, data, schema):
        """

        :param schema: [{"name": "n", "type": "int", "default": "default", "is_array": "False", "dt_format": None}]
        :param data: list, tuple
        """
        self.schema = schema
        self.data = data
        self.column_names = [i["name"] for i in schema]
        self.columns = {}
        self._Column = Column
        self.error_rows = []

    @property
    def error_values(self):
        d = {}
        for k, v in self.columns.items():
            d[k] = v.error_values
        return d

    @property
    def count_error_values(self):
        d = {}
        for k, v in self.columns.items():
            if len(v.error_values) > 0:
                d[k] = len(v.error_values)
        return d

    def _transform_row(self, row):
        if not self.columns:
            self.columns = {
                params["name"]: self._Column(**params) for params in self.schema
            }

        if isinstance(row, (list, tuple)):
            if len(row) != len(self.columns):
                # TODO: Обозначить появление ошибки в этом месте.
                self.error_rows.append(row)
                return None

            new_row = [
                self.columns[name].transform_value(value)
                for name, value in zip(self.column_names, row)
            ]

        elif isinstance(row, dict):
            new_row = {
                name: column.transform_value(row[name])
                for name, column in self.columns.items()
                if name in row
            }
        else:
            raise TypeError

        return new_row

    def filtered(self):
        count_values_in_row = [len(i) for i in self.data if i]
        min_values_in_row = min(count_values_in_row) if count_values_in_row else None

        def filtering_rows(row):
            if row is None:
                return False
            elif min_values_in_row and len(row) != min_values_in_row:
                self.error_rows.append(row)
                return False
            return True

        self.data = list(filter(filtering_rows, self.data))

    def transform(self):
        # TODO: опция: по первой строке проверить,
        #  если уже тип соответствует заданному, то не проходить по всем стркоам
        self.data = list(map(self._transform_row, self.data))

    def print_stats(self):
        logging.info(
            "Кол-во отфильтрованных строк из-за того, "
            "что в них кол-во значений/столбцов отличается: {}".format(
                len(self.error_rows)
            )
        )
        logging.info(
            "Кол-во преобразованных значений: {}".format(self.count_error_values)
        )

    def to_json(self):
        return self.data

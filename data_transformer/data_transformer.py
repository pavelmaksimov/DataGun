# -*- coding: utf-8 -*-
import ast
import datetime as dt
import re


class Column:
    def __init__(self, type, errors="default", custom_default="_", is_json=False, dt_format=None, **kwargs):
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


class Data:
    def __init__(self, data, schema):
        """

        :param schema: [{"name": "n", "type": "int", "default": "default", "is_json": "False", "dt_format": None}]
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

    def _transform_row(self, row):
        if not self.columns:
            self.columns = {params["name"]: self._Column(**params)
                            for params in self.schema}

        if isinstance(row, (list, tuple)):
            if len(row) != len(self.columns):
                self.error_rows.append(row)
                return None

            new_row = [self.columns[name].transform_value(value)
                       for name, value in zip(self.column_names, row)]
            array_sizes = [len(value) for name, value in zip(self.column_names, row)
                           if self.columns[name].is_json]

        elif isinstance(row, dict):
            new_row = {name: column.transform_value(row[name])
                       for name, column in self.columns.items()
                       if name in row}
            array_sizes = [len(value) for name, value in row.items()
                           if self.columns[name].is_json]
        else:
            raise TypeError

        if len(array_sizes) > 1 and min(array_sizes) != max(array_sizes):
            self.error_rows.append(row)
            return None
        else:
            return new_row

    def _filtered(self, data):
        count_values_in_row = min([len(i) for i in data if i])

        def filtering_rows(row):
            if row is None:
                return False
            elif len(row) != count_values_in_row:
                self.error_rows.append(row)
                return False
            return True

        splitted_text_filtered = list(filter(filtering_rows, data))

        return splitted_text_filtered

    def transform(self):
        self.data = self._filtered(list(map(self._transform_row, self.data)))

    def to_json(self):
        return self.data

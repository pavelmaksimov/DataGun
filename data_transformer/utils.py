# -*- coding: utf-8 -*-
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
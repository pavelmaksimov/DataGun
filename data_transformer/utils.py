# -*- coding: utf-8 -*-
def get_schema_from_clickhouse_describe_table(describe_table, **schema):
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

        dtype = None
        for k, v in dtypes.items():
            if k in col[1]:
                dtype = v
                break
        d = {
            **schema,
            "name": col[0],
            "type": dtype,
        }

        if depth > 0:
            d["depth"] = depth

        schema.append(d)

    return schema
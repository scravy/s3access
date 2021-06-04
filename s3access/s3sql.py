from collections.abc import Collection
from numbers import Number


def quote(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, Collection) and not isinstance(value, str):
        return f"({', '.join(quote(v) for v in value)})"
    if isinstance(value, Number):
        return f"{value}"
    value = str(value).replace("'", "''")
    return f"'{value}'"

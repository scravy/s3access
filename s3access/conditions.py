from abc import abstractmethod, ABC
from typing import Union, Sequence, Dict

from .s3sql import quote


class Condition(ABC):
    def __init__(self, value):
        self.__value = value

    @abstractmethod
    def get_sql_fragment(self, ref) -> str:
        raise NotImplementedError

    @abstractmethod
    def check(self, ref) -> bool:
        raise NotImplementedError

    @property
    def _quoted(self) -> str:
        return quote(self._value)

    @property
    def _value(self):
        return self.__value

    def __str__(self) -> str:
        return f"{type(self).__name__}({self._quoted})"


class EQ(Condition):
    def check(self, ref) -> bool:
        if self._value is None:
            return ref is None
        return ref == self._value

    def get_sql_fragment(self, ref):
        if self._value is None:
            return f"{ref} IS NULL"
        return f"{ref} = {self._quoted}"


class LT(Condition):
    def check(self, ref) -> bool:
        return ref < self._value

    def get_sql_fragment(self, ref):
        return f"{ref} < {self._quoted}"


class GT(Condition):
    def check(self, ref) -> bool:
        return ref > self._value

    def get_sql_fragment(self, ref):
        return f"{ref} > {self._quoted}"


class NEQ(Condition):
    def check(self, ref) -> bool:
        if self._value is None:
            return ref is not None
        return ref != self._value

    def get_sql_fragment(self, ref):
        if self._value is None:
            return f"{ref} IS NOT NULL"
        return f"{ref} <> {self._quoted}"


class LTE(Condition):
    def check(self, ref) -> bool:
        return ref <= self._value

    def get_sql_fragment(self, ref):
        return f"{ref} <= {self._quoted}"


class GTE(Condition):
    def check(self, ref) -> bool:
        return ref >= self._value

    def get_sql_fragment(self, ref):
        return f"{ref} >= {self._quoted}"


class IN(Condition):
    def __init__(self, *values):
        super().__init__(values)

    def check(self, ref) -> bool:
        return ref in self._value

    def get_sql_fragment(self, ref):
        return f"{ref} IN {self._quoted}"


class AND(Condition):
    def __init__(self, *conditions: Condition):
        super().__init__(None)
        self._conditions = conditions

    def check(self, ref) -> bool:
        for condition in self._conditions:
            if not condition.check(ref):
                return False
        return True

    def get_sql_fragment(self, ref) -> str:
        return ' AND '.join(f"({c.get_sql_fragment(ref)})" for c in self._conditions)


class OR(Condition):
    def __init__(self, *conditions: Condition):
        super().__init__(None)
        self._conditions = conditions

    def check(self, ref) -> bool:
        for condition in self._conditions:
            if condition.check(ref):
                return True
        return False

    def get_sql_fragment(self, ref) -> str:
        return ' OR '.join(f"({c.get_sql_fragment(ref)})" for c in self._conditions)


Conditionable = Union[Condition, str, int, float, Sequence[str]]


def make_condition(v):
    if isinstance(v, (str, int, float)):
        return EQ(v)
    if isinstance(v, Sequence):
        return IN(*v)
    assert isinstance(v, Condition), "must be a condition"
    return v


def make_conditions(conditions: Dict[str, Conditionable]) -> Dict[str, Condition]:
    return {k: make_condition(v) for k, v in conditions.items()}

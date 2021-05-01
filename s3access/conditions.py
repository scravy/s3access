from abc import abstractmethod, ABC
from collections.abc import Collection
from numbers import Number


class Condition(ABC):
    def __init__(self, value):
        self._value = value

    @abstractmethod
    def get_sql_fragment(self, ref) -> str:
        raise NotImplementedError

    @abstractmethod
    def check(self, ref) -> bool:
        raise NotImplementedError

    def quote(self, value) -> str:
        if isinstance(value, Collection) and not isinstance(value, str):
            return f"({', '.join(self.quote(v) for v in value)})"
        if isinstance(value, Number):
            return f"{value}"
        return f"'{value}'"

    @property
    def quoted(self) -> str:
        return self.quote(self._value)

    @property
    def value(self):
        return self._value

    def __str__(self) -> str:
        return f"{type(self).__name__}({self.quoted})"


class EQ(Condition):
    def check(self, ref) -> bool:
        return ref == self.value

    def get_sql_fragment(self, ref):
        return f"{ref} = {self.quoted}"


class LT(Condition):
    def check(self, ref) -> bool:
        return ref < self.value

    def get_sql_fragment(self, ref):
        return f"{ref} < {self.quoted}"


class GT(Condition):
    def check(self, ref) -> bool:
        return ref > self.value

    def get_sql_fragment(self, ref):
        return f"{ref} > {self.quoted}"


class NEQ(Condition):
    def check(self, ref) -> bool:
        return ref != self.value

    def get_sql_fragment(self, ref):
        return f"{ref} <> {self.quoted}"


class LTE(Condition):
    def check(self, ref) -> bool:
        return ref <= self.value

    def get_sql_fragment(self, ref):
        return f"{ref} <= {self.quoted}"


class GTE(Condition):
    def check(self, ref) -> bool:
        return ref >= self.value

    def get_sql_fragment(self, ref):
        return f"{ref} >= {self.quoted}"


class IN(Condition):
    def __init__(self, *values):
        super().__init__(values)

    def check(self, ref) -> bool:
        return ref in self.value

    def get_sql_fragment(self, ref):
        return f"{ref} IN {self.quoted}"

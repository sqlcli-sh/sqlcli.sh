from collections import OrderedDict
from inspect import isclass
from typing import Any, List, Union


def isexception(obj: Any) -> bool:
    """Given an object, return a boolean indicating whether it is an instance or subclass of :py:class:`Exception`."""
    if isinstance(obj, Exception):
        return True
    if isclass(obj) and issubclass(obj, Exception):
        return True
    return False


class Record:
    """A row, from a query, from a database."""

    __slots__ = ("_keys", "_values")

    def __init__(self, keys: List[str], values: List[Any]):
        """Initialize a Record with keys and values."""
        self._keys = keys
        self._values = values

        # Ensure that lengths match properly.
        assert len(self._keys) == len(self._values)

    def keys(self) -> List[str]:
        """Returns the list of column names from the query."""
        return self._keys

    def values(self) -> List[Any]:
        """Returns the list of values from the query."""
        return self._values

    def values_str(self) -> List[str]:
        """Returns the list of values from the query."""
        return [str(x) for x in self._values]

    def __repr__(self):
        """Returns a string representation of the Record."""
        return "<Record {}>".format(self.export("json")[1:-1])

    def __getitem__(self, key: Union[int, str]) -> Any:
        """Support for index-based and string-based lookup."""
        # Support for index-based lookup.
        if isinstance(key, int):
            return self.values()[key]

        # Support for string-based lookup.
        if key in self.keys():
            i = self.keys().index(key)
            if self.keys().count(key) > 1:
                raise KeyError(f"Record contains multiple '{key}' fields.")
            return self.values()[i]

        raise KeyError(f"Record contains no '{key}' field.")

    def __getattr__(self, key: Union[int, str]) -> Any:
        """Support for attribute-based lookup."""
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e) from e

    def __dir__(self):
        """Merge standard attrs with generated ones (from column names)."""
        standard = dir(super())
        return sorted(standard + [str(k) for k in self.keys()])

    def get(self, key: Union[int, str], default=None) -> Any:
        """Returns the value for a given key, or default."""
        try:
            return self[key]
        except KeyError:
            return default

    def as_dict(self, ordered: bool = False):
        """Returns the row as a dictionary, as ordered."""
        items = zip(self.keys(), self.values())

        return OrderedDict(items) if ordered else dict(items)


class RecordCollection:
    """A set of excellent Records from a query."""

    def __init__(self, rows):
        """Initialize a RecordCollection with rows.

        Args:
            rows: The rows to be stored in the RecordCollection.
        """
        self._rows = rows
        self._all_rows = []
        self.pending = True

    def __repr__(self):
        """Returns a string representation of the RecordCollection."""
        return f"<RecordCollection size={len(self)} pending={self.pending}>"

    def __iter__(self):
        """Iterate over all rows, consuming the underlying generator only when necessary."""
        i = 0
        while True:
            # Other code may have iterated between yields,
            # so always check the cache.
            if i < len(self):
                yield self[i]
            else:
                # Throws StopIteration when done.
                # Prevent StopIteration bubbling from generator, following https://www.python.org/dev/peps/pep-0479/
                try:
                    yield next(self)
                except StopIteration:
                    return
            i += 1

    def __next__(self):
        """Returns the next row in the RecordCollection."""
        try:
            nextrow = next(self._rows)
            self._all_rows.append(nextrow)
            return nextrow
        except StopIteration as stop:
            self.pending = False
            raise StopIteration("RecordCollection contains no more rows.") from stop

    def __len__(self):
        """Returns the number of rows in the RecordCollection."""
        return len(self._all_rows)

    def all(self, as_dict=False, as_ordereddict=False):  # noqa: A003
        """Returns a list of all rows for the RecordCollection.

        If they haven't been fetched yet, consume the iterator and cache the results.

        Args:
            as_dict: A boolean indicating whether to return the rows as dictionaries.
            as_ordereddict: A boolean indicating whether to return the rows as ordered dictionaries.
        """
        # By calling list it calls the __iter__ method
        rows = list(self)

        if as_dict:
            return [r.as_dict() for r in rows]
        if as_ordereddict:
            return [r.as_dict(ordered=True) for r in rows]

        return rows

    def as_dict(self, ordered=False):
        """Returns all rows as dictionaries or ordered dictionaries.

        Args:
            ordered: A boolean indicating whether to return the rows as ordered dictionaries.
        """
        return self.all(as_dict=not (ordered), as_ordereddict=ordered)

    def first(self, default=None, as_dict=False, as_ordereddict=False):
        """Returns a single record for the RecordCollection, or `default`.

        If `default` is an instance or subclass of Exception, then raise it
        instead of returning it.

        Args:
            default: The default value to return if the RecordCollection is empty.
            as_dict: A boolean indicating whether to return the record as a dictionary.
            as_ordereddict: A boolean indicating whether to return the record as an ordered dictionary.
        """
        # Try to get a record, or return/raise default.
        try:
            record = self[0]
        except IndexError as e:
            if isexception(default):
                raise default from e
            return default

        # Cast and return.
        if as_dict:
            return record.as_dict()
        if as_ordereddict:
            return record.as_dict(ordered=True)

        return record

    def one(self, default=None, as_dict=False, as_ordereddict=False):
        """Returns a single record for the RecordCollection.

        Ensures that it is the only record, or returns `default`. If `default` is an instance
        or subclass of Exception, then raise it instead of returning it.

        Args:
            default: The default value to return if the RecordCollection is empty or contains more than one row.
            as_dict: A boolean indicating whether to return the record as a dictionary.
            as_ordereddict: A boolean indicating whether to return the record as an ordered dictionary.
        """
        # Ensure that we don't have more than one row.
        try:
            self[1]
        except IndexError:
            return self.first(default=default, as_dict=as_dict, as_ordereddict=as_ordereddict)
        else:
            raise ValueError(
                "RecordCollection contained more than one row. "
                "Expects only one row when using "
                "RecordCollection.one"
            )

    def scalar(self, default=None):
        """Returns the first column of the first row, or `default`.

        Args:
            default: The default value to return if the RecordCollection is empty.
        """
        row = self.one()
        return row[0] if row else default

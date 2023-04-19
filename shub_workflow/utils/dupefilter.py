#!/usr/bin/env python
import os
import abc
import tempfile
from typing import Union
from typing_extensions import Protocol
from typing import Container

from sqlitedict import SqliteDict


class DupesFilterProtocol(Protocol, Container[str]):
    @abc.abstractmethod
    def add(self, elem: str):
        ...


class SqliteDictDupesFilter:
    def __init__(self):
        """
        SqlteDict based dupes filter
        """
        self.dupes_db_file = tempfile.mktemp()
        self.__filter: Union[SqliteDict, None] = None

    def __create_db(self):
        self.__filter = SqliteDict(self.dupes_db_file, flag="n", autocommit=True)

    def __contains__(self, element: object) -> bool:
        if self.__filter is None:
            self.__create_db()
        assert self.__filter is not None
        return element in self.__filter

    def add(self, element: str):
        if self.__filter is None:
            self.__create_db()
        assert self.__filter is not None
        self.__filter[element] = "-"

    def close(self):
        if self.__filter is not None:
            try:
                self.__filter.close()
                os.remove(self.dupes_db_file)
            except Exception:
                pass

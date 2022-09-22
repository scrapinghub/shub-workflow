#!/usr/bin/env python
import os
import tempfile

from sqlitedict import SqliteDict


class SqliteDictDupesFilter:
    def __init__(self):
        """
        SqlteDict based dupes filter
        """
        self.dupes_db_file = tempfile.mktemp()
        self.__filter = None

    def __create_db(self):
        self.__filter = SqliteDict(self.dupes_db_file, flag="n", autocommit=True)

    def __contains__(self, element):
        if self.__filter is None:
            self.__create_db()
        return element in self.__filter

    def add(self, element):
        if self.__filter is None:
            self.__create_db()
        self.__filter[element] = "-"

    def close(self):
        if self.__filter is not None:
            try:
                self.__filter.close()
                os.remove(self.dupes_db_file)
            except Exception:
                pass

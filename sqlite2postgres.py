import sqlite3
import csv
import psycopg2
import tempfile


class Converter:
    def __init__(self, sqliteCursor, postgresCursor):
        if not (isinstance(sqliteCursor, sqlite3.Cursor) and isinstance(postgresCursor, psycopg2._psycopg.cursor)):
            raise TypeError("input must be valid cursors")
        self._sqCursor = sqliteCursor
        self._pgCursor = postgresCursor

    def _createTempCSV(self, table):
        table = table.replace("\"", "\"\"")
        self._sqCursor.execute('''SELECT * FROM "{0}"'''.format(table))
        tmp = tempfile.TemporaryFile(mode="w+", newline='')
        writer = csv.writer(tmp, delimiter=';',
                            quoting=csv.QUOTE_NONNUMERIC)
        writer.writerows(self._sqCursor)
        tmp.seek(0)
        return tmp

    def toPostgres(self, srcTable, targetTable):
        tempFile = self._createTempCSV(srcTable)
        self._pgCursor.copy_from(tempFile, targetTable, sep=';')

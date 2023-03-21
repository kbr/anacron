"""
sql_interface.py

SQLite interface for storing tasks
"""

import datetime
import pickle
import sqlite3

from .configuration import configuration


DB_TABLE_NAME_TASK = "task"
CMD_CREATE_TASK_TABLE = f"""
CREATE TABLE IF NOT EXISTS {DB_TABLE_NAME_TASK}
(
    schedule datetime PRIMARY KEY,
    crontab TEXT,
    function_module TEXT,
    function_name TEXT,
    function_arguments BLOB
)
"""
CMD_STORE_CALLABLE = f"""
INSERT INTO {DB_TABLE_NAME_TASK} VALUES
(
    :schedule,
    :crontab,
    :function_module,
    :function_name,
    :function_arguments
)
"""
COLUMN_SEQUENCE = "\
    rowid,schedule,crontab,function_module,function_name,function_arguments"
CMD_GET_CALLABLES_BY_NAME = f"""\
    SELECT {COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_TASK}
    WHERE function_module == ? AND function_name == ?"""
CMD_GET_CALLABLES_ON_DUE = f"""\
    SELECT {COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_TASK} WHERE schedule <= ?"""
CMD_UPDATE_SCHEDULE = f"\
    UPDATE {DB_TABLE_NAME_TASK} SET schedule = ? WHERE rowid == ?"
CMD_DELETE_CALLABLE = f"DELETE FROM {DB_TABLE_NAME_TASK} WHERE rowid == ?"
SQLITE_STRFTIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


class SQLiteInterface:
    """
    SQLite interface for application specific operations.
    """

    def __init__(self, db_name=":memory:"):
        self.db_name = db_name
        self._execute(CMD_CREATE_TASK_TABLE)

    def _execute(self, cmd, parameters=()):
        """run a command with parameters."""
        con = sqlite3.connect(self.db_name)
        with con:
            return con.execute(cmd, parameters)

    @staticmethod
    def _fetch_all_callable_entries(cursor):
        """
        Internal generator to iterate over a selection of entries and unpack
        the columns to a dictionary with the following key-value pairs:

            {
                "rowid": integer,
                "schedule": datetime,
                "crontab": string,
                "function_module": string,
                "function_name": string,
                "args": tuple(of original datatypes),
                "kwargs": dict(of original datatypes),
            }

        """
        for entry in cursor.fetchall():
            # entry is an ordered list of columns as defined in `CREATE TABLE`
            # the blob column with the pickled arguments is the last column
            args, kwargs = pickle.loads(entry[-1])
            data = {
                key: entry[i] for i, key in enumerate(
                    COLUMN_SEQUENCE.strip().split(",")[:-1]
                )
            }
            # convert sqlite3 datetime to python datetime datatype:
            # (for testing: this needs more than 1 millisecond)
            data["schedule"] = datetime.datetime.strptime(
                data["schedule"], SQLITE_STRFTIME_FORMAT
            )
            data["args"] = args
            data["kwargs"] = kwargs
            yield data

    def register_callable(self, func, schedule=None, crontab="", args=(), kwargs=None):
        """
        Store a callable in the database.
        """
        if not schedule:
            schedule = datetime.datetime.now()
        if not kwargs:
            kwargs = {}
        arguments = pickle.dumps((args, kwargs))
        data = {
            "schedule": schedule,
            "crontab": crontab,
            "function_module": func.__module__,
            "function_name": func.__name__,
            "function_arguments": arguments,
        }
        self._execute(CMD_STORE_CALLABLE, data)

    def get_callables(self, schedule=None):
        """
        Generator function to return all callables that according to
        their schedules are on due. Callables are represented by a
        dictionary as returned from `_fetch_all_callable_entries()`
        """
        if not schedule:
            schedule = datetime.datetime.now()
        cursor = self._execute(CMD_GET_CALLABLES_ON_DUE, [schedule])
        yield from self._fetch_all_callable_entries(cursor)

    def find_callables(self, func):
        """
        Generator function to return all callables matching the
        function-signature. Callables are represented by a dictionary as
        returned from `_fetch_all_callable_entries()`
        """
        parameters = func.__module__, func.__name__
        cursor = self._execute(CMD_GET_CALLABLES_BY_NAME, parameters)
        yield from self._fetch_all_callable_entries(cursor)

    def delete_callable(self, entry):
        """
        Delete the entry in the callable-table. Entry should be a
        dictionary as returned from `get_callables()`. The row to delete
        gets identified by the `rowid`.
        """
        self._execute(CMD_DELETE_CALLABLE, [entry["rowid"]])

    def update_schedule(self, rowid, schedule):
        """
        Update the `schedule` of the table entry with the given `rowid`.
        """
        parameters = schedule, rowid
        self._execute(CMD_UPDATE_SCHEDULE, parameters)


interface = SQLiteInterface(db_name=configuration.db_file)

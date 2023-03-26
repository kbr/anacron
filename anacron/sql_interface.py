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
    uuid TEXT,
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
    :uuid,
    :schedule,
    :crontab,
    :function_module,
    :function_name,
    :function_arguments
)
"""
COLUMN_SEQUENCE = "\
    rowid,uuid,schedule,crontab,function_module,function_name,function_arguments"
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
        Internal function to iterate over a selection of entries and unpack
        the columns to a dictionary with the following key-value pairs:

            {
                "rowid": integer,
                "uuid": string,
                "schedule": datetime,
                "crontab": string,
                "function_module": string,
                "function_name": string,
                "args": tuple(of original datatypes),
                "kwargs": dict(of original datatypes),
            }

        Returns a list of dictionaries or an empty list if a selection
        does not match any row.
        """
        def process(row):
            """
            Gets a `row` and returns a dictionary with Python datatypes.
            `row` is an ordered tuple of columns as defined in `CREATE
            TABLE`. The blob column with the pickled arguments is the
            last column.
            """
            args, kwargs = pickle.loads(row[-1])
            data = {
                key: row[i] for i, key in enumerate(
                    COLUMN_SEQUENCE.strip().split(",")[:-1]
                )
            }
            # convert sqlite3 stores datetime as string
            data["schedule"] = datetime.datetime.strptime(
                data["schedule"], SQLITE_STRFTIME_FORMAT
            )
            data["args"] = args
            data["kwargs"] = kwargs
            return data
        return [process(row) for row in cursor.fetchall()]

    # pylint: disable=too-many-arguments
    def register_callable(
        self,
        func,
        uuid=None,
        schedule=None,
        crontab="",
        args=(),
        kwargs=None,
    ):
        """
        Store a callable in the database.
        """
        if not schedule:
            schedule = datetime.datetime.now()
        if not kwargs:
            kwargs = {}
        arguments = pickle.dumps((args, kwargs))
        data = {
            "uuid": uuid,
            "schedule": schedule,
            "crontab": crontab,
            "function_module": func.__module__,
            "function_name": func.__name__,
            "function_arguments": arguments,
        }
        self._execute(CMD_STORE_CALLABLE, data)

    def get_tasks_on_due(self, schedule=None):
        """
        Returns a list of all callables (tasks) that according to their
        schedules are on due. Callables are represented by a dictionary
        as returned from `_fetch_all_callable_entries()`
        """
        if not schedule:
            schedule = datetime.datetime.now()
        cursor = self._execute(CMD_GET_CALLABLES_ON_DUE, [schedule])
        return self._fetch_all_callable_entries(cursor)

    def find_callables(self, func):
        """
        Return a list of all callables matching the function-signature.
        Callables are represented by a dictionary as returned from
        `_fetch_all_callable_entries()`
        """
        parameters = func.__module__, func.__name__
        cursor = self._execute(CMD_GET_CALLABLES_BY_NAME, parameters)
        return self._fetch_all_callable_entries(cursor)

    def delete_callable(self, entry):
        """
        Delete the entry in the callable-table. Entry should be a
        dictionary as returned from `get_tasks_on_due()`. The row to delete
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

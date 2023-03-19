"""
sql_interface.py

SQLite interface for storing tasks
"""

import datetime
import pickle
import sqlite3

from .configuration import configuration


DB_TABLE_NAME_TASK = "task"
CMD_CREATE_TABLE = f"""
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
COLUMN_SEQUENCE = "rowid,schedule,crontab,function_module,function_name,function_arguments"
CMD_GET_CALLABLES = f"""
SELECT {COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_TASK} WHERE schedule <= ?
"""
CMD_DELETE_CALLABLE = f"DELETE FROM {DB_TABLE_NAME_TASK} WHERE rowid == ?"
SQLITE_STRFTIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


class SQLiteInterface:

    def __init__(self, db_name=":memory:"):
        self.db_name = db_name
        self._execute(CMD_CREATE_TABLE)

    def _execute(self, cmd, parameters=()):
        con = sqlite3.connect(self.db_name)
        with con:
            return con.execute(cmd, parameters)

    def register_callable(self, func, schedule=None, crontab="", args=(), kwargs=None):
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
        if not schedule:
            schedule = datetime.datetime.now()
        cursor = self._execute(CMD_GET_CALLABLES, [schedule])
        for entry in cursor.fetchall():
            args, kwargs = pickle.loads(entry[-1])
            data = {key: entry[i] for i, key in enumerate(COLUMN_SEQUENCE.split(",")[:-1])}
            data["args"] = args
            data["kwargs"] = kwargs
            yield data

    def delete_callable(self, entry):
        self._execute(CMD_DELETE_CALLABLE, [entry["rowid"]])


interface = SQLiteInterface(db_name=configuration.db_file)

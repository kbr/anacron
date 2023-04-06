"""
sql_interface.py

SQLite interface for storing tasks
"""

import datetime
import pickle
import sqlite3
import types

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
TASK_COLUMN_SEQUENCE =\
    "rowid,uuid,schedule,crontab,"\
    "function_module,function_name,function_arguments"
CMD_GET_CALLABLES_BY_NAME = f"""\
    SELECT {TASK_COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_TASK}
    WHERE function_module == ? AND function_name == ?"""
CMD_GET_CALLABLES_ON_DUE = f"""\
    SELECT {TASK_COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_TASK} WHERE schedule <= ?"""
CMD_UPDATE_SCHEDULE = f"\
    UPDATE {DB_TABLE_NAME_TASK} SET schedule = ? WHERE rowid == ?"
CMD_DELETE_CALLABLE = f"DELETE FROM {DB_TABLE_NAME_TASK} WHERE rowid == ?"

DB_TABLE_NAME_RESULT = "result"
CMD_CREATE_RESULT_TABLE = f"""
CREATE TABLE IF NOT EXISTS {DB_TABLE_NAME_RESULT}
(
    uuid TEXT PRIMARY KEY,
    status INTEGER,
    function_module TEXT,
    function_name TEXT,
    function_arguments BLOB,
    function_result BLOB,
    error_message TEXT,
    ttl datetime
)
"""
CMD_STORE_RESULT = f"""
INSERT INTO {DB_TABLE_NAME_RESULT} VALUES
(
    :uuid,
    :status,
    :function_module,
    :function_name,
    :function_arguments,
    :function_result,
    :error_message,
    :ttl
)
"""
RESULT_COLUMN_SEQUENCE =\
    "rowid,uuid,status,function_module,function_name,"\
    "function_arguments,function_result,error_message, ttl"
CMD_GET_RESULT_BY_UUID = f"""\
    SELECT {RESULT_COLUMN_SEQUENCE} FROM {DB_TABLE_NAME_RESULT}
    WHERE uuid == ?"""
CMD_UPDATE_RESULT = f"""
    UPDATE {DB_TABLE_NAME_RESULT} SET
        status = ?,
        function_result = ?,
        error_message = ?,
        ttl = ?
    WHERE uuid == ?"""
# CMD_DELETE_RESULT = f"""\
#     DELETE FROM {DB_TABLE_NAME_RESULT} WHERE uuid == ?"""
# CMD_DELETE_RESULTS = f"""\
#     DELETE FROM {DB_TABLE_NAME_RESULT} WHERE status == 1 AND ttl <= ?"""

SQLITE_STRFTIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

TASK_STATUS_WAITING = 0
TASK_STATUS_READY = 1
TASK_STATUS_ERROR = 3


class HybridNamespace(types.SimpleNamespace):
    """
    A namespace-object with dictionary-like attribute access.
    """
    def __init__(self, data=None):
        """
        Set initial values.
        If data is given, it must be a dictionary.
        """
        if data is not None:
            self.__dict__.update(data)

    def __getitem__(self, name):
        return self.__dict__[name]

    def __setitem__(self, name, value):
        self.__dict__[name] = value


class TaskResult(HybridNamespace):
    """
    Helper class to make task-results more handy.
    """

    @property
    def result(self):
        return self.function_result

    @property
    def is_waiting(self):
        return self.status == TASK_STATUS_WAITING

    @property
    def is_ready(self):
        return self.status == TASK_STATUS_READY



class SQLiteInterface:
    """
    SQLite interface for application specific operations.
    """

    def __init__(self, db_name=":memory:"):
        self.db_name = db_name
        self._execute(CMD_CREATE_TASK_TABLE)
        self._execute(CMD_CREATE_RESULT_TABLE)

    def _execute(self, cmd, parameters=()):
        """run a command with parameters."""
        con = sqlite3.connect(self.db_name)
        with con:
            return con.execute(cmd, parameters)

    # -- task-methods ---
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
                    TASK_COLUMN_SEQUENCE.strip().split(",")[:-1]
                )
            }
            # convert sqlite3 stores datetime as string
            data["schedule"] = datetime.datetime.strptime(
                data["schedule"], SQLITE_STRFTIME_FORMAT
            )
            data["args"] = args
            data["kwargs"] = kwargs
            return HybridNamespace(data)
        return [process(row) for row in cursor.fetchall()]

    # pylint: disable=too-many-arguments
    def register_callable(
        self,
        func,
        uuid="",
        schedule=None,
        crontab="",
        args=(),
        kwargs=None,
    ):
        """
        Store a callable in the task-table of the database.
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

    def get_tasks_by_signature(self, func):
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

    # -- result-methods ---

    @staticmethod
    def _get_result_ttl():
        return datetime.datetime.now() + configuration.result_ttl

    def register_result(
            self,
            func,
            uuid,
            args=(),
            kwargs=None,
        ):
        """
        Register an entry in the result table of the database. The entry
        stores the uuid and the status `False` as zero `0` because the
        task is pending and no result available jet.
        """
        if not kwargs:
            kwargs = {}
        arguments = pickle.dumps((args, kwargs))
        data = {
            "uuid": uuid,
            "status": TASK_STATUS_WAITING,
            "function_module": func.__module__,
            "function_name": func.__name__,
            "function_arguments": arguments,
            "function_result": pickle.dumps(None),
            "error_message": "",
            "ttl": self._get_result_ttl(),
        }
        self._execute(CMD_STORE_RESULT, data)

    def get_result_by_uuid(self, uuid):
        """
        Return a dataset (as TaskResult) or None.
        """
        cursor = self._execute(CMD_GET_RESULT_BY_UUID, (uuid,))
        row = cursor.fetchone()  # tuple of data or None
        if row:
            result = TaskResult(
                dict(zip(RESULT_COLUMN_SEQUENCE.split(","), row)))
            result.function_result = pickle.loads(result.function_result)
            result.function_arguments = pickle.loads(result.function_arguments)
        else:
            result = None
        return result

    def update_result(self, uuid, result=None, error_message=""):
        """
        Updates the result-entry with the given `uuid` to status 1|2 and
        stores the `result` or `error_message`.
        """
        status = TASK_STATUS_ERROR if error_message else TASK_STATUS_READY
        function_result = pickle.dumps(result)
        ttl = self._get_result_ttl()
        parameters = status, function_result, error_message, ttl, uuid
        self._execute(CMD_UPDATE_RESULT, parameters)



interface = SQLiteInterface(db_name=configuration.db_file)

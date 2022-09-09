# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
A Flight SQL client with a DBAPI 2.0/PEP 249-compatible API.

Depends on the ADBC driver manager.
"""

from typing import Dict, Optional

_ADBC_FOUND = False

try:
    import adbc_driver_manager
    import adbc_driver_manager.dbapi
except ImportError:
    pass
else:
    _ADBC_FOUND = True


_dbapi_names = [
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
    "Connection",
    "Cursor",
    "DataError",
    "DatabaseError",
    "Date",
    "DateFromTicks",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]


if _ADBC_FOUND:
    import pyarrow._flight_sql

    __all__ = _dbapi_names

    # ----------------------------------------------------------
    # Globals

    apilevel = adbc_driver_manager.dbapi.apilevel
    threadsafety = adbc_driver_manager.dbapi.threadsafety
    # XXX: the param style can't be determined up front
    paramstyle = "qmark"

    Warning = adbc_driver_manager.dbapi.Warning
    Error = adbc_driver_manager.dbapi.Error
    InterfaceError = adbc_driver_manager.dbapi.InterfaceError
    DatabaseError = adbc_driver_manager.dbapi.DatabaseError
    DataError = adbc_driver_manager.dbapi.DataError
    OperationalError = adbc_driver_manager.dbapi.OperationalError
    IntegrityError = adbc_driver_manager.dbapi.IntegrityError
    InternalError = adbc_driver_manager.dbapi.InternalError
    ProgrammingError = adbc_driver_manager.dbapi.ProgrammingError
    NotSupportedError = adbc_driver_manager.dbapi.NotSupportedError

    # ----------------------------------------------------------
    # Types

    Date = adbc_driver_manager.dbapi.Date
    Time = adbc_driver_manager.dbapi.Time
    Timestamp = adbc_driver_manager.dbapi.Timestamp
    DateFromTicks = adbc_driver_manager.dbapi.DateFromTicks
    TimeFromTicks = adbc_driver_manager.dbapi.TimeFromTicks
    TimestampFromTicks = adbc_driver_manager.dbapi.TimestampFromTicks
    STRING = adbc_driver_manager.dbapi.STRING
    BINARY = adbc_driver_manager.dbapi.BINARY
    NUMBER = adbc_driver_manager.dbapi.NUMBER
    DATETIME = adbc_driver_manager.dbapi.DATETIME
    ROWID = adbc_driver_manager.dbapi.ROWID

    # ----------------------------------------------------------
    # Functions

    def connect(uri: str, *, db_kwargs: Optional[Dict[str, str]] = None,
                conn_kwargs: Optional[Dict[str, str]] = None) -> "Connection":
        """
        Connect to a Flight SQL server via ADBC.

        Parameters
        ----------
        uri : str
            The Flight URI to connect to.
        db_kwargs : dict, optional
            Additional arguments to pass when creating the AdbcDatabase.
        conn_kwargs : dict, optional
            Additional arguments to pass when creating the AdbcConnection.
        """
        db = None
        conn = None
        db_kwargs = db_kwargs or {}
        conn_kwargs = conn_kwargs or {}

        try:
            db = pyarrow._flight_sql.connect_raw(uri, **db_kwargs)
            conn = adbc_driver_manager.AdbcConnection(db, **conn_kwargs)
            return adbc_driver_manager.dbapi.Connection(db, conn)
        except Exception:
            if conn:
                conn.close()
            if db:
                db.close()
            raise

    # ----------------------------------------------------------
    # Classes

    Connection = adbc_driver_manager.dbapi.Connection
    Cursor = adbc_driver_manager.dbapi.Cursor
else:

    def __getattr__(name):
        if name in _dbapi_names:
            raise RuntimeError(
                f"{__name__}.{name} requires adbc_driver_manager")
        else:
            raise AttributeError(
                f"module '{__name__}' has no attribute '{name}'")

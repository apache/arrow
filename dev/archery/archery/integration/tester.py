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

# Base class for language-specific integration test harnesses

from abc import ABC, abstractmethod
import os
import subprocess
import typing

from .util import log


_Predicate = typing.Callable[[], bool]


class CDataExporter(ABC):

    @abstractmethod
    def export_schema_from_json(self, json_path: os.PathLike,
                                c_schema_ptr: object):
        """
        Read a JSON integration file and export its schema.

        Parameters
        ----------
        json_path : Path
            Path to the JSON file
        c_schema_ptr : cffi pointer value
            Pointer to the ``ArrowSchema`` struct to export to.
        """

    @abstractmethod
    def export_batch_from_json(self, json_path: os.PathLike,
                               num_batch: int,
                               c_array_ptr: object):
        """
        Read a JSON integration file and export one of its batches.

        Parameters
        ----------
        json_path : Path
            Path to the JSON file
        num_batch : int
            Number of the record batch in the JSON file
        c_schema_ptr : cffi pointer value
            Pointer to the ``ArrowArray`` struct to export to.
        """

    @property
    @abstractmethod
    def supports_releasing_memory(self) -> bool:
        """
        Whether the implementation is able to release memory deterministically.

        Here, "release memory" means that, after the `release` callback of
        a C Data Interface export is called, `run_gc` is able to trigger
        the deallocation of the memory underlying the export (such as buffer data).

        If false, then `record_allocation_state` is allowed to raise
        NotImplementedError.
        """

    def record_allocation_state(self) -> object:
        """
        Return the current memory allocation state.

        Returns
        -------
        state : object
            Equality-comparable object representing the allocation state,
            for example the number of allocated or exported bytes.
        """
        raise NotImplementedError

    def run_gc(self):
        """
        Run the GC if necessary.

        This should ensure that any temporary objects and data created by
        previous exporter calls are collected.
        """

    @property
    def required_gc_runs(self):
        """
        The maximum number of calls to `run_gc` that need to be issued to
        ensure proper deallocation. Some implementations may require this
        to be greater than one.
        """
        return 1

    def close(self):
        """
        Final cleanup after usage.
        """

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class CDataImporter(ABC):

    @abstractmethod
    def import_schema_and_compare_to_json(self, json_path: os.PathLike,
                                          c_schema_ptr: object):
        """
        Import schema and compare it to the schema of a JSON integration file.

        An error is raised if importing fails or the schemas differ.

        Parameters
        ----------
        json_path : Path
            The path to the JSON file
        c_schema_ptr : cffi pointer value
            Pointer to the ``ArrowSchema`` struct to import from.
        """

    @abstractmethod
    def import_batch_and_compare_to_json(self, json_path: os.PathLike,
                                         num_batch: int,
                                         c_array_ptr: object):
        """
        Import record batch and compare it to one of the batches
        from a JSON integration file.

        The schema used for importing the record batch is the one from
        the JSON file.

        An error is raised if importing fails or the batches differ.

        Parameters
        ----------
        json_path : Path
            The path to the JSON file
        num_batch : int
            Number of the record batch in the JSON file
        c_array_ptr : cffi pointer value
            Pointer to the ``ArrowArray`` struct to import from.
        """

    @property
    @abstractmethod
    def supports_releasing_memory(self) -> bool:
        """
        Whether the implementation is able to release memory deterministically.

        Here, "release memory" means `run_gc()` is able to trigger the
        `release` callback of a C Data Interface export (which would then
        induce a deallocation mechanism on the exporter).
        """

    def run_gc(self):
        """
        Run the GC if necessary.

        This should ensure that any imported data has its release callback called.
        """

    @property
    def required_gc_runs(self):
        """
        The maximum number of calls to `run_gc` that need to be issued to
        ensure release callbacks are triggered. Some implementations may
        require this to be greater than one.
        """
        return 1

    def close(self):
        """
        Final cleanup after usage.
        """

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        # Make sure any exported data is released.
        for i in range(self.required_gc_runs):
            self.run_gc()
        self.close()


class Tester:
    """
    The interface to declare a tester to run integration tests against.
    """
    # whether the language supports producing / writing IPC
    PRODUCER = False
    # whether the language supports consuming / reading IPC
    CONSUMER = False
    # whether the language supports serving Flight
    FLIGHT_SERVER = False
    # whether the language supports receiving Flight
    FLIGHT_CLIENT = False
    # whether the language supports the C Data Interface as an exporter
    C_DATA_SCHEMA_EXPORTER = False
    C_DATA_ARRAY_EXPORTER = False
    # whether the language supports the C Data Interface as an importer
    C_DATA_SCHEMA_IMPORTER = False
    C_DATA_ARRAY_IMPORTER = False

    # the name used for skipping and shown in the logs
    name = "unknown"

    def __init__(self, debug=False, **args):
        self.args = args
        self.debug = debug

    def run_shell_command(self, cmd, **kwargs):
        cmd = ' '.join(cmd)
        if self.debug:
            log(cmd)
        kwargs.update(shell=True)
        subprocess.check_call(cmd, **kwargs)

    def json_to_file(self, json_path, arrow_path):
        """
        Run the conversion of an Arrow JSON integration file
        to an Arrow IPC file
        """
        raise NotImplementedError

    def stream_to_file(self, stream_path, file_path):
        """
        Run the conversion of an Arrow IPC stream to an
        Arrow IPC file
        """
        raise NotImplementedError

    def file_to_stream(self, file_path, stream_path):
        """
        Run the conversion of an Arrow IPC file to an Arrow IPC stream
        """
        raise NotImplementedError

    def validate(self, json_path, arrow_path, quirks=None):
        """
        Validate that the Arrow IPC file is equal to the corresponding
        Arrow JSON integration file
        """
        raise NotImplementedError

    def flight_server(self, scenario_name=None):
        """Start the Flight server on a free port.

        This should be a context manager that returns the port as the
        managed object, and cleans up the server on exit.
        """
        raise NotImplementedError

    def flight_request(self, port, json_path=None, scenario_name=None):
        raise NotImplementedError

    def make_c_data_exporter(self) -> CDataExporter:
        raise NotImplementedError

    def make_c_data_importer(self) -> CDataImporter:
        raise NotImplementedError

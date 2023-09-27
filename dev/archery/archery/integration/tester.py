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
        a C Data Interface export is called, `compare_allocation_state` is
        able to trigger the deallocation of the memory underlying the export
        (for example buffer data).

        If false, then `record_allocation_state` and `compare_allocation_state`
        are allowed to raise NotImplementedError.
        """

    def record_allocation_state(self) -> object:
        """
        Record the current memory allocation state.

        Returns
        -------
        state : object
            Opaque object representing the allocation state,
            for example the number of allocated bytes.
        """
        raise NotImplementedError

    def compare_allocation_state(self, recorded: object,
                                 gc_until: typing.Callable[[_Predicate], bool]
                                 ) -> bool:
        """
        Compare the current memory allocation state with the recorded one.

        Parameters
        ----------
        recorded : object
            The previous allocation state returned by
            `record_allocation_state()`
        gc_until : callable
            A callable itself accepting a callable predicate, and
            returning a boolean.
            `gc_until` should try to release memory until the predicate
            becomes true, or until it decides to give up. The final value
            of the predicate should be returned.
            `gc_until` is typically provided by the C Data Interface importer.

        Returns
        -------
        success : bool
            Whether memory allocation state finally reached its previously
            recorded value.
        """
        raise NotImplementedError


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

        Here, "release memory" means calling the `release` callback of
        a C Data Interface export (which should then trigger a deallocation
        mechanism on the exporter).

        If false, then `gc_until` is allowed to raise NotImplementedError.
        """

    def gc_until(self, predicate: _Predicate):
        """
        Try to release memory until the predicate becomes true, or fail.

        Depending on the CDataImporter implementation, this may for example
        try once, or run a garbage collector a given number of times, or
        any other implementation-specific strategy for releasing memory.

        The running time should be kept reasonable and compatible with
        execution of multiple C Data integration tests.

        This should not raise if `supports_releasing_memory` is true.

        Returns
        -------
        success : bool
            The final value of the predicate.
        """
        raise NotImplementedError


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
    C_DATA_EXPORTER = False
    # whether the language supports the C Data Interface as an importer
    C_DATA_IMPORTER = False

    # the name used for skipping and shown in the logs
    name = "unknown"

    def __init__(self, debug=False, **args):
        self.args = args
        self.debug = debug

    def run_shell_command(self, cmd):
        cmd = ' '.join(cmd)
        if self.debug:
            log(cmd)
        subprocess.check_call(cmd, shell=True)

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

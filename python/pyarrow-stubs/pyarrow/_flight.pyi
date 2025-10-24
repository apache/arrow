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

import asyncio
import enum
import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
from collections.abc import Generator, Iterable, Iterator, Sequence
from typing import Any, Generic, NamedTuple, TypeVar
from datetime import datetime
from typing_extensions import deprecated

from .ipc import _ReadPandasMixin, ReadStats
from .lib import (
    ArrowCancelled,
    ArrowException,
    ArrowInvalid,
    Buffer,
    IpcReadOptions,
    IpcWriteOptions,
    RecordBatch,
    RecordBatchReader,
    Scalar,
    Schema,
    Table,
    _CRecordBatchWriter,
    _Weakrefable,
)

_T = TypeVar("_T")


class FlightCallOptions(_Weakrefable):

    def __init__(
        self,
        timeout: float | None = None,
        write_options: IpcWriteOptions | None = None,
        headers: list[tuple[str | bytes, str | bytes]] | None = None,
        read_options: IpcReadOptions | None = None,
    ) -> None: ...


class CertKeyPair(NamedTuple):

    cert: str | bytes | None
    key: str | bytes | None


class FlightError(Exception):

    extra_info: bytes


class FlightInternalError(FlightError, ArrowException):
    ...


class FlightTimedOutError(FlightError, ArrowException):
    ...


class FlightCancelledError(FlightError, ArrowCancelled):
    def __init__(self, message: str, *, extra_info: bytes | None = None) -> None: ...


class FlightServerError(FlightError, ArrowException):
    ...


class FlightUnauthenticatedError(FlightError, ArrowException):
    ...


class FlightUnauthorizedError(FlightError, ArrowException):
    ...


class FlightUnavailableError(FlightError, ArrowException):
    ...


class FlightWriteSizeExceededError(ArrowInvalid):

    limit: int
    actual: int


class Action(_Weakrefable):

    def __init__(
        self, action_type: bytes | str, buf: Buffer | bytes | None) -> None: ...

    @property
    def type(self) -> str: ...

    @property
    def body(self) -> Buffer: ...

    def serialize(self) -> bytes: ...

    @classmethod
    def deserialize(cls, serialized: bytes) -> Self: ...


class ActionType(NamedTuple):

    type: str
    description: str

    def make_action(self, buf: Buffer | bytes) -> Action: ...


class Result(_Weakrefable):

    def __init__(self, buf: Buffer | bytes) -> None: ...

    @property
    def body(self) -> Buffer: ...

    def serialize(self) -> bytes: ...

    @classmethod
    def deserialize(cls, serialized: bytes) -> Self: ...


class BasicAuth(_Weakrefable):

    def __init__(
        self, username: str | bytes | None = None, password: str | bytes | None = None
    ) -> None: ...

    @property
    def username(self) -> bytes: ...
    @property
    def password(self) -> bytes: ...
    def serialize(self) -> str: ...
    @staticmethod
    def deserialize(serialized: str | bytes) -> BasicAuth: ...


class DescriptorType(enum.Enum):

    UNKNOWN = 0
    PATH = 1
    CMD = 2


class FlightMethod(enum.Enum):

    INVALID = 0
    HANDSHAKE = 1
    LIST_FLIGHTS = 2
    GET_FLIGHT_INFO = 3
    GET_SCHEMA = 4
    DO_GET = 5
    DO_PUT = 6
    DO_ACTION = 7
    LIST_ACTIONS = 8
    DO_EXCHANGE = 9


class FlightDescriptor(_Weakrefable):

    @staticmethod
    def for_path(*path: str | bytes) -> FlightDescriptor: ...

    @staticmethod
    def for_command(command: str | bytes) -> FlightDescriptor: ...

    @property
    def descriptor_type(self) -> DescriptorType: ...

    @property
    def path(self) -> list[bytes] | None: ...

    @property
    def command(self) -> bytes | None: ...

    def serialize(self) -> bytes: ...
    @classmethod
    def deserialize(cls, serialized: bytes) -> Self: ...


class Ticket(_Weakrefable):

    def __init__(self, ticket: str | bytes) -> None: ...
    @property
    def ticket(self) -> bytes: ...
    def serialize(self) -> bytes: ...
    @classmethod
    def deserialize(cls, serialized: bytes) -> Self: ...


class Location(_Weakrefable):

    def __init__(self, uri: str | bytes) -> None: ...
    @property
    def uri(self) -> bytes: ...
    def equals(self, other: Location) -> bool: ...
    @staticmethod
    def for_grpc_tcp(host: str | bytes, port: int) -> Location: ...

    @staticmethod
    def for_grpc_tls(host: str | bytes, port: int) -> Location: ...

    @staticmethod
    def for_grpc_unix(path: str | bytes) -> Location: ...


class FlightEndpoint(_Weakrefable):

    def __init__(
        self,
        ticket: Ticket | str | bytes | object,
        locations: list[str | bytes | Location | object],
        expiration_time: Scalar[Any] | str | datetime | None = ...,
        app_metadata: bytes | str | object = ...,
    ): ...

    @property
    def ticket(self) -> Ticket: ...

    @property
    def locations(self) -> list[Location]: ...

    def serialize(self) -> bytes: ...
    @property
    def expiration_time(self) -> Scalar[Any] | None: ...

    @property
    def app_metadata(self) -> bytes | str: ...

    @classmethod
    def deserialize(cls, serialized: bytes) -> Self: ...


class SchemaResult(_Weakrefable):

    def __init__(self, schema: Schema) -> None: ...

    @property
    def schema(self) -> Schema: ...

    def serialize(self) -> bytes: ...
    @classmethod
    def deserialize(cls, serialized: bytes) -> Self: ...


class FlightInfo(_Weakrefable):

    def __init__(
        self,
        schema: Schema | None,
        descriptor: FlightDescriptor,
        endpoints: list[FlightEndpoint],
        total_records: int | None = ...,
        total_bytes: int | None = ...,
        ordered: bool = ...,
        app_metadata: bytes | str = ...,
    ) -> None: ...

    @property
    def schema(self) -> Schema | None: ...

    @property
    def descriptor(self) -> FlightDescriptor: ...

    @property
    def endpoints(self) -> list[FlightEndpoint]: ...

    @property
    def total_records(self) -> int: ...

    @property
    def total_bytes(self) -> int: ...

    @property
    def ordered(self) -> bool: ...

    @property
    def app_metadata(self) -> bytes | str: ...

    def serialize(self) -> bytes: ...
    @classmethod
    def deserialize(cls, serialized: bytes) -> Self: ...


class FlightStreamChunk(_Weakrefable):

    @property
    def data(self) -> RecordBatch | None: ...
    @property
    def app_metadata(self) -> Buffer | None: ...
    def __iter__(self): ...


class _MetadataRecordBatchReader(_Weakrefable, _ReadPandasMixin):

    # Needs to be separate class so the "real" class can subclass the
    # pure-Python mixin class

    def __iter__(self) -> Self: ...
    def __next__(self) -> FlightStreamChunk: ...
    @property
    def schema(self) -> Schema: ...

    def read_all(self) -> Table: ...

    def read_chunk(self) -> FlightStreamChunk: ...

    def to_reader(self) -> RecordBatchReader: ...


class MetadataRecordBatchReader(_MetadataRecordBatchReader):
    @property
    def stats(self) -> ReadStats: ...


class FlightStreamReader(MetadataRecordBatchReader):
    @property
    def stats(self) -> ReadStats: ...

    def cancel(self) -> None: ...

    def read_all(self) -> Table: ...

    def read(self) -> RecordBatch | None: ...


class MetadataRecordBatchWriter(_CRecordBatchWriter):

    def begin(self, schema: Schema, options: IpcWriteOptions | None = None) -> None: ...

    def write_metadata(self, buf: Buffer | bytes) -> None: ...

    def write_batch(self, batch: RecordBatch) -> None: ...  # type: ignore[override]

    def write_table(self, table: Table, max_chunksize: int |
                    None = None, **kwargs) -> None: ...

    def close(self) -> None: ...

    def write_with_metadata(self, batch: RecordBatch, buf: Buffer | bytes) -> None: ...


class FlightStreamWriter(MetadataRecordBatchWriter):

    def done_writing(self) -> None: ...


class FlightMetadataReader(_Weakrefable):

    def read(self) -> Buffer | None: ...


class FlightMetadataWriter(_Weakrefable):

    def write(self, message: Buffer) -> None: ...


class AsyncioCall(Generic[_T]):

    _future: asyncio.Future[_T]

    def as_awaitable(self) -> asyncio.Future[_T]: ...
    def wakeup(self, result_or_exception: BaseException | _T) -> None: ...


class AsyncioFlightClient:

    def __init__(self, client: FlightClient) -> None: ...

    async def get_flight_info(
        self,
        descriptor: FlightDescriptor,
        *,
        options: FlightCallOptions | None = None,
    ): ...


class FlightClient(_Weakrefable):

    def __init__(
        self,
        location: str | tuple[str, int] | Location,
        *,
        tls_root_certs: str | None = None,
        cert_chain: str | None = None,
        private_key: str | None = None,
        override_hostname: str | None = None,
        middleware: list[ClientMiddlewareFactory] | None = None,
        write_size_limit_bytes: int | None = None,
        disable_server_verification: bool = False,
        generic_options: list[tuple[str, int | str]] | None = None,
    ): ...
    @property
    def supports_async(self) -> bool: ...
    def as_async(self) -> AsyncioFlightClient: ...
    def wait_for_available(self, timeout: int = 5) -> None: ...

    @classmethod
    @deprecated(
        "Use the ``FlightClient`` constructor or "
        "``pyarrow.flight.connect`` function instead."
    )
    def connect(
        cls,
        location: str | tuple[str, int] | Location,
        tls_root_certs: str | None = None,
        cert_chain: str | None = None,
        private_key: str | None = None,
        override_hostname: str | None = None,
        disable_server_verification: bool = False,
    ) -> FlightClient: ...

    def authenticate(
        self, auth_handler: ClientAuthHandler, options: FlightCallOptions | None = None
    ) -> None: ...

    def authenticate_basic_token(
        self, username: str | bytes, password: str | bytes,
        options: FlightCallOptions | None = None
    ) -> tuple[str, str]: ...

    def list_actions(self, options: FlightCallOptions |
                     None = None) -> list[Action]: ...

    def do_action(
        self, action: Action | tuple[bytes | str, bytes | str] | str,
        options: FlightCallOptions | None = None
    ) -> Iterator[Result]: ...

    def list_flights(
        self, criteria: str | bytes | None = None,
        options: FlightCallOptions | None = None
    ) -> Generator[FlightInfo, None, None]: ...

    def get_flight_info(
        self, descriptor: FlightDescriptor, options: FlightCallOptions | None = None
    ) -> FlightInfo: ...

    def get_schema(
        self, descriptor: FlightDescriptor, options: FlightCallOptions | None = None
    ) -> SchemaResult: ...

    def do_get(
        self, ticket: Ticket, options: FlightCallOptions | None = None
    ) -> FlightStreamReader: ...

    def do_put(
        self,
        descriptor: FlightDescriptor,
        schema: Schema | None,
        options: FlightCallOptions | None = None,
    ) -> tuple[FlightStreamWriter, FlightStreamReader]: ...

    def do_exchange(
        self, descriptor: FlightDescriptor, options: FlightCallOptions | None = None
    ) -> tuple[FlightStreamWriter, FlightStreamReader]: ...

    def close(self) -> None: ...

    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...


class FlightDataStream(_Weakrefable):
    ...


class RecordBatchStream(FlightDataStream):
    def __init__(self, data_source: RecordBatchReader | Table | None = None,
                 options: IpcWriteOptions | None = None) -> None: ...


class GeneratorStream(FlightDataStream):
    def __init__(
        self,
        schema: Schema,
        generator: Iterable[
            FlightDataStream
            | Table
            | RecordBatch
            | RecordBatchReader
            | tuple[RecordBatch, bytes]
        ],
        options: IpcWriteOptions | None = None,
    ) -> None: ...


class ServerCallContext(_Weakrefable):

    def peer_identity(self) -> bytes: ...

    def peer(self) -> str: ...

    # Set safe=True as gRPC on Windows sometimes gives garbage bytes
    def is_cancelled(self) -> bool: ...

    def add_header(self, key: str, value: str) -> None: ...

    def add_trailer(self, key: str, value: str) -> None: ...

    def get_middleware(self, key: str) -> ServerMiddleware | None: ...


class ServerAuthReader(_Weakrefable):

    def read(self) -> str: ...


class ServerAuthSender(_Weakrefable):

    def write(self, message: str) -> None: ...


class ClientAuthReader(_Weakrefable):

    def read(self) -> str: ...


class ClientAuthSender(_Weakrefable):

    def write(self, message: str) -> None: ...


class ServerAuthHandler(_Weakrefable):

    def authenticate(self, outgoing: ServerAuthSender, incoming: ServerAuthReader): ...

    def is_valid(self, token: str) -> bool: ...


class ClientAuthHandler(_Weakrefable):

    def authenticate(self, outgoing: ClientAuthSender, incoming: ClientAuthReader): ...

    def get_token(self) -> str: ...


class CallInfo(NamedTuple):

    method: FlightMethod


class ClientMiddlewareFactory(_Weakrefable):

    def start_call(self, info: CallInfo) -> ClientMiddleware | None: ...


class ClientMiddleware(_Weakrefable):

    def sending_headers(self) -> dict[str, list[str] | list[bytes]]: ...

    def received_headers(self, headers: dict[str, list[str] | list[bytes]]): ...

    def call_completed(self, exception: ArrowException): ...


class ServerMiddlewareFactory(_Weakrefable):

    def start_call(
        self, info: CallInfo, headers: dict[str, list[str] | list[bytes]]
    ) -> ServerMiddleware | None: ...


class TracingServerMiddlewareFactory(ServerMiddlewareFactory):
    ...


class ServerMiddleware(_Weakrefable):

    def sending_headers(self) -> dict[str, list[str] | list[bytes]]: ...

    def call_completed(self, exception: ArrowException): ...

    @property
    def trace_context(self) -> dict: ...


class TracingServerMiddleware(ServerMiddleware):
    trace_context: dict
    def __init__(self, trace_context: dict) -> None: ...


class _ServerMiddlewareFactoryWrapper(ServerMiddlewareFactory):

    def __init__(self, factories: dict[str, ServerMiddlewareFactory]) -> None: ...

    def start_call(  # type: ignore[override]
        self, info: CallInfo, headers: dict[str, list[str] | list[bytes]]
    ) -> _ServerMiddlewareFactoryWrapper | None: ...


class _ServerMiddlewareWrapper(ServerMiddleware):
    def __init__(self, middleware: dict[str, ServerMiddleware]) -> None: ...
    def send_headers(self) -> dict[str, dict[str, list[str] | list[bytes]]]: ...
    def call_completed(self, exception: ArrowException) -> None: ...


class _FlightServerFinalizer(_Weakrefable):

    def finalize(self) -> None: ...


class FlightServerBase(_Weakrefable):

    def __init__(
        self,
        location: str | tuple[str, int] | Location | None = None,
        auth_handler: ServerAuthHandler | None = None,
        tls_certificates: list[tuple[str, str]] | None = None,
        verify_client: bool = False,
        root_certificates: str | None = None,
        middleware: dict[str, ServerMiddlewareFactory] | None = None,
    ): ...
    @property
    def port(self) -> int: ...

    def list_flights(self, context: ServerCallContext,
                     criteria: str) -> Iterator[FlightInfo]: ...

    def get_flight_info(
        self, context: ServerCallContext, descriptor: FlightDescriptor
    ) -> FlightInfo: ...

    def get_schema(self, context: ServerCallContext,
                   descriptor: FlightDescriptor) -> Schema: ...

    def do_put(
        self,
        context: ServerCallContext,
        descriptor: FlightDescriptor,
        reader: MetadataRecordBatchReader,
        writer: FlightMetadataWriter,
    ) -> None: ...

    def do_get(self, context: ServerCallContext,
               ticket: Ticket) -> FlightDataStream: ...

    def do_exchange(
        self,
        context: ServerCallContext,
        descriptor: FlightDescriptor,
        reader: MetadataRecordBatchReader,
        writer: MetadataRecordBatchWriter,
    ) -> None: ...

    def list_actions(self, context: ServerCallContext) -> Iterable[Action]: ...

    def do_action(self, context: ServerCallContext,
                  action: Action) -> Iterable[bytes]: ...

    def serve(self) -> None: ...

    def run(self) -> None: ...

    def shutdown(self) -> None: ...

    def wait(self) -> None: ...

    def __enter__(self) -> Self: ...
    def __exit__(
        self, exc_type: object, exc_value: object, traceback: object) -> None: ...


def connect(
    location: str | tuple[str, int] | Location,
    *,
    tls_root_certs: str | None = None,
    cert_chain: str | None = None,
    private_key: str | None = None,
    override_hostname: str | None = None,
    middleware: list[ClientMiddlewareFactory] | None = None,
    write_size_limit_bytes: int | None = None,
    disable_server_verification: bool = False,
    generic_options: Sequence[tuple[str, int | str]] | None = None,
) -> FlightClient: ...

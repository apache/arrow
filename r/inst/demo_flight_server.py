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
    An example Flight Python server.
    See https://github.com/apache/arrow/blob/main/python/examples/flight/server.py
"""

import ast
import threading
import time

import pyarrow
import pyarrow.flight


class DemoFlightServer(pyarrow.flight.FlightServerBase):
    def __init__(self, host="localhost", port=5005):
        if isinstance(port, float):
            # Because R is looser with integer vs. float
            port = int(port)
        location = "grpc+tcp://{}:{}".format(host, port)
        super(DemoFlightServer, self).__init__(location)
        self.flights = {}
        self.host = host

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))

    def _make_flight_info(self, key, descriptor, table):
        location = pyarrow.flight.Location.for_grpc_tcp(self.host, self.port)
        endpoints = [pyarrow.flight.FlightEndpoint(repr(key), [location]), ]

        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(
            mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()

        return pyarrow.flight.FlightInfo(table.schema,
                                         descriptor, endpoints,
                                         table.num_rows, data_size)

    def list_flights(self, context, criteria):
        print("list_flights")
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = \
                    pyarrow.flight.FlightDescriptor.for_command(key[1])
            else:
                descriptor = pyarrow.flight.FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor, table)

    def get_flight_info(self, context, descriptor):
        print("get_flight_info")
        key = DemoFlightServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise KeyError('Flight not found.')

    def do_put(self, context, descriptor, reader, writer):
        print("do_put")
        key = DemoFlightServer.descriptor_to_key(descriptor)
        print(key)
        self.flights[key] = reader.read_all()
        print(self.flights[key])

    def do_get(self, context, ticket):
        print("do_get")
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        return pyarrow.flight.RecordBatchStream(self.flights[key])

    def list_actions(self, context):
        print("list_actions")
        return [
            ("clear", "Clear the stored flights."),
            ("shutdown", "Shut down this server."),
        ]

    def do_action(self, context, action):
        print("do_action")
        if action.type == "clear":
            raise NotImplementedError(
                "{} is not implemented.".format(action.type))
        elif action.type == "healthcheck":
            pass
        elif action.type == "shutdown":
            yield pyarrow.flight.Result(pyarrow.py_buffer(b'Shutdown!'))
            # Shut down on background thread to avoid blocking current
            # request
            threading.Thread(target=self._shutdown).start()
        else:
            raise KeyError("Unknown action {!r}".format(action.type))

    def _shutdown(self):
        """Shut down after a delay."""
        print("Server is shutting down...")
        time.sleep(2)
        self.shutdown()

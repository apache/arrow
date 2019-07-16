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

"""An example Flight Python server."""

import argparse
import ast
import threading
import time

import pyarrow
import pyarrow.flight


class FlightServer(pyarrow.flight.FlightServerBase):
    def __init__(self):
        super(FlightServer, self).__init__()
        self.flights = {}

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))

    def list_flights(self, context, criteria):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = \
                    pyarrow.flight.FlightDescriptor.for_command(key[1])
            else:
                descriptor = pyarrow.flight.FlightDescriptor.for_path(*key[2])

            endpoints = [
                pyarrow.flight.FlightEndpoint(repr(key),
                                              [('localhost', 5005)]),
            ]
            yield pyarrow.flight.FlightInfo(table.schema,
                                            descriptor, endpoints,
                                            table.num_rows, 0)

    def get_flight_info(self, context, descriptor):
        key = FlightServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            endpoints = [
                pyarrow.flight.FlightEndpoint(repr(key),
                                              [('localhost', 5005)]),
            ]
            return pyarrow.flight.FlightInfo(table.schema,
                                             descriptor, endpoints,
                                             table.num_rows, 0)
        raise KeyError('Flight not found.')

    def do_put(self, context, descriptor, reader):
        key = FlightServer.descriptor_to_key(descriptor)
        print(key)
        self.flights[key] = reader.read_all()
        print(self.flights[key])

    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        return pyarrow.flight.RecordBatchStream(self.flights[key])

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("shutdown", "Shut down this server."),
        ]

    def do_action(self, context, action):
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
            raise KeyError(f"Unknown action {action.type!r}")

    def _shutdown(self):
        """Shut down after a delay."""
        print("Server is shutting down...")
        time.sleep(2)
        self.shutdown()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5005)
    parser.add_argument("--tls", nargs=2, default=None)

    args = parser.parse_args()

    server = FlightServer()
    kwargs = {}
    scheme = "grpc+tcp"
    if args.tls:
        scheme = "grpc+tls"
        with open(args.tls[0], "rb") as cert_file:
            kwargs["tls_cert_chain"] = cert_file.read()
        with open(args.tls[1], "rb") as key_file:
            kwargs["tls_private_key"] = key_file.read()

    location = "{}://0.0.0.0:{}".format(scheme, args.port)
    print("Serving on", location)
    server.run(location, **kwargs)


if __name__ == '__main__':
    main()

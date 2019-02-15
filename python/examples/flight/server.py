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

import ast

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

    def list_flights(self, criteria):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = pyarrow.flight.FlightDescriptor.for_command(key[1])
            else:
                descriptor = pyarrow.flight.FlightDescriptor.for_path(*key[2])

            endpoints = [
                pyarrow.flight.FlightEndpoint(repr(key), [('localhost', 5005)]),
            ]
            yield pyarrow.flight.FlightInfo(table.schema,
                                            descriptor, endpoints,
                                            table.num_rows, 0)

    def get_flight_info(self, descriptor):
        key = FlightServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            endpoints = [
                pyarrow.flight.FlightEndpoint(repr(key), [('localhost', 5005)]),
            ]
            return pyarrow.flight.FlightInfo(table.schema,
                                             descriptor, endpoints,
                                             table.num_rows, 0)
        raise KeyError('Flight not found.')

    def do_put(self, descriptor, reader):
        key = FlightServer.descriptor_to_key(descriptor)
        print(key)
        self.flights[key] = reader.read_all()
        print(self.flights[key])

    def do_get(self, ticket):
        print(ticket.ticket)
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        return pyarrow.flight.RecordBatchStream(self.flights[key])


def main():
    server = FlightServer()
    server.run(5005)


if __name__ == '__main__':
    main()

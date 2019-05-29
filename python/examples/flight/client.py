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

"""An example Flight CLI client."""

import argparse
import sys

import pyarrow
import pyarrow.flight


def list_flights(args, client):
    print('Flights\n=======')
    for flight in client.list_flights():
        descriptor = flight.descriptor
        if descriptor.descriptor_type == pyarrow.flight.DescriptorType.PATH:
            print("Path:", descriptor.path)
        elif descriptor.descriptor_type == pyarrow.flight.DescriptorType.CMD:
            print("Command:", descriptor.command)
        else:
            print("Unknown descriptor type")

        print("Total records:", end=" ")
        if flight.total_records >= 0:
            print(flight.total_records)
        else:
            print("Unknown")

        print("Total bytes:", end=" ")
        if flight.total_bytes >= 0:
            print(flight.total_bytes)
        else:
            print("Unknown")

        print("Number of endpoints:", len(flight.endpoints))

        if args.list:
            print(flight.schema)

        print('---')

    print('\nActions\n=======')
    for action in client.list_actions():
        print("Type:", action.type)
        print("Description:", action.description)
        print('---')


def do_action(args, client):
    try:
        buf = pyarrow.allocate_buffer(0)
        action = pyarrow.flight.Action(args.action_type, buf)
        print('Running action', args.action_type)
        for result in client.do_action(action):
            print("Got result", result.body.to_pybytes())
    except pyarrow.lib.ArrowIOError as e:
        print("Error calling action:", e)


def get_flight(args, client):
    if args.path:
        descriptor = pyarrow.flight.FlightDescriptor.for_path(*args.path)
    else:
        descriptor = pyarrow.flight.FlightDescriptor.for_command(args.command)

    info = client.get_flight_info(descriptor)
    for endpoint in info.endpoints:
        print('Ticket:', endpoint.ticket)
        for location in endpoint.locations:
            print(location)
            get_client = pyarrow.flight.FlightClient.connect(location)
            reader = get_client.do_get(endpoint.ticket)
            df = reader.read_pandas()
            print(df)


def _add_common_arguments(parser):
    parser.add_argument('--tls', action='store_true')
    parser.add_argument('--tls-roots', default=None)
    parser.add_argument('host', type=str,
                        help="The host to connect to.")


def main():
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers()

    cmd_list = subcommands.add_parser('list')
    cmd_list.set_defaults(action='list')
    _add_common_arguments(cmd_list)
    cmd_list.add_argument('-l', '--list', action='store_true',
                          help="Print more details.")

    cmd_do = subcommands.add_parser('do')
    cmd_do.set_defaults(action='do')
    _add_common_arguments(cmd_do)
    cmd_do.add_argument('action_type', type=str,
                        help="The action type to run.")

    cmd_get = subcommands.add_parser('get')
    cmd_get.set_defaults(action='get')
    _add_common_arguments(cmd_get)
    cmd_get_descriptor = cmd_get.add_mutually_exclusive_group(required=True)
    cmd_get_descriptor.add_argument('-p', '--path', type=str, action='append',
                                    help="The path for the descriptor.")
    cmd_get_descriptor.add_argument('-c', '--command', type=str,
                                    help="The command for the descriptor.")

    args = parser.parse_args()
    if not hasattr(args, 'action'):
        parser.print_help()
        sys.exit(1)

    commands = {
        'list': list_flights,
        'do': do_action,
        'get': get_flight,
    }
    host, port = args.host.split(':')
    port = int(port)
    scheme = "grpc+tcp"
    connection_args = {}
    if args.tls:
        scheme = "grpc+tls"
        if args.tls_roots:
            with open(args.tls_roots, "rb") as root_certs:
                connection_args["tls_root_certs"] = root_certs.read()
    client = pyarrow.flight.FlightClient.connect(f"{scheme}://{host}:{port}",
                                                 **connection_args)
    while True:
        try:
            action = pyarrow.flight.Action("healthcheck", b"")
            options = pyarrow.flight.FlightCallOptions(timeout=1)
            list(client.do_action(action, options=options))
            break
        except pyarrow.ArrowIOError as e:
            if "Deadline" in str(e):
                print("Server is not ready, waiting...")
    commands[args.action](args, client)


if __name__ == '__main__':
    main()

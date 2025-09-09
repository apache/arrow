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

install_headers(
    [
        'api.h',
        'client_auth.h',
        'client_cookie_middleware.h',
        'client.h',
        'client_middleware.h',
        'client_tracing_middleware.h',
        'middleware.h',
        'otel_logging.h',
        'platform.h',
        'server_auth.h',
        'server.h',
        'server_middleware.h',
        'server_tracing_middleware.h',
        'test_auth_handlers.h',
        'test_definitions.h',
        'test_flight_server.h',
        'test_util.h',
        'transport.h',
        'transport_server.h',
        'type_fwd.h',
        'types_async.h',
        'types.h',
        'visibility.h',
    ],
    subdir: 'arrow/flight',
)

grpc_dep = dependency('grpc++')
protobuf_dep = dependency('protobuf')
abseil_sync_dep = dependency('absl_synchronization')

fs = import('fs')
protoc = find_program('protoc')

flight_proto_path = fs.parent(meson.project_source_root()) / 'format'

# To ensure messages from proto files are created correctly, we need to
# pass in dllexport_decl=<...> . Unfortunately, it doesn't appear that we
# can just pass in dllexport_decl=ARROW_FLIGHT_EXPORT, as the visibility
# macro won't be easily available to the generated proto file. See also
# https://github.com/protocolbuffers/protobuf/issues/19422
if cpp_compiler.get_id() == 'msvc'
    if get_option('default_library') != 'static'
        proto_visibility = 'dllexport_decl=__declspec(dllexport):'
    else
        proto_visibility = ''
    endif
else
    proto_visibility = 'dllexport_decl=__attribute__((visibility("default"))):'
endif

flight_proto_files = custom_target(
    'arrow-flight-proto-files',
    input: [flight_proto_path / 'Flight.proto'],
    output: ['Flight.pb.cc', 'Flight.pb.h'],
    command: [
        protoc,
        '--proto_path=' + flight_proto_path,
        '--cpp_out=@0@@1@'.format(proto_visibility, meson.current_build_dir()),
        '@INPUT@',
    ],
)

grpc_cpp_plugin = find_program('grpc_cpp_plugin')
flight_proto_grpc_files = custom_target(
    'arrow-flight-proto-grpc-files',
    input: [flight_proto_path / 'Flight.proto'],
    output: ['Flight.grpc.pb.cc', 'Flight.grpc.pb.h'],
    command: [
        protoc,
        '--proto_path=' + flight_proto_path,
        '--grpc_out=' + meson.current_build_dir(),
        '--plugin=protoc-gen-grpc=' + grpc_cpp_plugin.full_path(),
        '@INPUT@',
    ],
)

arrow_flight_srcs = [
    'client.cc',
    'client_cookie_middleware.cc',
    'client_tracing_middleware.cc',
    'cookie_internal.cc',
    'middleware.cc',
    'serialization_internal.cc',
    'server.cc',
    'server_auth.cc',
    'server_tracing_middleware.cc',
    'transport.cc',
    'transport_server.cc',
    'transport/grpc/grpc_client.cc',
    'transport/grpc/grpc_server.cc',
    'transport/grpc/serialization_internal.cc',
    'transport/grpc/protocol_grpc_internal.cc',
    'transport/grpc/util_internal.cc',
    'types.cc',
]

thread_dep = dependency('threads')

arrow_flight = library(
    'arrow-flight',
    # We intentionally index flight_proto_grpc_files[1] so as to avoid
    # adding 'Flight.grpc.pb.cc' to the sources. This is required
    # because protocol_grpc_internal.cc includes the source file
    # directly; using as a source here will cause a ODR violation
    sources: arrow_flight_srcs + [
        flight_proto_files,
        flight_proto_grpc_files[1],
    ],
    dependencies: [
        arrow_dep,
        grpc_dep,
        protobuf_dep,
        abseil_sync_dep,
        thread_dep,
    ],
    cpp_shared_args: ['-DARROW_FLIGHT_EXPORTING'],
    cpp_static_args: ['-DARROW_FLIGHT_STATIC'],
    gnu_symbol_visibility: 'inlineshidden',
)

arrow_flight_dep = declare_dependency(
    link_with: arrow_flight,
    dependencies: [grpc_dep, protobuf_dep, abseil_sync_dep],
)

if needs_testing
    arrow_flight_testing_lib = library(
        'arrow-flight-testing',
        sources: [
            'test_auth_handlers.cc',
            'test_definitions.cc',
            'test_flight_server.cc',
            'test_util.cc',
        ],
        dependencies: [arrow_test_dep, arrow_flight_dep, thread_dep],
        cpp_shared_args: ['-DARROW_FLIGHT_EXPORTING'],
        cpp_static_args: ['-DARROW_FLIGHT_STATIC'],
        gnu_symbol_visibility: 'inlineshidden',
    )

    arrow_flight_test_dep = declare_dependency(
        link_with: arrow_flight_testing_lib,
        dependencies: [arrow_flight_dep],
    )
else
    arrow_flight_test_dep = disabler()
endif

flight_tests = ['flight_internals_test', 'flight_test']
foreach flight_test : flight_tests
    test_name = '@0@'.format(flight_test.replace('_', '-'))
    exc = executable(
        test_name,
        sources: [
            '@0@.cc'.format(flight_test),
            # flight_internals_test.cc transitively includes Flight.grpc.pb.h
            # so we must declare that here to avoid a race condition
            flight_proto_grpc_files[1],
        ],
        dependencies: [arrow_test_dep, arrow_flight_test_dep],
    )
    test(test_name, exc)
endforeach

flight_test_dep_no_main = [
    arrow_dep,
    arrow_flight_test_dep,
    gtest_dep,
    gmock_dep,
    gflags_dep,
]

if needs_tests or needs_benchmarks
    executable(
        'flight-test-server',
        sources: ['test_server.cc'],
        dependencies: flight_test_dep_no_main,
    )
endif

if needs_benchmarks
    server_proto_path = meson.project_source_root() / 'src' / 'arrow' / 'flight'
    flight_proto_files = custom_target(
        'arrow-flight-benchmark-perf-proto-files',
        input: [server_proto_path / 'perf.proto'],
        output: ['perf.pb.cc', 'perf.pb.h'],
        command: [
            protoc,
            '--proto_path=' + meson.current_source_dir(),
            '--cpp_out=' + meson.current_build_dir(),
            '@INPUT@',
        ],
    )

    executable(
        'arrow-flight-perf-server',
        sources: ['perf_server.cc'] + flight_proto_files,
        dependencies: [flight_test_dep_no_main, arrow_testing_dep],
    )

    executable(
        'arrow-flight-benchmark',
        sources: ['flight_benchmark.cc'] + flight_proto_files,
        dependencies: [flight_test_dep_no_main, arrow_testing_dep],
    )
endif

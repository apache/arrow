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

project('substrait', 'cpp')

protobuf_dep = dependency('protobuf')
protoc = find_program('protoc')

# To ensure messages from proto files are created correctly, we need to
# pass in dllexport_decl=<...> . Unfortunately, it doesn't appear that we
# can just pass in dllexport_decl=SUBSTRAIT_EXPORT, as the visibility
# macro won't be easily available to the generated proto file. See also
# https://github.com/protocolbuffers/protobuf/issues/19422
if meson.get_compiler('cpp').get_id() == 'msvc'
    if get_option('default_library') != 'static'
        proto_visibility = 'dllexport_decl=__declspec(dllexport):'
    else
        proto_visibility = ''
    endif
else
    proto_visibility = 'dllexport_decl=__attribute__((visibility("default"))):'
endif

substrait_protos_dir = meson.current_source_dir() / 'proto'
gen = generator(
    protoc,
    output: ['@BASENAME@.pb.cc', '@BASENAME@.pb.h'],
    arguments: [
        '--proto_path=@0@'.format(substrait_protos_dir),
        '--cpp_out=@0@@1@'.format(proto_visibility, '@BUILD_DIR@'),
        '@INPUT@',
    ],
)
protos = [
    'substrait/algebra.proto',
    'substrait/extended_expression.proto',
    'substrait/extensions/extensions.proto',
    'substrait/plan.proto',
    'substrait/type.proto',
]
generated_sources = []
foreach proto : protos
    generated_sources += [
        gen.process(
            meson.current_source_dir() / 'proto' / proto,
            preserve_path_from: substrait_protos_dir,
        ),
    ]
endforeach

substrait_dep = declare_dependency(
    sources: generated_sources,
    include_directories: include_directories('.'),
)
meson.override_dependency('substrait', substrait_dep)

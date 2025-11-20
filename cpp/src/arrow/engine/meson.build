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

install_headers(['api.h'], subdir: 'arrow/engine')

arrow_substrait_protos_dir = meson.project_source_root() / 'proto'
substrait_proto_gen = generator(
    protoc,
    output: ['@BASENAME@.pb.cc', '@BASENAME@.pb.h'],
    arguments: [
        '--proto_path=@0@'.format(
            subproject('substrait').get_variable('substrait_protos_dir'),
        ),
        '--proto_path=@0@'.format(arrow_substrait_protos_dir),
        '--cpp_out=@0@@1@'.format(proto_visibility, '@BUILD_DIR@'),
        '@INPUT@',
    ],
)
ext_proto_gen = substrait_proto_gen.process(
    meson.project_source_root() / 'proto' / 'substrait' / 'extension_rels.proto',
    preserve_path_from: meson.project_source_root() / 'proto',
)
substrait_ext_dep = declare_dependency(sources: ext_proto_gen)

substrait_dep = dependency('substrait')
arrow_substrait_srcs = files(
    'substrait/expression_internal.cc',
    'substrait/extended_expression_internal.cc',
    'substrait/extension_set.cc',
    'substrait/extension_types.cc',
    'substrait/options.cc',
    'substrait/plan_internal.cc',
    'substrait/relation_internal.cc',
    'substrait/serde.cc',
    'substrait/test_plan_builder.cc',
    'substrait/type_internal.cc',
    'substrait/util.cc',
    'substrait/util_internal.cc',
)
arrow_substrait_deps = [
    substrait_dep,
    substrait_ext_dep,
    arrow_dataset_dep,
    protobuf_dep,
]
arrow_substrait_pkgconf_req = ['arrow-dataset']

arrow_substrait_lib = library(
    'arrow_substrait',
    sources: arrow_substrait_srcs,
    dependencies: arrow_substrait_deps,
    cpp_static_args: ['-DARROW_ENGINE_STATIC'],
    cpp_shared_args: ['-DARROW_ENGINE_EXPORTING'],
    gnu_symbol_visibility: 'inlineshidden',
)

arrow_substrait_args = []
if get_option('default_library') == 'static'
    arrow_substrait_args += ['-DARROW_ENGINE_STATIC']
endif
arrow_substrait_dep = declare_dependency(
    link_with: arrow_substrait_lib,
    dependencies: arrow_substrait_deps,
    compile_args: arrow_substrait_args,
)
meson.override_dependency('arrow-substrait', arrow_substrait_dep)

pkg_config_cflags = []
if get_option('default_library') == 'static'
    pkg_config_cflags += ['-DARROW_ENGINE_STATIC']
endif
pkg.generate(
    arrow_substrait_lib,
    filebase: 'arrow-substrait',
    name: 'Apache Arrow Substrait Consumer',
    description: 'Apache Arrow\'s Substrait Consumer.',
    extra_cflags: [pkg_config_cflags],
    requires: arrow_substrait_pkgconf_req,
    variables: {'Cflags.private': '-DARROW_ENGINE_STATIC'},
)

arrow_substrait_test = executable(
    'substrait-test',
    sources: files(
        'substrait/ext_test.cc',
        'substrait/function_test.cc',
        'substrait/protobuf_test_util.cc',
        'substrait/serde_test.cc',
        'substrait/test_util.cc',
    ),
    dependencies: [arrow_substrait_dep, arrow_compute_test_dep],
)
test('substrait-test', arrow_substrait_test)

subdir('substrait')

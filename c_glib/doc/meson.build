# -*- indent-tabs-mode: nil -*-
#
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

source_reference = get_option('source_reference')
gi_docgen = find_program('gi-docgen')

gi_docgen_toml_conf = configuration_data()
gi_docgen_toml_conf.set('SOURCE_REFERENCE', source_reference)
# We can't use "version.replace('-SNAPSHOT', '.dev')" here because
# Ubuntu 20.04's Meson is < 0.58.0.
if version_tag == ''
    gi_docgen_version_tag = ''
else
    # GI-DocGen doesn't like MAJOR.MINOR.PATCH-SNAPSHOT format.
    gi_docgen_version_tag = '.dev'
endif
gi_docgen_version = '@0@.@1@.@2@@3@'.format(
    version_major,
    version_minor,
    version_micro,
    gi_docgen_version_tag,
)
gi_docgen_toml_conf.set('VERSION', gi_docgen_version)

gir_top_build_dir = meson.current_build_dir() / '..'
arrow_glib_gir_dir = gir_top_build_dir / 'arrow-glib'
arrow_flight_glib_gir_dir = gir_top_build_dir / 'arrow-flight-glib'
entries = [['arrow-glib', arrow_glib_gir[0]]]
if arrow_cuda.found()
    entries += [['arrow-cuda-glib', arrow_cuda_glib_gir[0]]]
endif
if arrow_dataset.found()
    entries += [['arrow-dataset-glib', arrow_dataset_glib_gir[0]]]
endif
if arrow_flight.found()
    entries += [['arrow-flight-glib', arrow_flight_glib_gir[0]]]
endif
if arrow_flight_sql.found()
    entries += [['arrow-flight-sql-glib', arrow_flight_sql_glib_gir[0]]]
endif
if gandiva.found()
    entries += [['gandiva-glib', gandiva_glib_gir[0]]]
endif
if parquet.found()
    entries += [['parquet-glib', parquet_glib_gir[0]]]
endif
foreach entry : entries
    module_name = entry[0]
    gir = entry[1]
    gi_docgen_toml = configure_file(
        input: '@0@.toml.in'.format(module_name),
        output: '@0@.toml'.format(module_name),
        configuration: gi_docgen_toml_conf,
    )
    gir_dir = gir_top_build_dir / module_name
    current_source_dir = meson.current_source_dir()
    command = [
        gi_docgen,
        'generate',
        '--add-include-path=@0@'.format(arrow_flight_glib_gir_dir),
        '--add-include-path=@0@'.format(arrow_glib_gir_dir),
        '--add-include-path=@0@'.format(gir_dir),
        '--config=@INPUT0@',
        '--content-dir=@0@'.format(current_source_dir),
        '--no-namespace-dir',
        '--output-dir=@OUTPUT@',
        '--quiet',
    ]
    if get_option('werror')
        command += ['--fatal-warnings']
    endif
    command += ['@INPUT1@']
    custom_target(
        '@0@-doc'.format(module_name),
        input: [gi_docgen_toml, gir],
        depend_files: ['urlmap.js'],
        output: module_name,
        command: command,
        build_by_default: true,
        install: true,
        install_dir: doc_dir,
    )
endforeach

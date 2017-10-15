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

subdir('xml')

private_headers = [
]

content_files = [
]

html_images = [
]

glib_prefix = dependency('glib-2.0').get_pkgconfig_variable('prefix')
glib_doc_path = join_paths(glib_prefix, 'share', 'gtk-doc', 'html')
doc_path = join_paths(data_dir, meson.project_name(), 'gtk-doc', 'html')

gnome.gtkdoc(meson.project_name(),
             main_xml: meson.project_name() + '-docs.sgml',
             src_dir: [
               join_paths(meson.source_root(), 'arrow-glib'),
               join_paths(meson.build_root(), 'arrow-glib'),
             ],
             dependencies: libarrow_glib_dependency,
             gobject_typesfile: meson.project_name() + '.types',
             scan_args: [
               '--rebuild-types',
               '--deprecated-guards=GARROW_DISABLE_DEPRECATED',
             ],
             mkdb_args: [
               '--output-format=xml',
               '--name-space=garrow',
               '--source-suffixes=c,cpp,h',
             ],
             fixxref_args: [
               '--html-dir=' + doc_path,
               '--extra-dir=' + join_paths(glib_doc_path, 'glib'),
               '--extra-dir=' + join_paths(glib_doc_path, 'gobject'),
             ],
             html_assets: html_images,
             install: true)

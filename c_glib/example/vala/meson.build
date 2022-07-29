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

if generate_vapi
  vala_example_executable_kwargs = {
    'c_args': [
      '-I' + meson.build_root(),
      '-I' + meson.source_root(),
    ],
    'dependencies': [
      arrow_glib_vapi,
      dependency('gio-2.0'),
    ],
    'vala_args': [
      '--pkg', 'posix',
    ],
  }
  executable('build', 'build.vala',
             kwargs: vala_example_executable_kwargs)
  executable('read-file', 'read-file.vala',
             kwargs: vala_example_executable_kwargs)
  executable('read-stream', 'read-stream.vala',
             kwargs: vala_example_executable_kwargs)
  executable('write-file', 'write-file.vala',
             kwargs: vala_example_executable_kwargs)
  executable('write-stream', 'write-stream.vala',
             kwargs: vala_example_executable_kwargs)
endif

install_data('README.md',
             'read-file.vala',
             'read-stream.vala',
             'write-file.vala',
             'write-stream.vala',
             install_dir: join_paths(data_dir,
                                     meson.project_name(),
                                     'example',
                                     'vala'))

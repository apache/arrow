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

entities_conf = configuration_data()
entities_conf.set('package', meson.project_name())
entities_conf.set('package_bugreport',
                  'https://issues.apache.org/jira/browse/ARROW')
entities_conf.set('package_name', meson.project_name())
entities_conf.set('package_string',
                  ' '.join([meson.project_name(), version]))
entities_conf.set('package_url', 'https://arrow.apache.org/')
entities_conf.set('package_version', version)
configure_file(input: 'gtkdocentities.ent.in',
               output: 'gtkdocentities.ent',
               configuration: entities_conf)

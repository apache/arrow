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

from ursabot.steps import ShellCommand, ResultLogMixin


class Archery(ResultLogMixin, ShellCommand):
    name = 'Archery'
    command = ['archery']
    env = dict(LC_ALL='C.UTF-8', LANG='C.UTF-8')  # required for click


class Crossbow(ResultLogMixin, ShellCommand):
    name = 'Crossbow'
    command = ['python', 'crossbow.py']
    env = dict(LC_ALL='C.UTF-8', LANG='C.UTF-8')  # required for click


# TODO(kszucs): consider to move the predefined steps here from builders.py

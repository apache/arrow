#!/usr/bin/env python

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

import subprocess

exceptions = [
    '__once_proxy',
]

lines = subprocess.check_output(['nm', '-D', '-C',
                                 '/arrow-dist/lib64/libarrow.so'])

lines = lines.decode('ascii')
lines = lines.split('\n')
lines = [line for line in lines if ' T ' in line]
lines = [line for line in lines if 'arrow' not in line]
symbols = [line.split(' ')[2] for line in lines]
symbols = [symbol for symbol in symbols if symbol not in exceptions]

if len(symbols) != 2:
    raise Exception(symbols)

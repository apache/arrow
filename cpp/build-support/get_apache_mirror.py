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

# This script queries the ASF mirror system to obtain a suggested
# mirror for downloading dependencies, e.g. in CMake

import json
try:
    import requests

    def get_url(url):
        return requests.get(url).content
except ImportError:
    try:
        from urllib2 import urlopen
    except ImportError:
        # py3
        from urllib.request import urlopen

    def get_url(url):
        return urlopen(url).read()

suggested_mirror = get_url('https://www.apache.org/dyn/'
                           'closer.cgi?as_json=1')
print(json.loads(suggested_mirror.decode('utf-8'))['preferred'])

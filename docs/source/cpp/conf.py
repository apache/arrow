# -*- coding: utf-8 -*-
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

import os
import sys


sys.path.extend([
    os.path.join(os.path.dirname(__file__), '..')
])

from conf_common import *


# Additional extensions
extensions = extensions + [
    'breathe',
]

# Breathe configuration
breathe_projects = {"arrow_cpp": "../../../cpp/apidoc/xml"}
breathe_default_project = "arrow_cpp"


# Paths that need to be adjusted
templates_path = ['../_templates']
html_logo = "../_static/arrow.png"
html_favicon = "../_static/favicon.ico"
html_static_path = ['../_static']

# -- Options for HTML output ----------------------------------------------

html_context["doc_path"] = "docs/source/cpp"
html_context["subproject"] = "C++"


# The base URL which points to the root of the HTML documentation,
# used for canonical url
html_baseurl = "https://arrow.apache.org/docs/cpp"

intersphinx_mapping["arrow"] = ("https://arrow.apache.org/docs/", ('../../_build/html/objects.inv', None))

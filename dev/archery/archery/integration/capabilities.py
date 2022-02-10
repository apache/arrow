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

from typing import Set
import os.path

import pydantic
import ruamel.yaml


class Capabilities(pydantic.BaseModel):
    """
    A set of capabilies of a language
    """
    name: str
    # the set of capabilities that are currently not supported
    # by a given language
    # these currently must be integration test cases as defined
    # in ``datagen.CASES``
    unsupported: Set[str]


# the set of languages supported to run integration tests
# To add a new language (X):
# 1. add it here
# 2. pipe the language through the CLI call (e.g. ``with_X```)
# 3. add a file "X.yaml" to ``languages`` with what the
#    language does not support
LANGUAGES = ["cpp", "java", "go", "rust", "csharp", "javascript"]


def language_capabilities(language: str) -> Capabilities:
    """
    Reads ``Capabilities`` of a given language
    """
    here = os.path.dirname(__file__)
    path = os.path.join(here, "languages", f"{language}.yaml")
    with open(path, "r") as f:
        return Capabilities(**ruamel.yaml.safe_load(f))

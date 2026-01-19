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

import pytest


# Save output files from doctest examples into temp dir
@pytest.fixture(autouse=True)
def _docdir(request):
    # Trigger ONLY for the doctests
    from _pytest.doctest import DoctestItem
    is_doctest = isinstance(request.node, DoctestItem)

    if is_doctest:
        # Get the fixture dynamically by its name.
        tmpdir = request.getfixturevalue('tmpdir')

        # Chdir only for the duration of the test.
        with tmpdir.as_cwd():
            yield
    else:
        yield

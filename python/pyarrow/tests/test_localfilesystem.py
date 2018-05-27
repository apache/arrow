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

import pyarrow as pa
import pytest
import os


@pytest.fixture(scope="module")
def head_files():
    with open("test_apache_license.txt", "w+") as license:
        license.write("""# Licensed to the Apache Software Foundation (ASF) under one
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
""")

    with open("test_setup.py", "w+") as license:
        license.write("""import contextlib
import glob
import os
import os.path as osp
import re
import shlex
import shutil
import sys

from Cython.Distutils import build_ext as _build_ext
import Cython


import pkg_resources
from setuptools import setup, Extension, Distribution

from os.path import join as pjoin

""")
    yield
    os.remove("test_apache_license.txt")
    os.remove("test_setup.py")


def test_lfs_single_file_head(head_files, capsys):
    head = """# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
"""
    pa.LocalFileSystem().head("test_apache_license.txt", 4)
    captured = capsys.readouterr()
    assert captured.out == head


def test_lfs_file_list_head(head_files, capsys):
    files = ["test_apache_license.txt", "test_setup.py"]
    head = """==> test_apache_license.txt <==
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

==> test_setup.py <==
import contextlib
import glob
import os
import os.path as osp
import re
import shlex
import shutil
import sys

from Cython.Distutils import build_ext as _build_ext

"""

    pa.LocalFileSystem().head(files)
    captured = capsys.readouterr()
    assert captured.out == head

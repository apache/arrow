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
import shutil
import subprocess
import sys

import pytest

import pyarrow as pa


here = os.path.dirname(os.path.abspath(__file__))

setup_template = """if 1:
    from distutils.core import setup
    from Cython.Build import cythonize

    import numpy as np

    import pyarrow as pa

    ext_modules = cythonize({pyx_file!r})
    compiler_opts = {compiler_opts!r}
    custom_ld_path = {test_ld_path!r}

    for ext in ext_modules:
        # XXX required for numpy/numpyconfig.h,
        # included from arrow/python/api.h
        ext.include_dirs.append(np.get_include())
        ext.include_dirs.append(pa.get_include())
        ext.libraries.extend(pa.get_libraries())
        ext.library_dirs.extend(pa.get_library_dirs())
        if custom_ld_path:
            ext.library_dirs.append(custom_ld_path)
        ext.extra_compile_args.extend(compiler_opts)

    setup(
        ext_modules=ext_modules,
    )
"""


@pytest.mark.skipif(
    'ARROW_HOME' not in os.environ,
    reason='ARROW_HOME environment variable not defined')
def test_cython_api(tmpdir):
    """
    Basic test for the Cython API.
    """
    pytest.importorskip('Cython')

    ld_path_default = os.path.join(os.environ['ARROW_HOME'], 'lib')

    test_ld_path = os.environ.get('PYARROW_TEST_LD_PATH', ld_path_default)

    with tmpdir.as_cwd():
        # Set up temporary workspace
        pyx_file = 'pyarrow_cython_example.pyx'
        shutil.copyfile(os.path.join(here, pyx_file),
                        os.path.join(str(tmpdir), pyx_file))
        # Create setup.py file
        if os.name == 'posix':
            compiler_opts = ['-std=c++11']
        else:
            compiler_opts = []
        setup_code = setup_template.format(pyx_file=pyx_file,
                                           compiler_opts=compiler_opts,
                                           test_ld_path=test_ld_path)
        with open('setup.py', 'w') as f:
            f.write(setup_code)

        # Compile extension module
        subprocess.check_call([sys.executable, 'setup.py',
                               'build_ext', '--inplace'])

        # Check basic functionality
        orig_path = sys.path[:]
        sys.path.insert(0, str(tmpdir))
        try:
            mod = __import__('pyarrow_cython_example')
            arr = pa.array([1, 2, 3])
            assert mod.get_array_length(arr) == 3
            with pytest.raises(TypeError, match="not an array"):
                mod.get_array_length(None)
        finally:
            sys.path = orig_path

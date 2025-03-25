# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

py = import('python').find_installation(pure: false)

# When NumPy 2.0 becomes the minimum we can remove the
# custom location check
numpy_dep = dependency('numpy', required: false)
if not numpy_dep.found()
    incdir_numpy = run_command(
        py,
        [
            '-c',
            '''
            import os
            import numpy as np
            try:
                # Check if include directory is inside the pyarrow dir
                # e.g. a venv created inside the pyarrow dir
                # If so, convert it to a relative path
                incdir = os.path.relpath(np.get_include())
            except Exception:
                incdir = np.get_include()
            print(incdir)
            ''',
        ],
        check: true,
    ).stdout().strip()

    numpy_dep = declare_dependency(include_directories: incdir_numpy)
endif

cython_args = ['--include-dir', meson.current_source_dir()]
if get_option('buildtype') in ['debug', 'debugoptimized']
    cython_args += ['--gdb']
endif

pyarrow_srcs = files(
    'src/arrow/python/arrow_to_pandas.cc',
    'src/arrow/python/benchmark.cc',
    'src/arrow/python/common.cc',
    'src/arrow/python/datetime.cc',
    'src/arrow/python/decimal.cc',
    'src/arrow/python/extension_type.cc',
    'src/arrow/python/gdb.cc',
    'src/arrow/python/helpers.cc',
    'src/arrow/python/inference.cc',
    'src/arrow/python/io.cc',
    'src/arrow/python/ipc.cc',
    'src/arrow/python/numpy_convert.cc',
    'src/arrow/python/numpy_init.cc',
    'src/arrow/python/numpy_to_arrow.cc',
    'src/arrow/python/pyarrow.cc',
    'src/arrow/python/python_test.cc',
    'src/arrow/python/python_to_arrow.cc',
    'src/arrow/python/udf.cc',
)

# TODO: these are optional components so should detect if needed
# if needs_csv
pyarrow_srcs += files('src/arrow/python/csv.cc')
#endif
# if needs_filesystem
pyarrow_srcs += files('src/arrow/python/filesystem.cc')
#endif

cython_generated_headers = custom_target(
    'lib-pyx-headers',
    input: ['lib.pyx'],
    output: ['lib.h', 'lib_api.h'],
    command: ['cythonize', '@INPUT@'],
)

cython_generated_dep = declare_dependency(sources: cython_generated_headers)

arrow_python_lib = shared_library(
    'arrow_python',
    sources: pyarrow_srcs,
    include_directories: ['src'],
    dependencies: [arrow_dep, numpy_dep, cython_generated_dep, py.dependency()],
    override_options: ['b_lundef=false'],
    install: true,
)

cython_module_names = [
    'lib',
    '_fs',
    '_csv',
    '_json',
    '_compute',
    #'_cuda',
    #'_flight',
    '_dataset',
    '_dataset_orc',
    '_dataset_parquet',
    '_acero',
    '_feather',
    '_parquet',
    '_parquet_encryption',
    '_pyarrow_cpp_tests',
    '_orc',
    '_azurefs',
    #'_gcsfs',
    '_s3fs',
    #'_substrait',
    '_hdfs',
    'gandiva',
]

foreach cython_module_name : cython_module_names
    py.extension_module(
        cython_module_name,
        sources: ['@0@.pyx'.format(cython_module_name)],
        include_directories: ['src'],
        link_with: arrow_python_lib,
        cython_args: cython_args,
        dependencies: [arrow_dep, numpy_dep],
        override_options: ['cython_language=cpp'],
        subdir: 'pyarrow',
        install: true,
    )
endforeach

pysources = [
    'acero.py',
    'benchmark.py',
    'cffi.py',
    '_compute_docstrings.py',
    'compute.py',
    'conftest.py',
    'csv.py',
    'cuda.py',
    'dataset.py',
    'feather.py',
    'flight.py',
    'fs.py',
    '_generated_version.py',
    '__init__.py',
    'ipc.py',
    'json.py',
    'jvm.py',
    'orc.py',
    'pandas_compat.py',
    'substrait.py',
    'types.py',
    'util.py',
]
py.install_sources(pysources, subdir: 'pyarrow')

subdirs = ['interchange', 'parquet', 'tests', 'vendored']

foreach subdir : subdirs
    install_subdir(subdir, install_dir: py.get_install_dir() / 'pyarrow')
endforeach

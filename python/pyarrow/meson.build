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

cython_modules = {
    'lib': {},
    '_compute': {},
    '_csv': {},
    '_feather': {},
    '_fs': {},
    '_json': {},
    '_pyarrow_cpp_tests': {},
}

if get_option('azure').enabled()
    cython_modules += {'_azurefs': {}}
endif

if get_option('gcs').enabled()
    cython_modules += {'_gcsfs': {}}
endif

if get_option('s3').enabled()
    cython_modules += {'_s3fs': {}}
endif

if get_option('hdfs').enabled()
    cython_modules += {'_hdfs': {}}
endif

if get_option('cuda').enabled()
    cuda_dep = dependency(
        'arrow-cuda',
        'ArrowCUDA',
        modules: ['ArrowCUDA::arrow_cuda_shared'],
    )
    cython_modules += {'_cuda': {'dependencies': cuda_dep}}
endif

if get_option('acero').enabled()
    acero_dep = dependency(
        'arrow-acero',
        'ArrowAcero',
        modules: ['ArrowAcero::arrow_acero_shared'],
    )

    cython_modules += {'_acero': {'dependencies': acero_dep}}
endif

if get_option('dataset').enabled()
    dataset_dep = dependency(
        'arrow-dataset',
        'ArrowDataset',
        modules: ['ArrowDataset::arrow_dataset_shared'],
    )

    cython_modules += {'_dataset': {'dependencies': dataset_dep}}
endif

if get_option('parquet').enabled()
    parquet_dep = dependency(
        'parquet',
        'Parquet',
        modules: ['Parquet::parquet_shared'],
    )

    cython_modules += {'_parquet': {'dependencies': parquet_dep}}

    if get_option('parquet_encryption').enabled()
        arrow_encryption_lib = shared_library(
            'arrow-python-parquet-encryption',
            sources: ['src/arrow/python/parquet_encryption.cc'],
            include_directories: ['src'],
            link_with: [arrow_python_lib],
            dependencies: [parquet_dep, py.dependency()],
            install: true,
            override_options: ['b_lundef=false'],
        )

        arrow_encryption_dep = declare_dependency(
            link_with: arrow_encryption_lib,
        )

        cython_modules += {
            '_parquet_encryption': {
                'dependencies': [parquet_dep, arrow_encryption_dep],
            },
        }
    endif

    if get_option('dataset').enabled()
        cython_modules += {
            '_dataset_parquet': {'dependencies': [dataset_dep, parquet_dep]},
        }

        if get_option('parquet_encryption').enabled()
            cython_modules += {
                '_dataset_parquet_encryption': {
                    'dependencies': [dataset_dep, parquet_dep],
                },
            }
        endif
    endif
endif

if get_option('parquet_encryption').enabled() and not get_option('parquet').enabled()
    warning(
        'Building PyArrow with Parquet Encryption is requested, but Parquet itself is not enabled. Ignoring the Parquet Encryption setting.',
    )
endif

if get_option('orc').enabled()
    cython_modules += {'_orc': {}}

    if get_option('dataset').enabled()
        cython_modules += {'_dataset_orc': {'dependencies': [dataset_dep]}}
    endif
endif

if get_option('flight').enabled()
    flight_dep = dependency(
        'arrow-flight',
        'ArrowFlight',
        modules: ['ArrowFlight::arrow_flight_shared'],
    )

    flight_lib = shared_library(
        'arrow-flight-lib',
        sources: ['src/arrow/python/flight.cc'],
        dependencies: [flight_dep, py.dependency()],
        include_directories: ['src'],
        install: true,
        override_options: ['b_lundef=false'],
    )

    arrow_flight_dep = declare_dependency(link_with: flight_lib)
    cython_modules += {'_flight': {'dependencies': arrow_flight_dep}}
endif

if get_option('substrait').enabled()
    substrait_dep = dependency(
        'arrow-substrait',
        'ArrowSubstrait',
        modules: ['ArrowSubstrait::arrow_substrait_shared'],
    )

    cython_modules += {'_substrait': {'dependencies': substrait_dep}}
endif

if get_option('gandiva').enabled()
    gandiva_dep = dependency(
        'gandiva',
        'Gandiva',
        modules: ['Gandiva::gandiva_shared'],
    )

    cython_modules += {'gandiva': {'dependencies': gandiva_dep}}
endif

foreach key, val : cython_modules
    py.extension_module(
        key,
        sources: ['@0@.pyx'.format(key)],
        include_directories: ['src'],
        link_with: arrow_python_lib,
        cython_args: cython_args,
        dependencies: [arrow_dep, numpy_dep] + val.get('dependencies', []),
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

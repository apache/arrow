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

cython_args = ['--include-dir', meson.current_source_dir()]
if get_option('buildtype') in ['debug', 'debugoptimized']
    cython_args += ['--gdb']
endif

pyarrow_srcs = files(
    'src/arrow/python/arrow_to_pandas.cc',
    'src/arrow/python/benchmark.cc',
    'src/arrow/python/common.cc',
    'src/arrow/python/config.cc',
    'src/arrow/python/csv.cc',
    'src/arrow/python/datetime.cc',
    'src/arrow/python/decimal.cc',
    'src/arrow/python/extension_type.cc',
    'src/arrow/python/filesystem.cc',
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
    'src/arrow/python/util.cc',
)

subdir('src/arrow/python')

if get_option('sdist')
    arrow_compute_dep = []
else
    arrow_compute_dep = dependency(
        'arrow-compute',
        'ArrowCompute',
        modules: [f'ArrowCompute::arrow_compute_@cmake_suffix@'],
    )
endif

libpyarrow_deps = [arrow_dep, arrow_compute_dep]

cython_modules = {
    'lib': {},
    '_compute': {'dependencies': arrow_compute_dep},
    '_csv': {},
    '_feather': {},
    '_fs': {},
    '_json': {},
    '_pyarrow_cpp_tests': {},
}

needs_substrait = get_option('substrait').enabled()
needs_dataset = get_option('dataset').enabled() or needs_substrait
needs_acero = get_option('acero').enabled() or needs_dataset
needs_flight = get_option('flight').enabled()

if needs_acero
    if get_option('sdist')
        arrow_acero_dep = []
    else
        arrow_acero_dep = dependency(
            'arrow-acero',
            'ArrowAcero',
            modules: [f'ArrowAcero::arrow_acero_@cmake_suffix@'],
        )
    endif
    libpyarrow_deps += [arrow_acero_dep]
    cython_modules += {'_acero': {'dependencies': arrow_acero_dep}}
endif

if get_option('azure').enabled()
    cython_modules += {'_azurefs': {}}
endif

if get_option('cuda').enabled()
    if get_option('sdist')
        arrow_cuda_dep = []
    else
        arrow_cuda_dep = dependency(
            'arrow-cuda',
            'ArrowCUDA',
            modules: [f'ArrowCUDA::arrow_cuda_@cmake_suffix@'],
        )
    endif
    libpyarrow_deps += [arrow_cuda_dep]
    cython_modules += {'_cuda': {'dependencies': arrow_cuda_dep}}
endif

if needs_dataset
    if get_option('sdist')
        arrow_dataset_dep = []
    else
        arrow_dataset_dep = dependency(
            'arrow-dataset',
            'ArrowDataset',
            modules: [f'ArrowDataset::arrow_dataset_@cmake_suffix@'],
        )
    endif
    libpyarrow_deps += [arrow_dataset_dep]
    cython_modules += {'_dataset': {'dependencies': arrow_dataset_dep}}
endif

if needs_flight
    if get_option('sdist')
        arrow_flight_dep = []
    else
        arrow_flight_dep = dependency(
            'arrow-flight',
            'ArrowFlight',
            modules: [f'ArrowFlight::arrow_flight_@cmake_suffix@'],
        )
    endif
    libpyarrow_deps += [arrow_flight_dep]
endif

if get_option('gandiva').enabled()
    if get_option('sdist')
        gandiva_dep = []
    else
        gandiva_dep = dependency(
            'gandiva',
            'Gandiva',
            modules: [f'Gandiva::gandiva_@cmake_suffix@'],
        )
    endif
    libpyarrow_deps += [gandiva_dep]
    cython_modules += {'gandiva': {'dependencies': gandiva_dep}}
endif

if get_option('gcs').enabled()
    cython_modules += {'_gcsfs': {}}
endif

if get_option('hdfs').enabled()
    cython_modules += {'_hdfs': {}}
endif

if get_option('orc').enabled()
    cython_modules += {'_orc': {}}

    if needs_dataset
        cython_modules += {'_dataset_orc': {'dependencies': arrow_dataset_dep}}
    endif
endif

if get_option('s3').enabled()
    cython_modules += {'_s3fs': {}}
endif

if needs_substrait
    if get_option('sdist')
        arrow_substrait_dep = []
    else
        arrow_substrait_dep = dependency(
            'arrow-substrait',
            'ArrowSubstrait',
            modules: [f'ArrowSubstrait::arrow_substrait_@cmake_suffix@'],
        )
    endif
    libpyarrow_deps += [arrow_substrait_dep]
    cython_modules += {'_substrait': {'dependencies': arrow_substrait_dep}}
endif

needs_parquet = get_option('parquet').enabled()
needs_parquet_encryption = get_option('parquet_require_encryption').enabled()
if needs_parquet_encryption and not needs_parquet
    warning(
        '''
        Building PyArrow with Parquet Encryption is requested, but Parquet
        itself is not enabled. Ignoring the Parquet Encryption setting.,
    ''',
    )
    needs_parquet_encryption = false
endif

if needs_parquet
    if get_option('sdist')
        parquet_dep = []
    else
        parquet_dep = dependency(
            'parquet',
            'Parquet',
            modules: [f'Parquet::parquet_@cmake_suffix@'],
        )
    endif
    libpyarrow_deps += [parquet_dep]
    cython_modules += {'_parquet': {'dependencies': parquet_dep}}

    if needs_dataset
        cython_modules += {
            '_dataset_parquet': {
                'dependencies': [parquet_dep, arrow_dataset_dep],
            },
        }
    endif
endif

gnu_symbol_visibility = host_machine.system() == 'darwin' ? 'default' : 'inlineshidden'

if get_option('default_library') == 'static'
    pyarrow_private_args = ['-DARROW_PYTHON_STATIC']
    pyarrow_public_args = ['-DARROW_PYTHON_STATIC']
else
    pyarrow_private_args = ['-DARROW_PYTHON_EXPORTING']
    pyarrow_public_args = []
endif

pyarrow_lib = library(
    'arrow_python',
    sources: pyarrow_srcs,
    include_directories: ['src'],
    dependencies: libpyarrow_deps + [
        numpy_dep,
        cython_generated_dep,
        py.dependency(),
    ],
    cpp_args: pyarrow_private_args,
    install: true,
    install_dir: py.get_install_dir() / 'pyarrow',
    gnu_symbol_visibility: gnu_symbol_visibility,
    override_options: ['b_lundef=false'],
)
pyarrow_lib_dep = declare_dependency(
    link_with: [pyarrow_lib],
    compile_args: pyarrow_public_args,
)

if needs_flight
    if get_option('default_library') == 'static'
        pyarrow_flight_private_args = ['-DARROW_PYFLIGHT_STATIC']
        pyarrow_flight_public_args = ['-DARROW_PYFLIGHT_STATIC']
    else
        pyarrow_flight_private_args = ['-DARROW_PYFLIGHT_EXPORTING']
        pyarrow_flight_public_args = []
    endif

    pyarrow_flight_lib = library(
        'arrow_flight_lib',
        sources: ['src/arrow/python/flight.cc'],
        dependencies: [arrow_flight_dep, pyarrow_lib_dep, py.dependency()],
        include_directories: ['src'],
        cpp_args: pyarrow_flight_private_args,
        install: true,
        install_dir: py.get_install_dir() / 'pyarrow',
        gnu_symbol_visibility: gnu_symbol_visibility,
        override_options: ['b_lundef=false'],
    )

    pyarrow_flight_dep = declare_dependency(
        link_with: [pyarrow_flight_lib],
        dependencies: [arrow_flight_dep],
        compile_args: pyarrow_flight_public_args,
    )
    cython_modules += {'_flight': {'dependencies': [pyarrow_flight_dep]}}
endif

if needs_parquet_encryption
    if get_option('default_library') == 'static'
        pyarrow_pq_enc_private_args = [
            '-DARROW_PYTHON_PARQUET_ENCRYPTION_STATIC',
        ]
        pyarrow_pq_enc_public_args = [
            '-DARROW_PYTHON_PARQUET_ENCRYPTION_STATIC',
        ]
    else
        pyarrow_pq_enc_private_args = [
            '-DARROW_PYTHON_PARQUET_ENCRYPTION_EXPORTING',
        ]
        pyarrow_pq_enc_public_args = []
    endif

    pyarrow_encryption_lib = library(
        'arrow_python_parquet_encryption',
        sources: ['src/arrow/python/parquet_encryption.cc'],
        include_directories: ['src'],
        dependencies: [parquet_dep, pyarrow_lib_dep, py.dependency()],
        cpp_args: pyarrow_pq_enc_private_args,
        install: true,
        install_dir: py.get_install_dir() / 'pyarrow',
        gnu_symbol_visibility: gnu_symbol_visibility,
        override_options: ['b_lundef=false'],
    )

    pyarrow_parquet_encryption_dep = declare_dependency(
        link_with: [pyarrow_encryption_lib],
        dependencies: [parquet_dep],
        compile_args: pyarrow_pq_enc_public_args,
    )

    cython_modules += {
        '_parquet_encryption': {
            'dependencies': [pyarrow_parquet_encryption_dep],
        },
    }

    if needs_dataset
        cython_modules += {
            '_dataset_parquet_encryption': {
                'dependencies': [
                    arrow_dataset_dep,
                    pyarrow_parquet_encryption_dep,
                ],
            },
        }
    endif
endif

foreach key, val : cython_modules
    cython_mod_name = '@0@.pyx'.format(key)
    py.extension_module(
        key,
        sources: [cython_mod_name],
        include_directories: ['src'],
        cython_args: cython_args,
        dependencies: [arrow_dep, pyarrow_lib_dep, numpy_dep] + val.get(
            'dependencies',
            [],
        ),
        override_options: ['cython_language=cpp'],
        install: true,
        subdir: 'pyarrow',
        install_rpath: '$ORIGIN',
        gnu_symbol_visibility: gnu_symbol_visibility,
    )

    install_data(cython_mod_name, install_dir: py.get_install_dir() / 'pyarrow')
endforeach

cython_headers = [
    '_acero.pxd',
    'array.pxi',
    'benchmark.pxi',
    'builder.pxi',
    'compat.pxi',
    '_compute.pxd',
    'config.pxi',
    '_csv.pxd',
    '_cuda.pxd',
    '_dataset_parquet.pxd',
    '_dataset.pxd',
    'device.pxi',
    '_dlpack.pxi',
    'error.pxi',
    '_fs.pxd',
    '__init__.pxd',
    'io.pxi',
    'ipc.pxi',
    '_json.pxd',
    'lib.pxd',
    'memory.pxi',
    '_orc.pxd',
    'pandas-shim.pxi',
    '_parquet_encryption.pxd',
    '_parquet.pxd',
    'public-api.pxi',
    '_pyarrow_cpp_tests.pxd',
    'scalar.pxi',
    'table.pxi',
    'tensor.pxi',
    'types.pxi',
]

install_data(cython_headers, install_dir: py.get_install_dir() / 'pyarrow')
install_subdir('includes', install_dir: py.get_install_dir() / 'pyarrow')
install_subdir(
    'src/arrow/python',
    install_dir: py.get_install_dir() / 'pyarrow/src/arrow',
)

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

py.install_sources(
    files(
        'src/arrow/python/api.h',
        'src/arrow/python/arrow_to_pandas.h',
        'src/arrow/python/async.h',
        'src/arrow/python/benchmark.h',
        'src/arrow/python/common.h',
        'src/arrow/python/config.h',
        'src/arrow/python/csv.h',
        'src/arrow/python/datetime.h',
        'src/arrow/python/decimal.h',
        'src/arrow/python/extension_type.h',
        'src/arrow/python/filesystem.h',
        'src/arrow/python/flight.h',
        'src/arrow/python/gdb.h',
        'src/arrow/python/helpers.h',
        'src/arrow/python/inference.h',
        'src/arrow/python/io.h',
        'src/arrow/python/ipc.h',
        'src/arrow/python/iterators.h',
        'src/arrow/python/numpy_convert.h',
        'src/arrow/python/numpy_init.h',
        'src/arrow/python/numpy_interop.h',
        'src/arrow/python/numpy_to_arrow.h',
        'src/arrow/python/parquet_encryption.h',
        'src/arrow/python/platform.h',
        'src/arrow/python/pyarrow.h',
        'src/arrow/python/pyarrow_lib.h',
        'src/arrow/python/python_test.h',
        'src/arrow/python/python_to_arrow.h',
        'src/arrow/python/type_traits.h',
        'src/arrow/python/udf.h',
        'src/arrow/python/util.h',
        'src/arrow/python/visibility.h',
    ),
    subdir: 'pyarrow/include/arrow/python',
)

if not get_option('sdist')
    arrow_header_dir = arrow_dep.get_variable(
        pkgconfig: 'includedir',
        cmake: 'PACKAGE_INCLUDE_DIRS',
    ) / 'arrow'

    install_subdir(
        arrow_header_dir,
        install_dir: py.get_install_dir() / 'pyarrow' / 'include',
    )
endif

py.install_sources(
    files('src/arrow/python/vendored/pythoncapi_compat.h'),
    subdir: 'pyarrow/include/arrow/python/vendored',
)

subdirs = ['interchange', 'parquet', 'tests', 'vendored']

foreach subdir : subdirs
    install_subdir(subdir, install_dir: py.get_install_dir() / 'pyarrow')
endforeach

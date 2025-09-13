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

import contextlib
import os
import os.path
from os.path import join as pjoin
import re
import shlex
import sys
import warnings

if sys.version_info >= (3, 10):
    import sysconfig
else:
    # Get correct EXT_SUFFIX on Windows (https://bugs.python.org/issue39825)
    from distutils import sysconfig

from setuptools import setup, Extension, Distribution

from Cython.Distutils import build_ext as _build_ext
import Cython

# Check if we're running 64-bit Python
is_64_bit = sys.maxsize > 2**32

# We can't use sys.platform in a cross-compiling situation
# as here it may be set to the host not target platform
is_emscripten = (
    sysconfig.get_config_var("SOABI")
    and sysconfig.get_config_var("SOABI").find("emscripten") != -1
)


if Cython.__version__ < '3.1':
    raise Exception(
        'Please update your Cython version. Supported Cython >= 3.1')

setup_dir = os.path.abspath(os.path.dirname(__file__))

ext_suffix = sysconfig.get_config_var('EXT_SUFFIX')


@contextlib.contextmanager
def changed_dir(dirname):
    oldcwd = os.getcwd()
    os.chdir(dirname)
    try:
        yield
    finally:
        os.chdir(oldcwd)


def strtobool(val):
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    # Copied from distutils
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))


MSG_DEPR_SETUP_BUILD_FLAGS = """
  !!

        ***********************************************************************
        The '{}' flag is being passed to setup.py, but this is
        deprecated.

        If a certain component is available in Arrow C++, it will automatically
        be enabled for the PyArrow build as well. If you want to force the
        build of a certain component, you can still use the
        PYARROW_WITH_$COMPONENT environment variable.
        ***********************************************************************

  !!
"""


class build_ext(_build_ext):
    _found_names = ()

    def build_extensions(self):
        import numpy
        numpy_incl = numpy.get_include()

        self.extensions = [ext for ext in self.extensions
                           if ext.name != '__dummy__']

        for ext in self.extensions:
            if (hasattr(ext, 'include_dirs') and
                    numpy_incl not in ext.include_dirs):
                ext.include_dirs.append(numpy_incl)
        _build_ext.build_extensions(self)

    def run(self):
        self._run_cmake()
        _build_ext.run(self)

    # adapted from cmake_build_ext in dynd-python
    # github.com/libdynd/dynd-python

    description = "Build the C-extensions for arrow"
    user_options = ([('cmake-generator=', None, 'CMake generator'),
                     ('extra-cmake-args=', None, 'extra arguments for CMake'),
                     ('build-type=', None,
                      'build type (debug or release), default release'),
                     ('boost-namespace=', None,
                      'namespace of boost (default: boost)'),
                     ('with-cuda', None, 'build the Cuda extension'),
                     ('with-flight', None, 'build the Flight extension'),
                     ('with-substrait', None, 'build the Substrait extension'),
                     ('with-acero', None, 'build the Acero Engine extension'),
                     ('with-dataset', None, 'build the Dataset extension'),
                     ('with-parquet', None, 'build the Parquet extension'),
                     ('with-parquet-encryption', None,
                      'build the Parquet encryption extension'),
                     ('with-azure', None,
                      'build the Azure Blob Storage extension'),
                     ('with-gcs', None,
                      'build the Google Cloud Storage (GCS) extension'),
                     ('with-s3', None, 'build the Amazon S3 extension'),
                     ('with-static-parquet', None, 'link parquet statically'),
                     ('with-static-boost', None, 'link boost statically'),
                     ('with-orc', None, 'build the ORC extension'),
                     ('with-gandiva', None, 'build the Gandiva extension'),
                     ('generate-coverage', None,
                      'enable Cython code coverage'),
                     ('bundle-boost', None,
                      'bundle the (shared) Boost libraries'),
                     ('bundle-cython-cpp', None,
                      'bundle generated Cython C++ code '
                      '(used for code coverage)'),
                     ('bundle-arrow-cpp', None,
                      'bundle the Arrow C++ libraries'),
                     ('bundle-arrow-cpp-headers', None,
                      'bundle the Arrow C++ headers')] +
                    _build_ext.user_options)

    def initialize_options(self):
        _build_ext.initialize_options(self)
        self.cmake_generator = os.environ.get('PYARROW_CMAKE_GENERATOR')
        if not self.cmake_generator and sys.platform == 'win32':
            self.cmake_generator = 'Visual Studio 15 2017 Win64'
        self.extra_cmake_args = os.environ.get('PYARROW_CMAKE_OPTIONS', '')
        self.build_type = os.environ.get('PYARROW_BUILD_TYPE',
                                         'release').lower()

        self.cmake_cxxflags = os.environ.get('PYARROW_CXXFLAGS', '')

        if sys.platform == 'win32':
            # Cannot do debug builds in Windows unless Python itself is a debug
            # build
            if not hasattr(sys, 'gettotalrefcount'):
                self.build_type = 'release'

        self.with_azure = None
        self.with_gcs = None
        self.with_s3 = None
        self.with_hdfs = None
        self.with_cuda = None
        self.with_substrait = None
        self.with_flight = None
        self.with_acero = None
        self.with_dataset = None
        self.with_parquet = None
        self.with_parquet_encryption = None
        self.with_orc = None
        self.with_gandiva = None

        self.generate_coverage = strtobool(
            os.environ.get('PYARROW_GENERATE_COVERAGE', '0'))
        self.bundle_arrow_cpp = strtobool(
            os.environ.get('PYARROW_BUNDLE_ARROW_CPP', '0'))
        self.bundle_cython_cpp = strtobool(
            os.environ.get('PYARROW_BUNDLE_CYTHON_CPP', '0'))

    CYTHON_MODULE_NAMES = [
        'lib',
        '_fs',
        '_csv',
        '_json',
        '_compute',
        '_cuda',
        '_flight',
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
        '_gcsfs',
        '_s3fs',
        '_substrait',
        '_hdfs',
        'gandiva']

    def _run_cmake(self):
        # check if build_type is correctly passed / set
        if self.build_type.lower() not in ('release', 'debug',
                                           'relwithdebinfo'):
            raise ValueError("--build-type (or PYARROW_BUILD_TYPE) needs to "
                             "be 'release', 'debug' or 'relwithdebinfo'")

        # The directory containing this setup.py
        source = os.path.dirname(os.path.abspath(__file__))

        # The staging directory for the module being built
        build_cmd = self.get_finalized_command('build')
        saved_cwd = os.getcwd()
        build_temp = pjoin(saved_cwd, build_cmd.build_temp)
        build_lib = pjoin(saved_cwd, build_cmd.build_lib)

        if not os.path.isdir(build_temp):
            self.mkpath(build_temp)

        if self.inplace:
            # a bit hacky
            build_lib = saved_cwd

        install_prefix = pjoin(build_lib, "pyarrow")

        # Change to the build directory
        with changed_dir(build_temp):
            # Detect if we built elsewhere
            if os.path.isfile('CMakeCache.txt'):
                cachefile = open('CMakeCache.txt', 'r')
                cachedir = re.search('CMAKE_CACHEFILE_DIR:INTERNAL=(.*)',
                                     cachefile.read()).group(1)
                cachefile.close()
                if (cachedir != build_temp):
                    build_base = pjoin(saved_cwd, build_cmd.build_base)
                    print(f"-- Skipping build. Temp build {build_temp} does "
                          f"not match cached dir {cachedir}")
                    print("---- For a clean build you might want to delete "
                          f"{build_base}.")
                    return

            cmake_options = [
                f'-DCMAKE_INSTALL_PREFIX={install_prefix}',
                f'-DPYTHON_EXECUTABLE={sys.executable}',
                f'-DPython3_EXECUTABLE={sys.executable}',
                f'-DPYARROW_CXXFLAGS={self.cmake_cxxflags}',
            ]

            def append_cmake_bool(value, varname):
                cmake_options.append(f'-D{varname}={"on" if value else "off"}')

            def append_cmake_component(flag, varname):
                # only pass this to cmake if the user pass the --with-component
                # flag to setup.py build_ext
                if flag is not None:
                    flag_name = (
                        "--with-"
                        + varname.removeprefix("PYARROW_").lower().replace("_", "-"))
                    warnings.warn(
                        MSG_DEPR_SETUP_BUILD_FLAGS.format(flag_name),
                        UserWarning, stacklevel=2
                    )
                    append_cmake_bool(flag, varname)

            if self.cmake_generator:
                cmake_options += ['-G', self.cmake_generator]

            append_cmake_component(self.with_cuda, 'PYARROW_CUDA')
            append_cmake_component(self.with_substrait, 'PYARROW_SUBSTRAIT')
            append_cmake_component(self.with_flight, 'PYARROW_FLIGHT')
            append_cmake_component(self.with_gandiva, 'PYARROW_GANDIVA')
            append_cmake_component(self.with_acero, 'PYARROW_ACERO')
            append_cmake_component(self.with_dataset, 'PYARROW_DATASET')
            append_cmake_component(self.with_orc, 'PYARROW_ORC')
            append_cmake_component(self.with_parquet, 'PYARROW_PARQUET')
            append_cmake_component(self.with_parquet_encryption,
                                   'PYARROW_PARQUET_ENCRYPTION')
            append_cmake_component(self.with_azure, 'PYARROW_AZURE')
            append_cmake_component(self.with_gcs, 'PYARROW_GCS')
            append_cmake_component(self.with_s3, 'PYARROW_S3')
            append_cmake_component(self.with_hdfs, 'PYARROW_HDFS')

            append_cmake_bool(self.bundle_arrow_cpp,
                              'PYARROW_BUNDLE_ARROW_CPP')
            append_cmake_bool(self.bundle_cython_cpp,
                              'PYARROW_BUNDLE_CYTHON_CPP')
            append_cmake_bool(self.generate_coverage,
                              'PYARROW_GENERATE_COVERAGE')

            cmake_options.append(
                f'-DCMAKE_BUILD_TYPE={self.build_type.lower()}')

            extra_cmake_args = shlex.split(self.extra_cmake_args)

            build_tool_args = []
            if sys.platform == 'win32':
                if not is_64_bit:
                    raise RuntimeError('Not supported on 32-bit Windows')
            else:
                build_tool_args.append('--')
                if os.environ.get('PYARROW_BUILD_VERBOSE', '0') == '1':
                    cmake_options.append('-DCMAKE_VERBOSE_MAKEFILE=ON')
                parallel = os.environ.get('PYARROW_PARALLEL')
                if parallel:
                    build_tool_args.append(f'-j{parallel}')

            # Generate the build files
            if is_emscripten:
                print("-- Running emcmake cmake for PyArrow on Emscripten")
                self.spawn(['emcmake', 'cmake'] + extra_cmake_args +
                           cmake_options + [source])
            else:
                print("-- Running cmake for PyArrow")
                self.spawn(['cmake'] + extra_cmake_args + cmake_options + [source])

            print("-- Finished cmake for PyArrow")

            print("-- Running cmake --build for PyArrow")
            self.spawn(['cmake', '--build', '.', '--config', self.build_type] +
                       build_tool_args)
            print("-- Finished cmake --build for PyArrow")

            print("-- Running cmake --build --target install for PyArrow")
            self.spawn(['cmake', '--build', '.', '--config', self.build_type] +
                       ['--target', 'install'] + build_tool_args)
            print("-- Finished cmake --build --target install for PyArrow")

            self._found_names = []
            for name in self.CYTHON_MODULE_NAMES:
                built_path = pjoin(install_prefix, name + ext_suffix)
                if os.path.exists(built_path):
                    self._found_names.append(name)

    def _get_build_dir(self):
        # Get the package directory from build_py
        build_py = self.get_finalized_command('build_py')
        return build_py.get_package_dir('pyarrow')

    def _get_cmake_ext_path(self, name):
        # This is the name of the arrow C-extension
        filename = name + ext_suffix
        return pjoin(self._get_build_dir(), filename)

    def get_ext_generated_cpp_source(self, name):
        if sys.platform == 'win32':
            head, tail = os.path.split(name)
            return pjoin(head, tail + ".cpp")
        else:
            return pjoin(name + ".cpp")

    def get_ext_built_api_header(self, name):
        if sys.platform == 'win32':
            head, tail = os.path.split(name)
            return pjoin(head, tail + "_api.h")
        else:
            return pjoin(name + "_api.h")

    def get_names(self):
        return self._found_names

    def get_outputs(self):
        # Just the C extensions
        # regular_exts = _build_ext.get_outputs(self)
        return [self._get_cmake_ext_path(name)
                for name in self.get_names()]


class BinaryDistribution(Distribution):
    def has_ext_modules(foo):
        return True


setup(
    distclass=BinaryDistribution,
    # Dummy extension to trigger build_ext
    ext_modules=[Extension('__dummy__', sources=[])],
    cmdclass={
        'build_ext': build_ext
    },
)

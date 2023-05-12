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

if sys.version_info >= (3, 10):
    import sysconfig
else:
    # Get correct EXT_SUFFIX on Windows (https://bugs.python.org/issue39825)
    from distutils import sysconfig

import pkg_resources
from setuptools import setup, Extension, Distribution, find_namespace_packages

from Cython.Distutils import build_ext as _build_ext
import Cython

# Check if we're running 64-bit Python
is_64_bit = sys.maxsize > 2**32

if Cython.__version__ < '0.29.31':
    raise Exception('Please upgrade to Cython 0.29.31 or newer')

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


class build_ext(_build_ext):
    _found_names = ()

    def build_extensions(self):
        numpy_incl = pkg_resources.resource_filename('numpy', 'core/include')

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

        self.with_gcs = strtobool(
            os.environ.get('PYARROW_WITH_GCS', '0'))
        self.with_s3 = strtobool(
            os.environ.get('PYARROW_WITH_S3', '0'))
        self.with_hdfs = strtobool(
            os.environ.get('PYARROW_WITH_HDFS', '0'))
        self.with_cuda = strtobool(
            os.environ.get('PYARROW_WITH_CUDA', '0'))
        self.with_substrait = strtobool(
            os.environ.get('PYARROW_WITH_SUBSTRAIT', '0'))
        self.with_flight = strtobool(
            os.environ.get('PYARROW_WITH_FLIGHT', '0'))
        self.with_acero = strtobool(
            os.environ.get('PYARROW_WITH_ACERO', '0'))
        self.with_dataset = strtobool(
            os.environ.get('PYARROW_WITH_DATASET', '0'))
        self.with_parquet = strtobool(
            os.environ.get('PYARROW_WITH_PARQUET', '0'))
        self.with_parquet_encryption = strtobool(
            os.environ.get('PYARROW_WITH_PARQUET_ENCRYPTION', '0'))
        self.with_orc = strtobool(
            os.environ.get('PYARROW_WITH_ORC', '0'))
        self.with_gandiva = strtobool(
            os.environ.get('PYARROW_WITH_GANDIVA', '0'))
        self.generate_coverage = strtobool(
            os.environ.get('PYARROW_GENERATE_COVERAGE', '0'))
        self.bundle_arrow_cpp = strtobool(
            os.environ.get('PYARROW_BUNDLE_ARROW_CPP', '0'))
        self.bundle_cython_cpp = strtobool(
            os.environ.get('PYARROW_BUNDLE_CYTHON_CPP', '0'))

        self.with_parquet_encryption = (self.with_parquet_encryption and
                                        self.with_parquet)

        # enforce module dependencies
        if self.with_substrait:
            self.with_dataset = True
        if self.with_dataset:
            self.with_acero = True

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
        '_gcsfs',
        '_s3fs',
        '_substrait',
        '_hdfs',
        '_hdfsio',
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
                cmake_options.append('-D{0}={1}'.format(
                    varname, 'on' if value else 'off'))

            if self.cmake_generator:
                cmake_options += ['-G', self.cmake_generator]

            append_cmake_bool(self.with_cuda, 'PYARROW_BUILD_CUDA')
            append_cmake_bool(self.with_substrait, 'PYARROW_BUILD_SUBSTRAIT')
            append_cmake_bool(self.with_flight, 'PYARROW_BUILD_FLIGHT')
            append_cmake_bool(self.with_gandiva, 'PYARROW_BUILD_GANDIVA')
            append_cmake_bool(self.with_acero, 'PYARROW_BUILD_ACERO')
            append_cmake_bool(self.with_dataset, 'PYARROW_BUILD_DATASET')
            append_cmake_bool(self.with_orc, 'PYARROW_BUILD_ORC')
            append_cmake_bool(self.with_parquet, 'PYARROW_BUILD_PARQUET')
            append_cmake_bool(self.with_parquet_encryption,
                              'PYARROW_BUILD_PARQUET_ENCRYPTION')
            append_cmake_bool(self.with_gcs, 'PYARROW_BUILD_GCS')
            append_cmake_bool(self.with_s3, 'PYARROW_BUILD_S3')
            append_cmake_bool(self.with_hdfs, 'PYARROW_BUILD_HDFS')
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
                if not os.path.exists(built_path):
                    print(f'Did not find {built_path}')
                    if self._failure_permitted(name):
                        print(f'Cython module {name} failure permitted')
                        continue
                    raise RuntimeError('PyArrow C-extension failed to build:',
                                       os.path.abspath(built_path))

                self._found_names.append(name)

    def _failure_permitted(self, name):
        if name == '_parquet' and not self.with_parquet:
            return True
        if name == '_parquet_encryption' and not self.with_parquet_encryption:
            return True
        if name == '_orc' and not self.with_orc:
            return True
        if name == '_flight' and not self.with_flight:
            return True
        if name == '_substrait' and not self.with_substrait:
            return True
        if name == '_gcsfs' and not self.with_gcs:
            return True
        if name == '_s3fs' and not self.with_s3:
            return True
        if name == '_hdfs' and not self.with_hdfs:
            return True
        if name == '_dataset' and not self.with_dataset:
            return True
        if name == '_acero' and not self.with_acero:
            return True
        if name == '_exec_plan' and not self.with_acero:
            return True
        if name == '_dataset_orc' and not (
                self.with_orc and self.with_dataset
        ):
            return True
        if name == '_dataset_parquet' and not (
                self.with_parquet and self.with_dataset
        ):
            return True
        if name == '_cuda' and not self.with_cuda:
            return True
        if name == 'gandiva' and not self.with_gandiva:
            return True
        return False

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


# If the event of not running from a git clone (e.g. from a git archive
# or a Python sdist), see if we can set the version number ourselves
default_version = '13.0.0-SNAPSHOT'
if (not os.path.exists('../.git') and
        not os.environ.get('SETUPTOOLS_SCM_PRETEND_VERSION')):
    os.environ['SETUPTOOLS_SCM_PRETEND_VERSION'] = \
        default_version.replace('-SNAPSHOT', 'a0')


# See https://github.com/pypa/setuptools_scm#configuration-parameters
scm_version_write_to_prefix = os.environ.get(
    'SETUPTOOLS_SCM_VERSION_WRITE_TO_PREFIX', setup_dir)


def parse_git(root, **kwargs):
    """
    Parse function for setuptools_scm that ignores tags for non-C++
    subprojects, e.g. apache-arrow-js-XXX tags.
    """
    from setuptools_scm.git import parse
    kwargs['describe_command'] =\
        'git describe --dirty --tags --long --match "apache-arrow-[0-9]*.*"'
    return parse(root, **kwargs)


def guess_next_dev_version(version):
    if version.exact:
        return version.format_with('{tag}')
    else:
        def guess_next_version(tag_version):
            return default_version.replace('-SNAPSHOT', '')
        return version.format_next_version(guess_next_version)


with open('README.md') as f:
    long_description = f.read()


class BinaryDistribution(Distribution):
    def has_ext_modules(foo):
        return True


install_requires = (
    'numpy >= 1.16.6',
)


# Only include pytest-runner in setup_requires if we're invoking tests
if {'pytest', 'test', 'ptr'}.intersection(sys.argv):
    setup_requires = ['pytest-runner']
else:
    setup_requires = []


if strtobool(os.environ.get('PYARROW_INSTALL_TESTS', '1')):
    packages = find_namespace_packages(include=['pyarrow*'])
    exclude_package_data = {}
else:
    packages = find_namespace_packages(include=['pyarrow*'],
                                       exclude=["pyarrow.tests*"])
    # setuptools adds back importable packages even when excluded.
    # https://github.com/pypa/setuptools/issues/3260
    # https://github.com/pypa/setuptools/issues/3340#issuecomment-1219383976
    exclude_package_data = {"pyarrow": ["tests*"]}


setup(
    name='pyarrow',
    packages=packages,
    zip_safe=False,
    package_data={'pyarrow': ['*.pxd', '*.pyx', 'includes/*.pxd']},
    include_package_data=True,
    exclude_package_data=exclude_package_data,
    distclass=BinaryDistribution,
    # Dummy extension to trigger build_ext
    ext_modules=[Extension('__dummy__', sources=[])],
    cmdclass={
        'build_ext': build_ext
    },
    use_scm_version={
        'root': os.path.dirname(setup_dir),
        'parse': parse_git,
        'write_to': os.path.join(scm_version_write_to_prefix,
                                 'pyarrow/_generated_version.py'),
        'version_scheme': guess_next_dev_version
    },
    setup_requires=['setuptools_scm', 'cython >= 0.29.31'] + setup_requires,
    install_requires=install_requires,
    tests_require=['pytest', 'pandas', 'hypothesis'],
    python_requires='>=3.7',
    description='Python library for Apache Arrow',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    license='Apache License, Version 2.0',
    maintainer='Apache Arrow Developers',
    maintainer_email='dev@arrow.apache.org',
    test_suite='pyarrow.tests',
    url='https://arrow.apache.org/',
    project_urls={
        'Documentation': 'https://arrow.apache.org/docs/python',
        'Source': 'https://github.com/apache/arrow',
    },
)

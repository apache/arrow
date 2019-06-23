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

from distutils.command.clean import clean as _clean
from distutils.util import strtobool
from distutils import sysconfig

# Check if we're running 64-bit Python
is_64_bit = sys.maxsize > 2**32

if Cython.__version__ < '0.29':
    raise Exception('Please upgrade to Cython 0.29 or newer')

setup_dir = os.path.abspath(os.path.dirname(__file__))


ext_suffix = sysconfig.get_config_var('EXT_SUFFIX')
if ext_suffix is None:
    # https://bugs.python.org/issue19555
    ext_suffix = sysconfig.get_config_var('SO')


@contextlib.contextmanager
def changed_dir(dirname):
    oldcwd = os.getcwd()
    os.chdir(dirname)
    try:
        yield
    finally:
        os.chdir(oldcwd)


class clean(_clean):

    def run(self):
        _clean.run(self)
        for x in []:
            try:
                os.remove(x)
            except OSError:
                pass


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
                     ('with-parquet', None, 'build the Parquet extension'),
                     ('with-static-parquet', None, 'link parquet statically'),
                     ('with-static-boost', None, 'link boost statically'),
                     ('with-plasma', None, 'build the Plasma extension'),
                     ('with-tensorflow', None,
                      'build pyarrow with TensorFlow support'),
                     ('with-orc', None, 'build the ORC extension'),
                     ('with-gandiva', None, 'build the Gandiva extension'),
                     ('generate-coverage', None,
                      'enable Cython code coverage'),
                     ('bundle-boost', None,
                      'bundle the (shared) Boost libraries'),
                     ('bundle-arrow-cpp', None,
                      'bundle the Arrow C++ libraries')] +
                    _build_ext.user_options)

    def initialize_options(self):
        _build_ext.initialize_options(self)
        self.cmake_generator = os.environ.get('PYARROW_CMAKE_GENERATOR')
        if not self.cmake_generator and sys.platform == 'win32':
            self.cmake_generator = 'Visual Studio 14 2015 Win64'
        self.extra_cmake_args = os.environ.get('PYARROW_CMAKE_OPTIONS', '')
        self.build_type = os.environ.get('PYARROW_BUILD_TYPE',
                                         'release').lower()
        self.boost_namespace = os.environ.get('PYARROW_BOOST_NAMESPACE',
                                              'boost')

        self.cmake_cxxflags = os.environ.get('PYARROW_CXXFLAGS', '')

        if sys.platform == 'win32':
            # Cannot do debug builds in Windows unless Python itself is a debug
            # build
            if not hasattr(sys, 'gettotalrefcount'):
                self.build_type = 'release'

        self.with_cuda = strtobool(
            os.environ.get('PYARROW_WITH_CUDA', '0'))
        self.with_flight = strtobool(
            os.environ.get('PYARROW_WITH_FLIGHT', '0'))
        self.with_parquet = strtobool(
            os.environ.get('PYARROW_WITH_PARQUET', '0'))
        self.with_static_parquet = strtobool(
            os.environ.get('PYARROW_WITH_STATIC_PARQUET', '0'))
        self.with_static_boost = strtobool(
            os.environ.get('PYARROW_WITH_STATIC_BOOST', '0'))
        self.with_plasma = strtobool(
            os.environ.get('PYARROW_WITH_PLASMA', '0'))
        self.with_tensorflow = strtobool(
            os.environ.get('PYARROW_WITH_TENSORFLOW', '0'))
        self.with_orc = strtobool(
            os.environ.get('PYARROW_WITH_ORC', '0'))
        self.with_gandiva = strtobool(
            os.environ.get('PYARROW_WITH_GANDIVA', '0'))
        self.generate_coverage = strtobool(
            os.environ.get('PYARROW_GENERATE_COVERAGE', '0'))
        self.bundle_arrow_cpp = strtobool(
            os.environ.get('PYARROW_BUNDLE_ARROW_CPP', '0'))
        self.bundle_boost = strtobool(
            os.environ.get('PYARROW_BUNDLE_BOOST', '0'))

    CYTHON_MODULE_NAMES = [
        'lib',
        '_csv',
        '_json',
        '_cuda',
        '_flight',
        '_parquet',
        '_orc',
        '_plasma',
        'gandiva']

    def _run_cmake(self):
        # check if build_type is correctly passed / set
        if self.build_type.lower() not in ('release', 'debug'):
            raise ValueError("--build-type (or PYARROW_BUILD_TYPE) needs to "
                             "be 'release' or 'debug'")

        # The directory containing this setup.py
        source = osp.dirname(osp.abspath(__file__))

        # The staging directory for the module being built
        build_temp = pjoin(os.getcwd(), self.build_temp)
        build_lib = os.path.join(os.getcwd(), self.build_lib)
        saved_cwd = os.getcwd()

        if not os.path.isdir(self.build_temp):
            self.mkpath(self.build_temp)

        # Change to the build directory
        with changed_dir(self.build_temp):
            # Detect if we built elsewhere
            if os.path.isfile('CMakeCache.txt'):
                cachefile = open('CMakeCache.txt', 'r')
                cachedir = re.search('CMAKE_CACHEFILE_DIR:INTERNAL=(.*)',
                                     cachefile.read()).group(1)
                cachefile.close()
                if (cachedir != build_temp):
                    return

            static_lib_option = ''

            cmake_options = [
                '-DPYTHON_EXECUTABLE=%s' % sys.executable,
                static_lib_option,
            ]

            if self.cmake_generator:
                cmake_options += ['-G', self.cmake_generator]
            if self.with_cuda:
                cmake_options.append('-DPYARROW_BUILD_CUDA=on')
            if self.with_flight:
                cmake_options.append('-DPYARROW_BUILD_FLIGHT=on')
            if self.with_parquet:
                cmake_options.append('-DPYARROW_BUILD_PARQUET=on')
            if self.with_static_parquet:
                cmake_options.append('-DPYARROW_PARQUET_USE_SHARED=off')
            if not self.with_static_boost:
                cmake_options.append('-DPYARROW_BOOST_USE_SHARED=on')
            else:
                cmake_options.append('-DPYARROW_BOOST_USE_SHARED=off')

            if self.with_plasma:
                cmake_options.append('-DPYARROW_BUILD_PLASMA=on')

            if self.with_tensorflow:
                cmake_options.append('-DPYARROW_USE_TENSORFLOW=on')

            if self.with_orc:
                cmake_options.append('-DPYARROW_BUILD_ORC=on')

            if self.with_gandiva:
                cmake_options.append('-DPYARROW_BUILD_GANDIVA=on')

            if len(self.cmake_cxxflags) > 0:
                cmake_options.append('-DPYARROW_CXXFLAGS={0}'
                                     .format(self.cmake_cxxflags))

            if self.generate_coverage:
                cmake_options.append('-DPYARROW_GENERATE_COVERAGE=on')

            if self.bundle_arrow_cpp:
                cmake_options.append('-DPYARROW_BUNDLE_ARROW_CPP=ON')
                # ARROW-1090: work around CMake rough edges
                if 'ARROW_HOME' in os.environ and sys.platform != 'win32':
                    pkg_config = pjoin(os.environ['ARROW_HOME'], 'lib',
                                       'pkgconfig')
                    os.environ['PKG_CONFIG_PATH'] = pkg_config
                    del os.environ['ARROW_HOME']

            if self.bundle_boost:
                cmake_options.append('-DPYARROW_BUNDLE_BOOST=ON')

            cmake_options.append('-DCMAKE_BUILD_TYPE={0}'
                                 .format(self.build_type.lower()))

            if self.boost_namespace != 'boost':
                cmake_options.append('-DBoost_NAMESPACE={}'
                                     .format(self.boost_namespace))

            extra_cmake_args = shlex.split(self.extra_cmake_args)

            build_tool_args = []
            if sys.platform == 'win32':
                if not is_64_bit:
                    raise RuntimeError('Not supported on 32-bit Windows')
            else:
                build_tool_args.append('--')
                if os.environ.get('PYARROW_BUILD_VERBOSE', '0') == '1':
                    cmake_options.append('-DCMAKE_VERBOSE_MAKEFILE=ON')
                if os.environ.get('PYARROW_PARALLEL'):
                    build_tool_args.append(
                        '-j{0}'.format(os.environ['PYARROW_PARALLEL']))

            # Generate the build files
            print("-- Running cmake for pyarrow")
            self.spawn(['cmake'] + extra_cmake_args + cmake_options + [source])
            print("-- Finished cmake for pyarrow")

            # Do the build
            print("-- Running cmake --build for pyarrow")
            self.spawn(['cmake', '--build', '.', '--config', self.build_type]
                       + build_tool_args)
            print("-- Finished cmake --build for pyarrow")

            if self.inplace:
                # a bit hacky
                build_lib = saved_cwd

            # Move the libraries to the place expected by the Python build
            try:
                os.makedirs(pjoin(build_lib, 'pyarrow'))
            except OSError:
                pass

            if sys.platform == 'win32':
                build_prefix = ''
            else:
                build_prefix = self.build_type

            print('Bundling includes: ' + pjoin(build_prefix, 'include'))
            if os.path.exists(pjoin(build_lib, 'pyarrow', 'include')):
                shutil.rmtree(pjoin(build_lib, 'pyarrow', 'include'))
            shutil.move(pjoin(build_prefix, 'include'),
                        pjoin(build_lib, 'pyarrow'))

            # Move the built C-extension to the place expected by the Python
            # build
            self._found_names = []
            for name in self.CYTHON_MODULE_NAMES:
                built_path = self.get_ext_built(name)
                if not os.path.exists(built_path):
                    print('Did not find {0}'.format(built_path))
                    if self._failure_permitted(name):
                        print('Cython module {0} failure permitted'
                              .format(name))
                        continue
                    raise RuntimeError('pyarrow C-extension failed to build:',
                                       os.path.abspath(built_path))

                cpp_generated_path = self.get_ext_generated_cpp_source(name)
                if not os.path.exists(cpp_generated_path):
                    raise RuntimeError('expected to find generated C++ file '
                                       'in {0!r}'.format(cpp_generated_path))

                # The destination path to move the generated C++ source to
                # (for Cython source coverage)
                cpp_path = pjoin(build_lib, self._get_build_dir(),
                                 os.path.basename(cpp_generated_path))
                if os.path.exists(cpp_path):
                    os.remove(cpp_path)

                # The destination path to move the built C extension to
                ext_path = pjoin(build_lib, self._get_cmake_ext_path(name))
                if os.path.exists(ext_path):
                    os.remove(ext_path)
                self.mkpath(os.path.dirname(ext_path))

                print('Moving generated C++ source', cpp_generated_path,
                      'to build path', cpp_path)
                shutil.move(cpp_generated_path, cpp_path)
                print('Moving built C-extension', built_path,
                      'to build path', ext_path)
                shutil.move(built_path, ext_path)
                self._found_names.append(name)

                if os.path.exists(self.get_ext_built_api_header(name)):
                    shutil.move(self.get_ext_built_api_header(name),
                                pjoin(os.path.dirname(ext_path),
                                      name + '_api.h'))

            if self.bundle_arrow_cpp:
                print(pjoin(build_lib, 'pyarrow'))
                move_shared_libs(build_prefix, build_lib, "arrow")
                move_shared_libs(build_prefix, build_lib, "arrow_python")
                if self.with_cuda:
                    move_shared_libs(build_prefix, build_lib, "arrow_cuda")
                if self.with_flight:
                    move_shared_libs(build_prefix, build_lib, "arrow_flight")
                if self.with_plasma:
                    move_shared_libs(build_prefix, build_lib, "plasma")
                if self.with_gandiva:
                    move_shared_libs(build_prefix, build_lib, "gandiva")
                if self.with_parquet and not self.with_static_parquet:
                    move_shared_libs(build_prefix, build_lib, "parquet")
                if not self.with_static_boost and self.bundle_boost:
                    move_shared_libs(
                        build_prefix, build_lib,
                        "{}_filesystem".format(self.boost_namespace),
                        implib_required=False)
                    move_shared_libs(
                        build_prefix, build_lib,
                        "{}_system".format(self.boost_namespace),
                        implib_required=False)
                    move_shared_libs(
                        build_prefix, build_lib,
                        "{}_regex".format(self.boost_namespace),
                        implib_required=False)
                if sys.platform == 'win32':
                    # zlib uses zlib.dll for Windows
                    zlib_lib_name = 'zlib'
                    move_shared_libs(build_prefix, build_lib, zlib_lib_name,
                                     implib_required=False)
                    if self.with_flight:
                        # DLL dependencies for gRPC / Flight
                        for lib_name in ['cares', 'libprotobuf',
                                         'libcrypto-1_1-x64',
                                         'libssl-1_1-x64']:
                            move_shared_libs(build_prefix, build_lib, lib_name,
                                             implib_required=False)

            if self.with_plasma:
                # Move the plasma store
                source = os.path.join(self.build_type, "plasma_store_server")
                target = os.path.join(build_lib,
                                      self._get_build_dir(),
                                      "plasma_store_server")
                shutil.move(source, target)

    def _failure_permitted(self, name):
        if name == '_parquet' and not self.with_parquet:
            return True
        if name == '_plasma' and not self.with_plasma:
            return True
        if name == '_orc' and not self.with_orc:
            return True
        if name == '_flight' and not self.with_flight:
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

    def get_ext_built(self, name):
        if sys.platform == 'win32':
            head, tail = os.path.split(name)
            # Visual Studio seems to differ from other generators in
            # where it places output files.
            if self.cmake_generator.startswith('Visual Studio'):
                return pjoin(head, self.build_type, tail + ext_suffix)
            else:
                return pjoin(head, tail + ext_suffix)
        else:
            return pjoin(self.build_type, name + ext_suffix)

    def get_names(self):
        return self._found_names

    def get_outputs(self):
        # Just the C extensions
        # regular_exts = _build_ext.get_outputs(self)
        return [self._get_cmake_ext_path(name)
                for name in self.get_names()]


def move_shared_libs(build_prefix, build_lib, lib_name,
                     implib_required=True):
    if sys.platform == 'win32':
        # Move all .dll and .lib files
        libs = [lib_name + '.dll']
        if implib_required:
            libs.append(lib_name + '.lib')
        for filename in libs:
            shutil.move(pjoin(build_prefix, filename),
                        pjoin(build_lib, 'pyarrow', filename))
    else:
        _move_shared_libs_unix(build_prefix, build_lib, lib_name)


def _move_shared_libs_unix(build_prefix, build_lib, lib_name):
    shared_library_prefix = 'lib'
    if sys.platform == 'darwin':
        shared_library_suffix = '.dylib'
    else:
        shared_library_suffix = '.so'

    lib_filename = (shared_library_prefix + lib_name +
                    shared_library_suffix)
    # Also copy libraries with ABI/SO version suffix
    if sys.platform == 'darwin':
        lib_pattern = (shared_library_prefix + lib_name +
                       ".*" + shared_library_suffix[1:])
        libs = glob.glob(pjoin(build_prefix, lib_pattern))
    else:
        libs = glob.glob(pjoin(build_prefix, lib_filename) + '*')

    if not libs:
        raise Exception('Could not find library:' + lib_filename +
                        ' in ' + build_prefix)

    # Longest suffix library should be copied, all others symlinked
    libs.sort(key=lambda s: -len(s))
    print(libs, libs[0])
    lib_filename = os.path.basename(libs[0])
    shutil.move(pjoin(build_prefix, lib_filename),
                pjoin(build_lib, 'pyarrow', lib_filename))


# If the event of not running from a git clone (e.g. from a git archive
# or a Python sdist), see if we can set the version number ourselves
default_version = '0.14.0-SNAPSHOT'
if (not os.path.exists('../.git/index')
        and not os.environ.get('SETUPTOOLS_SCM_PRETEND_VERSION')):
    if os.path.exists('PKG-INFO'):
        # We're probably in a Python sdist, setuptools_scm will handle fine
        pass
    else:
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
        'git describe --dirty --tags --long --match "apache-arrow-[0-9].*"'
    return parse(root, **kwargs)


with open('README.md') as f:
    long_description = f.read()


class BinaryDistribution(Distribution):
    def has_ext_modules(foo):
        return True


install_requires = (
    'numpy >= 1.14',
    'six >= 1.0.0',
    'futures; python_version < "3.2"',
    'enum34 >= 1.1.6; python_version < "3.4"',
)


# Only include pytest-runner in setup_requires if we're invoking tests
if {'pytest', 'test', 'ptr'}.intersection(sys.argv):
    setup_requires = ['pytest-runner']
else:
    setup_requires = []


setup(
    name='pyarrow',
    packages=['pyarrow', 'pyarrow.tests'],
    zip_safe=False,
    package_data={'pyarrow': ['*.pxd', '*.pyx', 'includes/*.pxd']},
    include_package_data=True,
    distclass=BinaryDistribution,
    # Dummy extension to trigger build_ext
    ext_modules=[Extension('__dummy__', sources=[])],
    cmdclass={
        'clean': clean,
        'build_ext': build_ext
    },
    entry_points={
        'console_scripts': [
            'plasma_store = pyarrow:_plasma_store_entry_point'
        ]
    },
    use_scm_version={
        'root': os.path.dirname(setup_dir),
        'parse': parse_git,
        'write_to': os.path.join(scm_version_write_to_prefix,
                                 'pyarrow/_generated_version.py')
    },
    setup_requires=['setuptools_scm', 'cython >= 0.29'] + setup_requires,
    install_requires=install_requires,
    tests_require=['pytest', 'pandas', 'hypothesis',
                   'pathlib2; python_version < "3.4"'],
    description='Python library for Apache Arrow',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    license='Apache License, Version 2.0',
    maintainer='Apache Arrow Developers',
    maintainer_email='dev@arrow.apache.org',
    test_suite='pyarrow.tests',
    url='https://arrow.apache.org/'
)

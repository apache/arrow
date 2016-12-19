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

import glob
import os.path as osp
import re
import shutil
from Cython.Distutils import build_ext as _build_ext
import Cython

import sys

import pkg_resources
from setuptools import setup, Extension

import os

from os.path import join as pjoin

from distutils.command.clean import clean as _clean
from distutils import sysconfig

# Check if we're running 64-bit Python
is_64_bit = sys.maxsize > 2**32

if Cython.__version__ < '0.19.1':
    raise Exception('Please upgrade to Cython 0.19.1 or newer')

VERSION = '0.1.0'
ISRELEASED = False

if not ISRELEASED:
    VERSION += '.dev'

setup_dir = os.path.abspath(os.path.dirname(__file__))


def write_version_py(filename=os.path.join(setup_dir, 'pyarrow/version.py')):
    a = open(filename, 'w')
    file_content = "\n".join(["",
                              "# THIS FILE IS GENERATED FROM SETUP.PY",
                              "version = '%(version)s'",
                              "isrelease = '%(isrelease)s'"])

    a.write(file_content % {'version': VERSION,
                            'isrelease': str(ISRELEASED)})
    a.close()


class clean(_clean):

    def run(self):
        _clean.run(self)
        for x in []:
            try:
                os.remove(x)
            except OSError:
                pass


class build_ext(_build_ext):

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
    user_options = ([('extra-cmake-args=', None, 'extra arguments for CMake'),
                     ('build-type=', None, 'build type (debug or release)'),
                     ('with-parquet', None, 'build the Parquet extension')] +
                    _build_ext.user_options)

    def initialize_options(self):
        _build_ext.initialize_options(self)
        self.extra_cmake_args = os.environ.get('PYARROW_CMAKE_OPTIONS', '')
        self.build_type = os.environ.get('PYARROW_BUILD_TYPE', 'debug').lower()
        self.with_parquet = False

    CYTHON_MODULE_NAMES = [
        'array',
        'config',
        'error',
        'io',
        'ipc',
        'parquet',
        'scalar',
        'schema',
        'table']

    def _run_cmake(self):
        # The directory containing this setup.py
        source = osp.dirname(osp.abspath(__file__))

        # The staging directory for the module being built
        build_temp = pjoin(os.getcwd(), self.build_temp)
        build_lib = os.path.join(os.getcwd(), self.build_lib)

        # Change to the build directory
        saved_cwd = os.getcwd()
        if not os.path.isdir(self.build_temp):
            self.mkpath(self.build_temp)
        os.chdir(self.build_temp)

        # Detect if we built elsewhere
        if os.path.isfile('CMakeCache.txt'):
            cachefile = open('CMakeCache.txt', 'r')
            cachedir = re.search('CMAKE_CACHEFILE_DIR:INTERNAL=(.*)',
                                 cachefile.read()).group(1)
            cachefile.close()
            if (cachedir != build_temp):
                return

        static_lib_option = ''
        build_tests_option = ''

        cmake_options = [
            '-DPYTHON_EXECUTABLE=%s' % sys.executable,
            static_lib_option,
            build_tests_option,
        ]

        if self.with_parquet:
            cmake_options.append('-DPYARROW_BUILD_PARQUET=on')

        if sys.platform != 'win32':
            cmake_options.append('-DCMAKE_BUILD_TYPE={0}'
                                 .format(self.build_type))

            cmake_command = (['cmake', self.extra_cmake_args] +
                             cmake_options + [source])

            self.spawn(cmake_command)
            args = ['make', 'VERBOSE=1']
            if 'PYARROW_PARALLEL' in os.environ:
                args.append('-j{0}'.format(os.environ['PYARROW_PARALLEL']))
            self.spawn(args)
        else:
            import shlex
            cmake_generator = 'Visual Studio 14 2015'
            if is_64_bit:
                cmake_generator += ' Win64'
            # Generate the build files
            extra_cmake_args = shlex.split(self.extra_cmake_args)
            cmake_command = (['cmake'] + extra_cmake_args +
                             cmake_options +
                             [source,
                             '-G', cmake_generator])
            if "-G" in self.extra_cmake_args:
                cmake_command = cmake_command[:-2]

            self.spawn(cmake_command)
            # Do the build
            self.spawn(['cmake', '--build', '.', '--config', self.build_type])

        if self.inplace:
            # a bit hacky
            build_lib = saved_cwd

        # Move the built libpyarrow library to the place expected by the Python
        # build
        if sys.platform != 'win32':
            name, = glob.glob(pjoin(self.build_type, 'libpyarrow.*'))
            try:
                os.makedirs(pjoin(build_lib, 'pyarrow'))
            except OSError:
                pass
            shutil.move(name,
                        pjoin(build_lib, 'pyarrow', os.path.split(name)[1]))
        else:
            shutil.move(pjoin(self.build_type, 'pyarrow.dll'),
                        pjoin(build_lib, 'pyarrow', 'pyarrow.dll'))

        # Move the built C-extension to the place expected by the Python build
        self._found_names = []
        for name in self.CYTHON_MODULE_NAMES:
            built_path = self.get_ext_built(name)
            if not os.path.exists(built_path):
                print(built_path)
                if self._failure_permitted(name):
                    print('Cython module {0} failure permitted'.format(name))
                    continue
                raise RuntimeError('libpyarrow C-extension failed to build:',
                                   os.path.abspath(built_path))

            ext_path = pjoin(build_lib, self._get_cmake_ext_path(name))
            if os.path.exists(ext_path):
                os.remove(ext_path)
            self.mkpath(os.path.dirname(ext_path))
            print('Moving built libpyarrow C-extension', built_path,
                  'to build path', ext_path)
            shutil.move(self.get_ext_built(name), ext_path)
            self._found_names.append(name)

            if os.path.exists(self.get_ext_built_api_header(name)):
                shutil.move(self.get_ext_built_api_header(name),
                            pjoin(os.path.dirname(ext_path), name + '_api.h'))

        os.chdir(saved_cwd)

    def _failure_permitted(self, name):
        if name == 'parquet' and not self.with_parquet:
            return True
        return False

    def _get_inplace_dir(self):
        pass

    def _get_cmake_ext_path(self, name):
        # Get the package directory from build_py
        build_py = self.get_finalized_command('build_py')
        package_dir = build_py.get_package_dir('pyarrow')
        # This is the name of the arrow C-extension
        suffix = sysconfig.get_config_var('EXT_SUFFIX')
        if suffix is None:
            suffix = sysconfig.get_config_var('SO')
        filename = name + suffix
        return pjoin(package_dir, filename)

    def get_ext_built_api_header(self, name):
        if sys.platform == 'win32':
            head, tail = os.path.split(name)
            return pjoin(head, tail + "_api.h")
        else:
            return pjoin(name + "_api.h")

    def get_ext_built(self, name):
        if sys.platform == 'win32':
            head, tail = os.path.split(name)
            suffix = sysconfig.get_config_var('SO')
            return pjoin(head, self.build_type, tail + suffix)
        else:
            suffix = sysconfig.get_config_var('SO')
            return pjoin(self.build_type, name + suffix)

    def get_names(self):
        return self._found_names

    def get_outputs(self):
        # Just the C extensions
        # regular_exts = _build_ext.get_outputs(self)
        return [self._get_cmake_ext_path(name)
                for name in self.get_names()]

write_version_py()

DESC = """\
Python library for Apache Arrow"""

setup(
    name="pyarrow",
    packages=['pyarrow', 'pyarrow.tests'],
    version=VERSION,
    zip_safe=False,
    package_data={'pyarrow': ['*.pxd', '*.pyx']},
    # Dummy extension to trigger build_ext
    ext_modules=[Extension('__dummy__', sources=[])],

    cmdclass={
        'clean': clean,
        'build_ext': build_ext
    },
    install_requires=['cython >= 0.23', 'numpy >= 1.9', 'six >= 1.0.0'],
    description=DESC,
    license='Apache License, Version 2.0',
    maintainer="Apache Arrow Developers",
    maintainer_email="dev@arrow.apache.org",
    test_suite="pyarrow.tests"
)

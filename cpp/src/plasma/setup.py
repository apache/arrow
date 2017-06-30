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
import sys

from setuptools import setup, find_packages, Distribution
import setuptools.command.build_ext as _build_ext
from distutils import sysconfig

class build_ext(_build_ext.build_ext):
    def run(self):
        self._run_cmake()

    def _run_cmake(self):
        self.build_type = os.environ.get('PYARROW_BUILD_TYPE', 'debug').lower()

        # The directory containing this setup.py
        source = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../")

        # The staging directory for the module being built
        build_temp = os.path.join(os.getcwd(), self.build_temp)
        build_lib = os.path.join(os.getcwd(), self.build_lib)

        # Change to the build directory
        saved_cwd = os.getcwd()
        if not os.path.isdir(self.build_temp):
            self.mkpath(self.build_temp)
        os.chdir(self.build_temp)

        static_lib_option = ''

        cmake_options = [
            '-DPYTHON_EXECUTABLE=%s' % sys.executable,
            '-DARROW_PLASMA=on',
            '-DPLASMA_PYTHON=on',
            '-DARROW_BUILD_TESTS=off',
            static_lib_option,
        ]

        cmake_command = (['cmake'] + cmake_options + [source])

        print("-- Runnning cmake for plasma")
        self.spawn(cmake_command)
        print("-- Finished cmake for plasma")

        args = ['make']
        print("-- Running make for plasma")
        self.spawn(args)
        print("-- Finished make for plasma")

        self._found_names = []
        for name in ["plasma"]:
            built_path = self.get_ext_built(name)
            if not os.path.exists(built_path):
                raise RuntimeError('plasma C-extension failed to build:',
                                   os.path.abspath(built_path))

            ext_path = os.path.join(build_lib, self._get_cmake_ext_path(name))
            if os.path.exists(ext_path):
                os.remove(ext_path)
            self.mkpath(os.path.dirname(ext_path))
            print('Moving built C-extension', built_path,
                  'to build path', ext_path)
            shutil.move(self.get_ext_built(name), ext_path)
            self._found_names.append(name)

        # Move the plasma store
        build_py = self.get_finalized_command('build_py')
        source = os.path.join(self.build_type, "plasma_store")
        target = os.path.join(build_lib, build_py.get_package_dir('plasma'), "plasma_store")
        shutil.move(source, target)

        os.chdir(saved_cwd)

    def _get_cmake_ext_path(self, name):
        # Get the package directory from build_py
        build_py = self.get_finalized_command('build_py')
        package_dir = build_py.get_package_dir('plasma')
        # This is the name of the arrow C-extension
        suffix = sysconfig.get_config_var('EXT_SUFFIX')
        if suffix is None:
            suffix = sysconfig.get_config_var('SO')
        filename = name + suffix
        return os.path.join(package_dir, filename)

    def get_ext_built(self, name):
        if sys.platform == 'win32':
            head, tail = os.path.split(name)
            suffix = sysconfig.get_config_var('SO')
            return os.path.join(head, self.build_type, tail + suffix)
        else:
            suffix = sysconfig.get_config_var('SO')
            return os.path.join(self.build_type, name + suffix)

    def get_names(self):
        return self._found_names

    def get_outputs(self):
        # Just the C extensions
        # regular_exts = _build_ext.get_outputs(self)
        return [self._get_cmake_ext_path(name)
                for name in self.get_names()]

class BinaryDistribution(Distribution):
  def has_ext_modules(self):
    return True

setup(name="plasma",
      version="0.0.1",
      packages=["plasma"],
      cmdclass={"build_ext": build_ext},
      # The BinaryDistribution argument triggers build_ext.
      distclass=BinaryDistribution,
      install_requires=["numpy"],
      include_package_data=True,
      zip_safe=False,
      license="Apache 2.0")

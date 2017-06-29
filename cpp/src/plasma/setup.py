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

from setuptools import setup, find_packages, Distribution
import setuptools.command.build_ext as _build_ext

class build_ext(_build_ext.build_ext):
  def run(self):
    # Ideally, we could include these files by putting them in a MANIFEST.in or
    # using the package_data argument to setup, but the MANIFEST.in gets
    # applied at the very beginning when setup.py runs before these files have
    # been created, so we have to move the files manually.
    for filename in files_to_include:
      self.move_file(filename)

  def move_file(self, filename):
    # TODO(rkn): This feels very brittle. It may not handle all cases. See
    # https://github.com/apache/arrow/blob/master/python/setup.py for an
    # example.
    source = filename
    destination = os.path.join(self.build_lib, "plasma.cpython-36m-x86_64-linux-gnu.so")
    print("XXX build_lib is ", self.build_lib)
    # Create the target directory if it doesn't already exist.
    parent_directory = os.path.dirname(destination)
    if not os.path.exists(parent_directory):
      os.makedirs(parent_directory)
    print("Copying {} to {}.".format(source, destination))
    # shutil.copy(source, destination)
    shutil.copy("../../../python/arrow-build-3.6/debug/plasma.cpython-36m-x86_64-linux-gnu.so", destination)

files_to_include = [
    # "../../../python/arrow-build-3.6/debug/plasma.cpython-36m-x86_64-linux-gnu.so"
    "../../../cpp/build/debug/plasma.cpython-35m-x86_64-linux-gnu.so"
]

class BinaryDistribution(Distribution):
  def has_ext_modules(self):
    return True

setup(name="plasma",
      version="0.0.1",
      packages=find_packages(),
      cmdclass={"build_ext": build_ext},
      # The BinaryDistribution argument triggers build_ext.
      distclass=BinaryDistribution,
      install_requires=["numpy"],
      include_package_data=True,
      zip_safe=False,
      license="Apache 2.0")

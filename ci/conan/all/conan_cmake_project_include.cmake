# MIT License
#
# Copyright (c) 2019 Conan.io
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

if(ARROW_S3)
  find_package(AWSSDK REQUIRED)
  # Fix issue where scripts expect a variable called "AWSSDK_LINK_LIBRARIES"
  # which is not defined by the generated AWSSDKConfig.cmake
  if(NOT DEFINED AWSSDK_LINK_LIBRARIES)
    set(AWSSDK_LINK_LIBRARIES "${AWSSDK_LIBRARIES}")
  endif()

  # Causes logic used for generated .pc file to not run
  # avoiding instropection of target `aws-cpp-sdk::aws-cpp-sdk`
  # This is fine because the generated .pc file is not of use
  set(AWSSDK_SOURCE "conan")
endif()

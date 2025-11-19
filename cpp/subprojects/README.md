<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Meson Subprojects

The contents of this directory are exclusively used by the Meson build configuration to
manage project dependencies. The Meson documentation for this functionality can be found
at https://mesonbuild.com/Subprojects.html

To summarize, Arrow relies upon other projects to successfully compile and link. In
the case that those dependencies cannot be found on the host system, Meson by convention
forces users to place those dependencies in the `subprojects` directory at the root
of the project.

The easiest way to populate subprojects is to use Meson's
[WrapDB system](https://mesonbuild.com/Wrapdb-projects.html). To illustrate how this
works, let's take a look at the `googletest` library that Arrow depends upon for
its test system. To create that as a subproject, a developer once ran:

```bash
meson wrap install gtest
```

From the project root directory. From that invocation, Meson creates the
`subprojects/gtest.wrap` file which instructs the build system where it can
get the source for gtest (if required), optionally alongside any "patch files"
required to build gtest. If a project uses Meson natively, there is no need for
patch files. However, if the project uses another build system (in the case of gtest
, Bazel or CMake), then the patch files are user-created Meson configuration files
that still expose the required build targets, without being a full rewrite of the
native build generator. For an example of a user-created patch file for googletest,
check out
https://github.com/mesonbuild/wrapdb/tree/9e3862083a250680061aa46e8746499c419ad43c/subprojects/packagefiles/gtest

If you depend upon a project that is not available in Meson's WrapDB system,
you may still be able to have Meson auto-generate a wrapper for it. An example
of this is the `subprojects/azure.wrap`, which looks like:

```
[wrap-file]
source_url = https://github.com/Azure/azure-sdk-for-cpp/archive/azure-identity_1.9.0.tar.gz
source_filename = azure-sdk-for-cpp-azure-identity_1.9.0.tar.gz
source_hash = 97065bfc971ac8df450853ce805f820f52b59457bd7556510186a1569502e4a1
directory = azure-sdk-for-cpp-azure-identity_1.9.0
method = cmake
```

The `method = cmake` line is important here; it instructs Meson to inspect any
CMakeLists.txt files from the downloaded source and auto generate Meson configuration
files therefrom. The generated meson.build configuration(s) will be placed in
`<build_directory>/subprojects/<subproject_name>` at project configuration time.

In the default case, Meson will use the wrap file as a fallback. If a dependency
can be satisfied by the system, then it will not use the wrap file to download
any sources. However, you can toggle the behavior of the wrap system via the
`--wrap-mode=` configuration option. `--wrap-mode=forcefallback` will always
download and use the source defined in a wrap file, even if the depdendency could
be satisfied by the system. By contrast, `--wrap-mode=nofallback` will require
that the system satisfies dependencies. For more ways to handle wrap dependencies,
see https://mesonbuild.com/Subprojects.html#commandline-options

For more information on Meson's wrap system, see also
https://mesonbuild.com/Wrap-dependency-system-manual.html

In the majority of cases you will be using wrap files to describe subprojects, although
it is not always required. You could alternatively place a copy of the third party
project into the subprojects directory, if your preference is to completely vendor it.

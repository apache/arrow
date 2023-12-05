<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Java Jars Task

This directory is responsible to generate the jar files for the Arrow components that depend on C++ shared libraries to execute.

The Arrow C++ libraries are compiled both on macOS and Linux distributions, with their dependencies linked statically, and they are added
in the jars at the end, so the file can be used on both systems.

## Linux Docker Image
To compile the C++ libraries in Linux, a docker image is used. 
It is created used the **ci/docker/java-bundled-jars.dockerfile** file. 
If it is necessary to add any new dependency, you need to change that file.
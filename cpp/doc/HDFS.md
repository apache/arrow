<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

## Using Arrow's HDFS (Apache Hadoop Distributed File System) interface

### Build requirements

To build the integration, pass the following option to CMake

```shell
-DARROW_HDFS=on
```

For convenience, we have bundled `hdfs.h` for libhdfs from Apache Hadoop in
Arrow's thirdparty. If you wish to build against the `hdfs.h` in your installed
Hadoop distribution, set the `$HADOOP_HOME` environment variable.

### Runtime requirements

By default, the HDFS client C++ class in `libarrow_io` uses the libhdfs JNI
interface to the Java Hadoop client. This library is loaded **at runtime**
(rather than at link / library load time, since the library may not be in your
LD_LIBRARY_PATH), and relies on some environment variables.

* `HADOOP_HOME`: the root of your installed Hadoop distribution. Check in the
  `lib/native` directory to look for `libhdfs.so` if you have any questions
  about which directory you're after.
* `JAVA_HOME`: the location of your Java SDK installation
* `CLASSPATH`: must contain the Hadoop jars. You can set these using:

```shell
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
```

#### Setting $JAVA_HOME  automatically on OS X

The installed location of Java on OS X can vary, however the following snippet
will set it automatically for you:

```shell
export JAVA_HOME=$(/usr/libexec/java_home)
```

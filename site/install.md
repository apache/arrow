---
layout: default
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

## Current Version: 0.3.0

### Released: 5 May 2017

Apache Arrow 0.3.0 is the third major release of the project and has seen
significant iteration and hardening of logical types and the binary formats. It
is safe for production use, though there may be API changes and binary format
breaks in the future.

### Source release

* **Source Release**: [apache-arrow-0.3.0.tar.gz][6]
* **Verification**: [md5][3], [asc][7]
* [Git tag d8db8f8][2]

### Java Packages

[Java Artifacts on Maven Central][4]

### C++ and Python Conda Packages (Unofficial)

We have provided binary conda packages on [conda-forge][5] for the following
platforms:

* Linux and OS X (Python 2.7, 3.5, and 3.6)
* Windows (Python 3.5 and 3.6)

Install them with:


```shell
conda install arrow-cpp -c conda-forge
conda install pyarrow -c conda-forge
```

### Python Wheels on PyPI (Unofficial)

We have provided Linux binary wheels on PyPI, which can be installed with pip.

```shell
pip install pyarrow
```

These include the Apache Arrow and Apache Parquet C++ binary libraries bundled
with the wheel.

[1]: https://dist.apache.org/repos/dist/release/arrow/arrow-0.3.0
[2]: https://github.com/apache/arrow/releases/tag/apache-arrow-0.3.0
[3]: https://dist.apache.org/repos/dist/release/arrow/arrow-0.3.0/apache-arrow-0.3.0.tar.gz.md5
[4]: http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.arrow%22%20AND%20v%3A%220.3.0%22
[5]: http://conda-forge.github.io
[6]: https://dist.apache.org/repos/dist/release/arrow/arrow-0.3.0/apache-arrow-0.3.0.tar.gz
[7]: https://dist.apache.org/repos/dist/release/arrow/arrow-0.3.0/apache-arrow-0.3.0.tar.gz.asc

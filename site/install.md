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

## Current Version: {{site.data.versions['current'].number}}

### Released: {{site.data.versions['current'].date}}

See the [release notes][10] for more about what's new.

### Source release

* **Source Release**: [{{site.data.versions['current'].tarball_name}}][6]
* **Verification**: [asc signature][13], [sha256 checksum][14], [sha512 checksum][15], ([verification instructions][12])
* [Git tag {{site.data.versions['current'].git-tag}}][2]
* [GPG keys for release signatures][11]

### Java Packages

[Java Artifacts on Maven Central][4]

## Binary Installers for C, C++, Python

### C++ and Python Conda Packages (Unofficial)

We have provided binary conda packages on [conda-forge][5] for the following
platforms:

* Linux and macOS (Python 2.7, 3.5, and 3.6)
* Windows (Python 3.5 and 3.6)

Install them with:


```shell
conda install arrow-cpp=0.10.* -c conda-forge
conda install pyarrow=0.10.* -c conda-forge
```

### Python Wheels on PyPI

We have provided official binary wheels on PyPI for Linux, macOS, and Windows:

```shell
pip install pyarrow==0.10.*
```

We recommend pinning `0.10.*` in `requirements.txt` to install the latest patch
release.

These include the Apache Arrow and Apache Parquet C++ binary libraries bundled
with the wheel.

### C++ and GLib (C) Packages for Debian GNU/Linux, Ubuntu and CentOS (Unofficial)

We have provided APT and Yum repositories for Apache Arrow C++ and
Apache Arrow GLib (C). Here are supported platforms:

* Debian GNU/Linux stretch
* Ubuntu 14.04 LTS
* Ubuntu 16.04 LTS
* Ubuntu 18.04 LTS
* CentOS 6
* CentOS 7

Debian GNU/Linux:

```shell
sudo apt install -y -V apt-transport-https
sudo apt install -y -V lsb-release
cat <<APT_LINE | sudo tee /etc/apt/sources.list.d/red-data-tools.list
deb https://packages.red-data-tools.org/debian/ $(lsb_release --codename --short) main
deb-src https://packages.red-data-tools.org/debian/ $(lsb_release --codename --short) main
APT_LINE
sudo apt update --allow-insecure-repositories
sudo apt install -y -V --allow-unauthenticated red-data-tools-keyring
sudo apt update
sudo apt install -y -V libarrow-dev # For C++
sudo apt install -y -V libarrow-glib-dev # For GLib (C)
```

Ubuntu:

```shell
sudo apt install -y -V apt-transport-https
sudo apt install -y -V lsb-release
cat <<APT_LINE | sudo tee /etc/apt/sources.list.d/red-data-tools.list
deb https://packages.red-data-tools.org/ubuntu/ $(lsb_release --codename --short) universe
deb-src https://packages.red-data-tools.org/ubuntu/ $(lsb_release --codename --short) universe
APT_LINE
sudo apt update --allow-insecure-repositories || sudo apt update
sudo apt install -y -V --allow-unauthenticated red-data-tools-keyring
sudo apt update
sudo apt install -y -V libarrow-dev # For C++
sudo apt install -y -V libarrow-glib-dev # For GLib (C)
```

CentOS:

```shell
sudo yum install -y https://packages.red-data-tools.org/centos/red-data-tools-release-1.0.0-1.noarch.rpm
sudo yum install -y --enablerepo=epel arrow-devel # For C++
sudo yum install -y --enablerepo=epel arrow-glib-devel # For GLib (C)
```

These repositories also provide Apache Parquet C++ and
[Parquet GLib][8]. You can install them by the followings:

Debian GNU/Linux and Ubuntu:

```shell
sudo apt install -y -V libparquet-dev # For Apache Parquet C++
sudo apt install -y -V libparquet-glib-dev # For Parquet GLib (C)
```

CentOS:

```shell
sudo yum install -y --enablerepo=epel parquet-devel # For Apache Parquet C++
sudo yum install -y --enablerepo=epel parquet-glib-devel # For Parquet GLib (C)
```

These repositories are managed at
[red-data-tools/arrow-packages][9]. If you have any feedback, please
send it to the project instead of Apache Arrow project.

[1]: {{site.data.versions['current'].mirrors}}
[2]: {{site.data.versions['current'].github-tag-link}}
[4]: {{site.data.versions['current'].java-artifacts}}
[5]: http://conda-forge.github.io
[6]: {{site.data.versions['current'].mirrors-tar}}
[8]: https://github.com/red-data-tools/parquet-glib
[9]: https://github.com/red-data-tools/arrow-packages
[10]: {{site.data.versions['current'].release-notes}}
[11]: http://www.apache.org/dist/arrow/KEYS
[12]: https://www.apache.org/dyn/closer.cgi#verify
[13]: {{site.data.versions['current'].asc}}
[14]: {{site.data.versions['current'].sha256}}
[15]: {{site.data.versions['current'].sha512}}

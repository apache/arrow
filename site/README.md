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

# Apache Arrow Website

## Overview

Site content is maintained in the main Arrow repository, mostly in markdown
format. [Jekyll](https://jekyllrb.com/) is used to generate HTML files that can
then be committed to the [arrow-site](https://github.com/apache/arrow-site)
repository.

## Prerequisites

With Ruby >= 2.1 installed, run the following commands to install
[Jekyll](https://jekyllrb.com/).

```shell
gem install jekyll bundler
bundle install
```

On some platforms, the Ruby `nokogiri` library may fail to build, in
such cases the following configuration option may help:

```
bundle config build.nokogiri --use-system-libraries
```

If you are planning to publish the website, you must clone the arrow-site git
repository. Run this command from the `site` directory so that `asf-site` is a
subdirectory of `site`.

```shell
git clone --branch=asf-site https://github.com/apache/arrow-site.git asf-site
```

## Previewing the site

From the `site` directory, run the following to generate HTML files and run the
web site locally.

```
bundle exec jekyll serve
```

## Publishing

After following the above instructions, run the following commands from the
`site` directory:

```shell
JEKYLL_ENV=production bundle exec jekyll build
rsync -r build/ asf-site/
cd asf-site
git status
```

Now `git add` any new files, then commit everything, and push:

```
git push
```

### Updating Code Documentation

To update the documentation, run the script `./dev/gen_apidocs.sh`. This script
will run the code documentation tools in a fixed environment.

#### C (GLib)

First, build Apache Arrow C++ and Apache Arrow GLib.

```
mkdir -p ../cpp/build
cd ../cpp/build
cmake .. -DCMAKE_BUILD_TYPE=debug
make
cd ../../c_glib
./autogen.sh
./configure \
  --with-arrow-cpp-build-dir=$PWD/../cpp/build \
  --with-arrow-cpp-build-type=debug \
  --enable-gtk-doc
LD_LIBRARY_PATH=$PWD/../cpp/build/debug make GTK_DOC_V_XREF=": "
rsync -r doc/reference/html/ ../site/asf-site/docs/c_glib/
```

#### Javascript

```
cd ../js
npm run doc
rsync -r doc/ ../site/asf-site/docs/js
```

Then add/commit/push from the site/asf-site git checkout.

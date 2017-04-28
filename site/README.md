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

## Apache Arrow Website

### Development instructions

If you are planning to publish the website, you must first clone the arrow-site
git repository:

```shell
git clone --branch=asf-site https://git-wip-us.apache.org/repos/asf/arrow-site.git asf-site
```

Now, with Ruby >= 2.1 installed, run:

```shell
gem install jekyll bundler
bundle install

# This imports the format Markdown documents so they will be rendered
scripts/sync_format_docs.sh

bundle exec jekyll serve
```

### Publishing

After following the above instructions the base `site/` directory, run:

```shell
bundle exec jekyll build
rsync -r build/ asf-site/
cd asf-site
git status
```

Now `git add` any new files, then commit everything, and push:

```
git push
```

### Updating Code Documentation

#### Java

```
cd ../java
mvn install
mvn site
rsync -r target/site/apidocs/ ../site/asf-site/docs/java/
```

#### C++

```
cd ../cpp/apidoc
doxygen Doxyfile
rsync -r html/ ../../site/asf-site/docs/cpp
```

#### Python

First, build PyArrow with all optional extensions (Apache Parquet, jemalloc).

```
cd ../python
python setup.py build_ext --inplace --with-parquet --with-jemalloc
python setup.py build_sphinx -s doc/source
rsync -r doc/_build/html/ ../site/asf-site/docs/python/
```

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

Then add/commit/push from the site/asf-site git checkout.

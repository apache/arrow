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

## Manylinux1 wheels for Apache Arrow

This folder provides base Docker images and an infrastructure to build
`manylinux1` compatible Python wheels that should be installable on all
Linux distributions published in last four years.

The process is split up in two parts: There are base Docker images that build
the native, Python-indenpendent dependencies. For these you can select if you
want to also build the dependencies used for the Parquet support. Depending on
these images, there is also a bash script that will build the pyarrow wheels
for all supported Python versions and place them in the `dist` folder.

### Build instructions

```bash
# Create a clean copy of the arrow source tree
git clone ../../ arrow
# Build the native baseimage
docker build -t arrow-base-x86_64 -f Dockerfile-x86_64 .
# Build the python packages
docker run --rm -v $PWD:/io arrow-base-x86_64 /io/build_arrow.sh
# Now the new packages are located in the dist/ folder
ls -l dist/
```

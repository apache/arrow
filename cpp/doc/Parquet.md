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

## Building Arrow-Parquet integration

To use Arrow C++ with Parquet, you must first build the Arrow C++ libraries and
install them someplace. Then, you can build [parquet-cpp][1] with the Arrow
adapter library:

```bash
# Set this to your preferred install location
export ARROW_HOME=$HOME/local

git clone https://github.com/apache/parquet-cpp.git
cd parquet-cpp
cmake -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME -DPARQUET_ARROW=on
make -j4
make install
```

[1]: https://github.com/apache/parquet-cpp

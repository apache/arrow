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

# Arrow Lua example

There are Lua example codes in this directory.

## How to run

All example codes use [LGI](https://github.com/pavouk/lgi) to use
Arrow GLib based bindings.

Here are command lines to install LGI on Debian GNU/Linux and Ubuntu:

```text
% sudo apt install -y luarocks
% sudo luarocks install lgi
```

## Lua example codes

Here are example codes in this directory:

  * `write-batch.lua`: It shows how to write Arrow array to file in
    batch mode.

  * `read-batch.lua`: It shows how to read Arrow array from file in
    batch mode.

  * `write-stream.lua`: It shows how to write Arrow array to file in
    stream mode.

  * `read-stream.lua`: It shows how to read Arrow array from file in
    stream mode.

  * `stream-to-torch-tensor.lua`: It shows how to read Arrow array
    from file in stream mode and convert it to
    [Torch](http://torch.ch/)'s
    [`Tensor` object](http://torch7.readthedocs.io/en/rtd/tensor/index.html).

# Copyright © 2019-2020 Intel Corporation

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

project(
  'google-brotli',
  'c',
  license: 'MIT',
  version: '1.1.1',
  meson_version: '>= 0.49',
)

cc = meson.get_compiler('c')

dep_m = cc.find_library('m', required: false)
if not cc.has_function('log2', dependencies: dep_m)
  error('Cannot find required log2() function')
endif

inc = include_directories('c/include')

if host_machine.system() == 'linux'
  add_project_arguments('-DOS_LINUX', language: 'c')
elif host_machine.system() == 'freebsd'
  add_project_arguments('-DOS_FREEBSD', language: 'c')
elif host_machine.system() == 'darwin'
  add_project_arguments('-DOS_MACOSX', language: 'c')
endif

c_args = []
bcs = []
bds = []
bes = []
install = false
if get_option('default_library') == 'shared'
  c_args += ['-DBROTLI_SHARED_COMPILATION', '-DBROTLI_BUILD_PORTABLE']
  install = true
  bcs = '-DBROTLICOMMON_SHARED_COMPILATION'
  bds = '-DBROTLIDEC_SHARED_COMPILATION'
  bes = '-DBROTLIENC_SHARED_COMPILATION'
elif get_option('default_library') == 'both'
  error('both not yet supported.')
endif

common_src = files(
  'c/common/constants.c',
  'c/common/context.c',
  'c/common/dictionary.c',
  'c/common/platform.c',
  'c/common/shared_dictionary.c',
  'c/common/transform.c',
)

libbrotli_common = library(
  'brotli_common',
  common_src,
  gnu_symbol_visibility: 'hidden',
  c_args: [c_args, bcs],
  include_directories: inc,
  version: meson.project_version(),
  install: install,
)

dec_src = files(
  'c/dec/bit_reader.c',
  'c/dec/decode.c',
  'c/dec/huffman.c',
  'c/dec/state.c',
)

libbrotli_decoder = library(
  'brotli_decoder',
  dec_src,
  gnu_symbol_visibility: 'hidden',
  c_args: [c_args, bds],
  include_directories: inc,
  link_with: libbrotli_common,
  version: meson.project_version(),
  install: install,
)

enc_src = files(
  'c/enc/backward_references.c',
  'c/enc/backward_references_hq.c',
  'c/enc/bit_cost.c',
  'c/enc/block_splitter.c',
  'c/enc/brotli_bit_stream.c',
  'c/enc/cluster.c',
  'c/enc/command.c',
  'c/enc/compound_dictionary.c',
  'c/enc/compress_fragment.c',
  'c/enc/compress_fragment_two_pass.c',
  'c/enc/dictionary_hash.c',
  'c/enc/encode.c',
  'c/enc/encoder_dict.c',
  'c/enc/entropy_encode.c',
  'c/enc/fast_log.c',
  'c/enc/histogram.c',
  'c/enc/literal_cost.c',
  'c/enc/memory.c',
  'c/enc/metablock.c',
  'c/enc/static_dict.c',
  'c/enc/utf8_util.c',
)

libbrotli_encoder = library(
  'brotli_encoder',
  enc_src,
  c_args: [c_args, bes],
  gnu_symbol_visibility: 'hidden',
  include_directories: inc,
  link_with: libbrotli_common,
  dependencies: dep_m,
  version: meson.project_version(),
  install: install,
)

brotli_common_dep = declare_dependency(
  link_with: libbrotli_common,
  include_directories: inc,
)

brotli_encoder_dep = declare_dependency(
  link_with: [libbrotli_common, libbrotli_encoder],
  include_directories: inc,
)

brotli_decoder_dep = declare_dependency(
  link_with: [libbrotli_common, libbrotli_decoder],
  include_directories: inc,
)

executable(
  'brotli',
  'c/tools/brotli.c',
  dependencies: [brotli_decoder_dep, brotli_encoder_dep],
)

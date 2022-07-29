#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import subprocess as sbp
import sys

try:
    import pandas as pd
    HAVE_PANDAS = True
except ImportError:
    HAVE_PANDAS = False

SYMBOL_FILTERS = {
    'std::chrono::duration': 'duration',
    'std::__cxx11::basic_string': 'std::string',
    'arrow::ArrayData': 'ArrayData',
    'arrow::ArraySpan': 'ArraySpan',
    'arrow::Datum': 'Datum',
    'arrow::Scalar': 'Scalar',
    'arrow::Status': 'Status',
    'arrow::Type': 'Type',
    'arrow::TimestampType': 'TsT',
    'arrow::BinaryType': 'BinaryT',
    'arrow::BooleanType': 'BoolT',
    'arrow::StringType': 'StringT',
    'arrow::LargeStringType': 'LStringT',
    'arrow::DoubleType': 'DoubleT',
    'arrow::FloatType': 'FloatT',
    'arrow::Int64Type': 'Int64T',
    'arrow::UInt64Type': 'UInt64T',
    'arrow::LargeListType': 'LListT',
    'arrow::ListType': 'ListT',
    'arrow::FixedSizeListType': 'FSLT',
    'arrow::compute::': 'ac::',
    'ac::internal::': '',
    'arrow::internal::': 'ai::',
    '(anonymous namespace)::': '',
    'internal::applicator::': '',
    'internal::CastFunctor': 'CastFunctor',
    'ac::KernelContext*': 'C*',
    'ArrayData const&': 'A&',
    'ArraySpan const&': 'A&',
    'ArrayData*': 'O*',
    'Scalar const&': 'S&',
    'Datum const&': 'V&',
    'Datum*': 'O*',
    'ac::ExecBatch const&': 'B&',
    'ac::ExecSpan const&': 'B&',
    'ac::ExecValue const&': 'V&',
    'ac::ExecResult*': 'O*',
    'Type::type': 'T',
}


def filter_symbol(symbol_name):
    for token, replacement in SYMBOL_FILTERS.items():
        symbol_name = symbol_name.replace(token, replacement)
    return symbol_name


def get_symbols_and_sizes(object_file):
    cmd = f"nm --print-size --size-sort {object_file} | c++filt"
    output = sbp.check_output(cmd, shell=True).decode('utf-8')
    symbol_sizes = []
    for x in output.split('\n'):
        if len(x) == 0:
            continue
        _, hex_size, _, symbol_name = x.split(' ', 3)
        symbol_name = filter_symbol(symbol_name)
        symbol_sizes.append((symbol_name, int(hex_size, 16)))
    return dict(symbol_sizes)


if __name__ == '__main__':
    base, contender = sys.argv[1], sys.argv[2]

    base_results = get_symbols_and_sizes(base)
    contender_results = get_symbols_and_sizes(contender)

    all_symbols = set(base_results.keys()) | set(contender_results.keys())

    diff_table = []
    for name in all_symbols:
        if name in base_results and name in contender_results:
            base_size = base_results[name]
            contender_size = contender_results[name]
        elif name in base_results:
            base_size = base_results[name]
            contender_size = 0
        else:
            base_size = 0
            contender_size = contender_results[name]
        diff = contender_size - base_size
        diff_table.append((name, base_size, contender_size, diff))
    diff_table.sort(key=lambda x: x[3])

    if HAVE_PANDAS:
        diff = pd.DataFrame.from_records(diff_table,
                                         columns=['symbol', 'base',
                                                  'contender', 'diff'])
        pd.options.display.max_rows = 1000
        pd.options.display.max_colwidth = 150
        print(diff[diff['diff'] < - 700])
        print(diff[diff['diff'] > 700])
    else:
        # TODO
        pass

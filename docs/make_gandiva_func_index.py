# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

'''Generates a RST file containing all Gandiva function signatures

This is meant as a temporary script until we begin writing docs for Gandiva funcs.'''
from collections import defaultdict
import pyarrow as pa

def get_functions():
    try:
        import pyarrow.gandiva as gandiva
        return gandiva.get_registered_function_signatures()
    except ImportError:
        return []

COMPARE_FUNCS = [
    'not',
    'isnull',
    'isnotnull',
    'isnumeric',
    'equal',
    'eq',
    'same',
    'not_equal',
    'less_than',
    'less_than_or_equal_to',
    'greater_than',
    'greater_than_or_equal_to',
    'is_distinct_from',
    'is_not_distinct_from',
]

ARITHMETIC_FUNCS = [
    'add',
    'subtract',
    'multiply',
    'divide',
    'div',
    'mod',
    'modulo',
    'bitwise_and',
    'bitwise_or',
    'bitwise_xor',
    'bitwise_not',
    'round',
    'cbrt',
    'exp',
    'log',
    'log10',
    'power',
    'pow',
    'sin',
    'cos',
    'asin',
    'acos',
    'tan',
    'atan',
    'sinh',
    'cosh',
    'tanh',
    'cot',
    'radians',
    'degrees',
    'atan2',
    'abs',
    'ceil',
    'floor',
    'truncate',
    'trunc',
    'rand',
    'random',
]

STRING_TYPES = [pa.string(), pa.binary(), ]

def get_function_group(func):
    if func.name().startswith('cast'):
        return 'Cast'
    elif func.name().startswith('hash'):
        return 'Hash'
    elif func.name() in COMPARE_FUNCS:
        return 'Comparisons'
    elif func.name() in ARITHMETIC_FUNCS:
        return 'Arithmetic and Math'
    elif ((len(func.param_types()) > 0 and pa.types.is_temporal(func.param_types()[0])) or
          func.name().startswith('timestamp') or func.name().startswith('date')
          or func.name() in ['extractDay']):
        return 'Dates and Timestamps'
    elif len(func.param_types()) > 0 and func.param_types()[0] in STRING_TYPES:
        return 'String Transformations'
    else:
        return 'Other'

grouped_funcs = defaultdict(lambda: defaultdict(list))

for func in get_functions():
    grouped_funcs[get_function_group(func)][func.name()].append(func)

with open('source/cpp/gandiva_functions.rst', 'w') as file:
    for group, name_list in grouped_funcs.items():
        file.write(f'{group}\n')
        file.write('-' * len(str(group)) + '\n')
        file.write('\n.. list-table:: \n\n')
        for name, func_list in name_list.items():
            # file.write(f"  * - {name}\n")

            for i, func in enumerate(sorted(func_list, key=lambda f: f.name())):
                if i == 0:
                    file.write(f"  * - {func.name()}\n")
                else:
                    file.write(f"  * -\n")
                file.write(f"    - ``({', '.join(str(f) for f in func.param_types())})`` -> ``{func.return_type()}``\n")
        file.write('\n')
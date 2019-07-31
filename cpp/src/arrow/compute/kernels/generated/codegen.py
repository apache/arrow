#!/usr/bin/env python

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

# Generate boilerplate code for kernel instantiation and other tedious tasks


import io
import os


INTEGER_TYPES = ['UInt8', 'Int8', 'UInt16', 'Int16',
                 'UInt32', 'Int32', 'UInt64', 'Int64']
FLOATING_TYPES = ['Float', 'Double']
NUMERIC_TYPES = ['Boolean'] + INTEGER_TYPES + FLOATING_TYPES

DATE_TIME_TYPES = ['Date32', 'Date64', 'Time32', 'Time64', 'Timestamp']

ARITHMETIC_TYPES_MAP = {
    'INT8': 'int8_t',
    'UINT8': 'uint8_t',
    'INT16': 'int16_t',
    'UINT16': 'uint16_t',
    'INT32': 'int32_t',
    'UINT32': 'uint32_t',
    'INT64': 'int64_t',
    'UINT64': 'uint64_t',
    'FLOAT': 'float',
    'DOUBLE': 'double',
}


def _format_type(name):
    return name + "Type"

class CastCodeGenerator(object):

    def __init__(self, type_name, out_types, parametric=False,
                 exclusions=None):
        self.type_name = type_name
        self.out_types = out_types
        self.parametric = parametric
        self.exclusions = exclusions

    def generate(self):
        buf = io.StringIO()
        print("#define {0}_CASES(TEMPLATE) \\"
              .format(self.type_name.upper()), file=buf)

        this_type = _format_type(self.type_name)

        templates = []
        for out_type in self.out_types:
            if not self.parametric and out_type == self.type_name:
                # Parametric types need T -> T cast generated
                continue
            templates.append("  TEMPLATE({0}, {1})"
                             .format(this_type, _format_type(out_type)))

        print(" \\\n".join(templates), file=buf)
        return buf.getvalue()


CAST_GENERATORS = [
    CastCodeGenerator('Boolean', NUMERIC_TYPES),
    CastCodeGenerator('UInt8', NUMERIC_TYPES),
    CastCodeGenerator('Int8', NUMERIC_TYPES),
    CastCodeGenerator('UInt16', NUMERIC_TYPES),
    CastCodeGenerator('Int16', NUMERIC_TYPES),
    CastCodeGenerator('UInt32', NUMERIC_TYPES),
    CastCodeGenerator('UInt64', NUMERIC_TYPES),
    CastCodeGenerator('Int32', NUMERIC_TYPES),
    CastCodeGenerator('Int64', NUMERIC_TYPES),
    CastCodeGenerator('Float', NUMERIC_TYPES),
    CastCodeGenerator('Double', NUMERIC_TYPES),
    CastCodeGenerator('Date32', ['Date64']),
    CastCodeGenerator('Date64', ['Date32']),
    CastCodeGenerator('Time32', ['Time32', 'Time64'],
                      parametric=True),
    CastCodeGenerator('Time64', ['Time32', 'Time64'],
                      parametric=True),
    CastCodeGenerator('Timestamp', ['Date32', 'Date64', 'Timestamp'],
                      parametric=True),
    CastCodeGenerator('Binary', ['String']),
    CastCodeGenerator('LargeBinary', ['LargeString']),
    CastCodeGenerator('String', NUMERIC_TYPES + ['Timestamp']),
    CastCodeGenerator('LargeString', NUMERIC_TYPES + ['Timestamp']),
    CastCodeGenerator('Dictionary',
                      INTEGER_TYPES + FLOATING_TYPES + DATE_TIME_TYPES +
                      ['Null', 'Binary', 'FixedSizeBinary', 'String',
                       'Decimal128'])
]


def generate_cast_code():
    blocks = [generator.generate() for generator in CAST_GENERATORS]
    return '\n'.join(blocks)


def generate_arithmetic_code():
    buf = io.StringIO()
    print("#define ARITHMETIC_TYPESWITCH(OUT, TYPE, T1, T2, T3) \\", file=buf)

    def generate_kernel_instantiation(tab, typs):
        print("{0}OUT = new TYPE<{1}>(T1); \\".format("  " * (tab), 
            ', '.join(typs)), file=buf)

    def generate_type_switch(tab, cur_typ, typs):
        if(len(typs) == 3):
            generate_kernel_instantiation(tab+1, typs)
            return

        print("{0}switch({1}->id()) {{ \\".format("  " * (tab + 1), "T" + str(cur_typ)), file=buf)
        for t, ctyp in ARITHMETIC_TYPES_MAP.items():
            print("{0}case Type::{1}: \\".format("  " * (tab + 2), t), file=buf)
            generate_type_switch(tab+2, cur_typ+1, typs + [ctyp])
            print("{0}break; \\".format("  " * (tab + 3)), file=buf)
        print("{0}default: \\".format("  " * (tab + 2)), file=buf)
        print("{0}OUT = NULLPTR; \\".format("  " * (tab + 3)), file=buf)
        print("{0}break; \\".format("  " * (tab + 2)), file=buf)
        print("{0}}} \\".format("  " * (tab + 1)), file=buf)

    generate_type_switch(0, 1, [])
    print(file=buf)
    return buf.getvalue()    

def write_file_with_preamble(path, code):
    preamble = """// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// THIS FILE IS AUTOMATICALLY GENERATED, DO NOT EDIT
// Generated by codegen.py script
"""

    with open(path, 'wb') as f:
        f.write(preamble.encode('utf-8'))
        f.write(code.encode('utf-8'))


def write_files():
    here = os.path.abspath(os.path.dirname(__file__))
    cast_code = generate_cast_code()
    write_file_with_preamble(os.path.join(here, 'cast-codegen-internal.h'),
                             cast_code)
    arithmetic_code = generate_arithmetic_code()
    write_file_with_preamble(os.path.join(here, 'arithmetic-codegen-internal.h'),
                            arithmetic_code)


if __name__ == '__main__':
    write_files()

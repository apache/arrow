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

# cmake-format configuration file
# Use `archery lint --cmake-format --fix` to reformat all cmake files in the
# source tree

# -----------------------------
# Options affecting formatting.
# -----------------------------
with section("format"):
    # How wide to allow formatted cmake files
    line_width = 90

    # How many spaces to tab for indent
    tab_size = 2

    # If a positional argument group contains more than this many arguments,
    # then force it to a vertical layout.
    max_pargs_hwrap = 4

    # If the statement spelling length (including space and parenthesis) is
    # smaller than this amount, then force reject nested layouts.
    # This value only comes into play when considering whether or not to nest
    # arguments below their parent. If the number of characters in the parent
    # is less than this value, we will not nest.
    min_prefix_chars = 32

    # If true, separate flow control names from their parentheses with a space
    separate_ctrl_name_with_space = False

    # If true, separate function names from parentheses with a space
    separate_fn_name_with_space = False

    # If a statement is wrapped to more than one line, than dangle the closing
    # parenthesis on it's own line
    dangle_parens = False

    # What style line endings to use in the output.
    line_ending = 'unix'

    # Format command names consistently as 'lower' or 'upper' case
    command_case = 'lower'

    # Format keywords consistently as 'lower' or 'upper' case
    keyword_case = 'unchanged'

# ------------------------------------------------
# Options affecting comment reflow and formatting.
# ------------------------------------------------
with section("markup"):
    # enable comment markup parsing and reflow
    enable_markup = False

    # If comment markup is enabled, don't reflow the first comment block in
    # eachlistfile. Use this to preserve formatting of your
    # copyright/licensestatements.
    first_comment_is_literal = True

    # If comment markup is enabled, don't reflow any comment block which
    # matches this (regex) pattern. Default is `None` (disabled).
    literal_comment_pattern = None

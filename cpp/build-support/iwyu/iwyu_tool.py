#!/usr/bin/env python

# This file has been imported into the apache source tree from
# the IWYU source tree as of version 0.8
#   https://github.com/include-what-you-use/include-what-you-use/blob/master/iwyu_tool.py
# and corresponding license has been added:
#   https://github.com/include-what-you-use/include-what-you-use/blob/master/LICENSE.TXT
#
# ==============================================================================
# LLVM Release License
# ==============================================================================
# University of Illinois/NCSA
# Open Source License
#
# Copyright (c) 2003-2010 University of Illinois at Urbana-Champaign.
# All rights reserved.
#
# Developed by:
#
#     LLVM Team
#
#     University of Illinois at Urbana-Champaign
#
#     http://llvm.org
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal with
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimers.
#
#     * Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimers in the
#       documentation and/or other materials provided with the distribution.
#
#     * Neither the names of the LLVM Team, University of Illinois at
#       Urbana-Champaign, nor the names of its contributors may be used to
#       endorse or promote products derived from this Software without specific
#       prior written permission.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
# CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE
# SOFTWARE.

""" Driver to consume a Clang compilation database and invoke IWYU.

Example usage with CMake:

  # Unix systems
  $ mkdir build && cd build
  $ CC="clang" CXX="clang++" cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ...
  $ iwyu_tool.py -p .

  # Windows systems
  $ mkdir build && cd build
  $ cmake -DCMAKE_CXX_COMPILER="%VCINSTALLDIR%/bin/cl.exe" \
    -DCMAKE_C_COMPILER="%VCINSTALLDIR%/VC/bin/cl.exe" \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -G Ninja ...
  $ python iwyu_tool.py -p .

See iwyu_tool.py -h for more details on command-line arguments.
"""

import os
import sys
import json
import argparse
import subprocess
import re

import logging

logging.basicConfig(filename='iwyu.log')
LOGGER = logging.getLogger("iwyu")


def iwyu_formatter(output):
    """ Process iwyu's output, basically a no-op. """
    print('\n'.join(output))


CORRECT_RE = re.compile(r'^\((.*?) has correct #includes/fwd-decls\)$')
SHOULD_ADD_RE = re.compile(r'^(.*?) should add these lines:$')
SHOULD_REMOVE_RE = re.compile(r'^(.*?) should remove these lines:$')
FULL_LIST_RE = re.compile(r'The full include-list for (.*?):$')
END_RE = re.compile(r'^---$')
LINES_RE = re.compile(r'^- (.*?)  // lines ([0-9]+)-[0-9]+$')


GENERAL, ADD, REMOVE, LIST = range(4)


def clang_formatter(output):
    """ Process iwyu's output into something clang-like. """
    state = (GENERAL, None)
    for line in output:
        match = CORRECT_RE.match(line)
        if match:
            print('%s:1:1: note: #includes/fwd-decls are correct', match.groups(1))
            continue
        match = SHOULD_ADD_RE.match(line)
        if match:
            state = (ADD, match.group(1))
            continue
        match = SHOULD_REMOVE_RE.match(line)
        if match:
            state = (REMOVE, match.group(1))
            continue
        match = FULL_LIST_RE.match(line)
        if match:
            state = (LIST, match.group(1))
        elif END_RE.match(line):
            state = (GENERAL, None)
        elif not line.strip():
            continue
        elif state[0] == GENERAL:
            print(line)
        elif state[0] == ADD:
            print('%s:1:1: error: add the following line', state[1])
            print(line)
        elif state[0] == REMOVE:
            match = LINES_RE.match(line)
            line_no = match.group(2) if match else '1'
            print('%s:%s:1: error: remove the following line', state[1], line_no)
            print(match.group(1))


DEFAULT_FORMAT = 'iwyu'
FORMATTERS = {
    'iwyu': iwyu_formatter,
    'clang': clang_formatter
}

def get_output(cwd, command):
    """ Run the given command and return its output as a string. """
    process = subprocess.Popen(command,
                               cwd=cwd,
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    return process.communicate()[0].decode("utf-8").splitlines()


def run_iwyu(cwd, compile_command, iwyu_args, verbose, formatter):
    """ Rewrite compile_command to an IWYU command, and run it. """
    compiler, _, args = compile_command.partition(' ')
    if compiler.endswith('cl.exe'):
        # If the compiler name is cl.exe, let IWYU be cl-compatible
        clang_args = ['--driver-mode=cl']
    else:
        clang_args = []

    iwyu_args = ['-Xiwyu ' + a for a in iwyu_args]
    command = ['include-what-you-use'] + clang_args + iwyu_args
    command = '%s %s' % (' '.join(command), args.strip())

    if verbose:
        print('%s:', command)

    formatter(get_output(cwd, command))


def main(compilation_db_path, source_files, verbose, formatter, iwyu_args):
    """ Entry point. """
    # Canonicalize compilation database path
    if os.path.isdir(compilation_db_path):
        compilation_db_path = os.path.join(compilation_db_path,
                                           'compile_commands.json')

    compilation_db_path = os.path.realpath(compilation_db_path)
    if not os.path.isfile(compilation_db_path):
        print('ERROR: No such file or directory: \'%s\'', compilation_db_path)
        return 1

    # Read compilation db from disk
    with open(compilation_db_path, 'r') as fileobj:
        compilation_db = json.load(fileobj)

    # expand symlinks
    for entry in compilation_db:
        entry['file'] = os.path.realpath(entry['file'])

    # Cross-reference source files with compilation database
    source_files = [os.path.realpath(s) for s in source_files]
    if not source_files:
        # No source files specified, analyze entire compilation database
        entries = compilation_db
    else:
        # Source files specified, analyze the ones appearing in compilation db,
        # warn for the rest.
        entries = []
        for source in source_files:
            matches = [e for e in compilation_db if e['file'] == source]
            if matches:
                entries.extend(matches)
            else:
                # TODO: As long as there is no complete compilation database available this check cannot be performed
                pass
                #print('WARNING: \'%s\' not found in compilation database.', source)

    # Run analysis
    try:
        for entry in entries:
            cwd, compile_command = entry['directory'], entry['command']
            run_iwyu(cwd, compile_command, iwyu_args, verbose, formatter)
    except OSError as why:
        print('ERROR: Failed to launch include-what-you-use: %s', why)
        return 1

    return 0


def _bootstrap():
    """ Parse arguments and dispatch to main(). """
    # This hackery is necessary to add the forwarded IWYU args to the
    # usage and help strings.
    def customize_usage(parser):
        """ Rewrite the parser's format_usage. """
        original_format_usage = parser.format_usage
        parser.format_usage = lambda: original_format_usage().rstrip() + \
                              ' -- [<IWYU args>]' + os.linesep

    def customize_help(parser):
        """ Rewrite the parser's format_help. """
        original_format_help = parser.format_help

        def custom_help():
            """ Customized help string, calls the adjusted format_usage. """
            helpmsg = original_format_help()
            helplines = helpmsg.splitlines()
            helplines[0] = parser.format_usage().rstrip()
            return os.linesep.join(helplines) + os.linesep

        parser.format_help = custom_help

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Include-what-you-use compilation database driver.',
        epilog='Assumes include-what-you-use is available on the PATH.')
    customize_usage(parser)
    customize_help(parser)

    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Print IWYU commands')
    parser.add_argument('-o', '--output-format', type=str,
                        choices=FORMATTERS.keys(), default=DEFAULT_FORMAT,
                        help='Output format (default: %s)' % DEFAULT_FORMAT)
    parser.add_argument('-p', metavar='<build-path>', required=True,
                        help='Compilation database path', dest='dbpath')
    parser.add_argument('source', nargs='*',
                        help='Zero or more source files to run IWYU on. '
                        'Defaults to all in compilation database.')

    def partition_args(argv):
        """ Split around '--' into driver args and IWYU args. """
        try:
            double_dash = argv.index('--')
            return argv[:double_dash], argv[double_dash+1:]
        except ValueError:
            return argv, []
    argv, iwyu_args = partition_args(sys.argv[1:])
    args = parser.parse_args(argv)

    sys.exit(main(args.dbpath, args.source, args.verbose,
                  FORMATTERS[args.output_format], iwyu_args))


if __name__ == '__main__':
    _bootstrap()

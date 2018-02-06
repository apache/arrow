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

from __future__ import print_function

import functools
import os
import pprint
import sys
import subprocess


perr = functools.partial(print, file=sys.stderr)

LANGUAGE_TOPICS = ['c_glib', 'cpp', 'java', 'js', 'python']

ALL_TOPICS = LANGUAGE_TOPICS + ['integration', 'site']


def list_affected_files(commit_range):
    """
    Return a list of files changed by the given git commit range.
    """
    cmdline = ["git", "diff", "--name-only", commit_range]
    proc = subprocess.Popen(cmdline,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError("Command {cmdline} failed with code {returncode}, "
                           "stderr was:\n{stderr}\n"
                           .format(cmdline=cmdline, returncode=returncode,
                                   stderr=err.decode()))

    return list(filter(None, (s.strip() for s in out.decode().splitlines())))


def get_travis_commit_range():
    cr = os.environ['TRAVIS_COMMIT_RANGE']
    assert cr
    # See https://github.com/travis-ci/travis-ci/issues/4596#issuecomment-139811122
    return cr.replace('...', '..')


def get_affected_topics(commit_range):
    """
    Return a dict of topics affected by the given git commit range.
    Each dict value is True if affected, False otherwise.
    """
    affected_files = list_affected_files(commit_range)
    perr("Affected files:", affected_files)

    affected = dict.fromkeys(ALL_TOPICS, False)

    for path in affected_files:
        parts = []
        head = path
        while head:
            head, tail = os.path.split(head)
            parts.append(tail)
        parts.reverse()
        assert parts
        p = parts[0]
        if p in ('ci', 'dev', '.travis.yml'):
            # For these changes, test everything
            for k in ALL_TOPICS:
                affected[k] = True
            break
        elif p in ('cpp', 'format'):
            # All languages are potentially affected
            for k in LANGUAGE_TOPICS:
                affected[k] = True
        elif p == 'java':
            affected['java'] = True
            affected['integration'] = True
        elif p in ('c_glib', 'js', 'python', 'site'):
            affected[p] = True

    perr("Affected topics:")
    perr(pprint.pformat(affected))

    return affected


def make_env_for_topics(affected):
    return {'ARROW_CI_{0}_AFFECTED'.format(k.upper()): '1' if v else '0'
            for k, v in affected.items()}


def get_unix_shell_eval(env):
    """
    Return a shell-evalable string to setup some environment variables.
    """
    return "; ".join(("export {0}='{1}'".format(k, v)
                      for k, v in env.items()))


def run_from_travis():
    affected = get_affected_topics(get_travis_commit_range())
    assert set(affected) <= set(ALL_TOPICS), affected
    return get_unix_shell_eval(make_env_for_topics(affected))


if __name__ == "__main__":
    print(run_from_travis())

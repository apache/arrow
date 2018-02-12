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


def run_cmd(cmdline):
    proc = subprocess.Popen(cmdline,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError("Command {cmdline} failed with code {returncode}, "
                           "stderr was:\n{stderr}\n"
                           .format(cmdline=cmdline, returncode=proc.returncode,
                                   stderr=err.decode()))
    return out


def get_commit_description(commit):
    """
    Return the textual description (title + body) of the given git commit.
    """
    out = run_cmd(["git", "show", "--no-patch", "--pretty=format:%B",
                   commit])
    return out.decode('utf-8', 'ignore')


def list_affected_files(commit_range):
    """
    Return a list of files changed by the given git commit range.
    """
    out = run_cmd(["git", "diff", "--name-only", commit_range])
    return list(filter(None, (s.strip() for s in out.decode().splitlines())))


def get_travis_head_commit():
    return os.environ['TRAVIS_COMMIT']


def get_travis_commit_range():
    cr = os.environ['TRAVIS_COMMIT_RANGE']
    # See https://github.com/travis-ci/travis-ci/issues/4596#issuecomment-139811122
    return cr.replace('...', '..')


def get_travis_commit_description():
    # Prefer this to get_commit_description(get_travis_head_commit()),
    # as rebasing or other repository events may make TRAVIS_COMMIT invalid
    # at the time we inspect it
    return os.environ['TRAVIS_COMMIT_MESSAGE']


def list_travis_affected_files():
    """
    Return a list of files affected in the current Travis build.
    """
    commit_range = get_travis_commit_range()
    try:
        return list_affected_files(commit_range)
    except RuntimeError:
        # TRAVIS_COMMIT_RANGE can contain invalid revisions when
        # building a branch (not a PR) after rebasing:
        # https://github.com/travis-ci/travis-ci/issues/2668
        if os.environ['TRAVIS_EVENT_TYPE'] == 'pull_request':
            raise
        # If it's a rebase, it's probably enough to use the last commit only
        commit_range = '{0}^..'.format(get_travis_head_commit())
        return list_affected_files(commit_range)


def get_affected_topics(affected_files):
    """
    Return a dict of topics affected by the given files.
    Each dict value is True if affected, False otherwise.
    """
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
        fn = parts[-1]
        if fn.startswith('README'):
            continue
        if p in ('ci', 'dev', '.travis.yml'):
            # For these changes, test everything
            for k in ALL_TOPICS:
                affected[k] = True
            break
        elif p in ('cpp', 'format'):
            # All languages are potentially affected
            for k in LANGUAGE_TOPICS:
                affected[k] = True
            affected['integration'] = True
        elif p in ('java', 'js'):
            affected[p] = True
            affected['integration'] = True
        elif p in ('c_glib', 'integration', 'python', 'site'):
            affected[p] = True

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
    desc = get_travis_commit_description()
    if '[skip travis]' in desc:
        # Skip everything
        affected = dict.fromkeys(ALL_TOPICS, False)
    elif '[force ci]' in desc or '[force travis]' in desc:
        # Test everything
        affected = dict.fromkeys(ALL_TOPICS, True)
    else:
        # Test affected topics
        affected_files = list_travis_affected_files()
        perr("Affected files:", affected_files)
        affected = get_affected_topics(affected_files)
        assert set(affected) <= set(ALL_TOPICS), affected

    perr("Affected topics:")
    perr(pprint.pformat(affected))
    return get_unix_shell_eval(make_env_for_topics(affected))


if __name__ == "__main__":
    # This script should have its output evaluated by a shell,
    # e.g. "eval `python ci/travis_detect_changes.py`"
    try:
        print(run_from_travis())
    except:
        # Make sure the enclosing eval will return an error
        print("exit 1")
        raise

#!/usr/bin/env python

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

# Utility for creating well-formed pull request merges and pushing them to
# Apache.
#   usage: ./merge_arrow_pr.py    (see config env vars below)
#
# This utility assumes you already have a local Arrow git clone and that you
# have added remotes corresponding to both (i) the Github Apache Arrow mirror
# and (ii) the apache git repo.
#
# There are several pieces of authorization possibly needed via environment
# variables
#
# JIRA_USERNAME: your Apache JIRA id
# JIRA_PASSWORD: your Apache JIRA password
# ARROW_GITHUB_API_TOKEN: a GitHub API token to use for API requests (to avoid
# rate limiting)

import os
import pprint
import re
import subprocess
import sys
import requests
import getpass

from six.moves import input
import six

try:
    import jira.client
except ImportError:
    print("Could not find jira library. "
          "Run 'sudo pip install jira' to install.")
    print("Exiting without trying to close the associated JIRA.")
    sys.exit(1)


# Prefix added to temporary branches
BRANCH_PREFIX = "PR_TOOL"
JIRA_API_BASE = "https://issues.apache.org/jira"


def get_json(url, headers=None):
    req = requests.get(url, headers=headers)
    return req.json()


def run_cmd(cmd):
    if isinstance(cmd, six.string_types):
        cmd = cmd.split(' ')

    try:
        output = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as e:
        # this avoids hiding the stdout / stderr of failed processes
        print('Command failed: %s' % cmd)
        print('With output:')
        print('--------------')
        print(e.output)
        print('--------------')
        raise e

    if isinstance(output, six.binary_type):
        output = output.decode('utf-8')
    return output


original_head = run_cmd("git rev-parse HEAD")[:8]


def clean_up():
    print("Restoring head pointer to %s" % original_head)
    run_cmd("git checkout %s" % original_head)

    branches = run_cmd("git branch").replace(" ", "").split("\n")

    for branch in [x for x in branches
                   if x.startswith(BRANCH_PREFIX)]:
        print("Deleting local branch %s" % branch)
        run_cmd("git branch -D %s" % branch)


_REGEX_CI_DIRECTIVE = re.compile(r'\[[^\]]*\]')


def strip_ci_directives(commit_message):
    # Remove things like '[force ci]', '[skip appveyor]' from the assembled
    # commit message
    return _REGEX_CI_DIRECTIVE.sub('', commit_message)


def fix_version_from_branch(branch, versions):
    # Note: Assumes this is a sorted (newest->oldest) list of un-released
    # versions
    if branch == "master":
        return versions[-1]
    else:
        branch_ver = branch.replace("branch-", "")
        return [x for x in versions if x.name.startswith(branch_ver)][-1]


# We can merge both ARROW and PARQUET patchesa
SUPPORTED_PROJECTS = ['ARROW', 'PARQUET']
PR_TITLE_REGEXEN = [(project, re.compile(r'^(' + project + r'-[0-9]+)\b.*$'))
                    for project in SUPPORTED_PROJECTS]


class JiraIssue(object):

    def __init__(self, jira_con, jira_id, project, cmd):
        self.jira_con = jira_con
        self.jira_id = jira_id
        self.project = project
        self.cmd = cmd

        try:
            self.issue = jira_con.issue(jira_id)
        except Exception as e:
            self.cmd.fail("ASF JIRA could not find %s\n%s" % (jira_id, e))

    def get_candidate_fix_versions(self, merge_branches=('master',)):
        # Only suggest versions starting with a number, like 0.x but not JS-0.x
        all_versions = self.jira_con.project_versions(self.project)
        unreleased_versions = [x for x in all_versions
                               if not x.raw['released']]

        unreleased_versions = sorted(unreleased_versions,
                                     key=lambda x: x.name, reverse=True)

        mainline_version_regex = re.compile(r'\d.*')
        mainline_versions = [x for x in unreleased_versions
                             if mainline_version_regex.match(x.name)]

        default_fix_versions = [
            fix_version_from_branch(x, mainline_versions).name
            for x in merge_branches]

        for v in default_fix_versions:
            # Handles the case where we have forked a release branch but not
            # yet made the release.  In this case, if the PR is committed to
            # the master branch and the release branch, we only consider the
            # release branch to be the fix version. E.g. it is not valid to
            # have both 1.1.0 and 1.0.0 as fix versions.
            (major, minor, patch) = v.split(".")
            if patch == "0":
                previous = "%s.%s.%s" % (major, int(minor) - 1, 0)
                if previous in default_fix_versions:
                    default_fix_versions = [x for x in default_fix_versions
                                            if x != v]

        return unreleased_versions, default_fix_versions

    def resolve(self, fix_versions, comment):
        cur_status = self.issue.fields.status.name
        cur_summary = self.issue.fields.summary
        cur_assignee = self.issue.fields.assignee
        if cur_assignee is None:
            cur_assignee = "NOT ASSIGNED!!!"
        else:
            cur_assignee = cur_assignee.displayName

        if cur_status == "Resolved" or cur_status == "Closed":
            self.cmd.fail("JIRA issue %s already has status '%s'"
                          % (self.jira_id, cur_status))
        print("=== JIRA %s ===" % self.jira_id)
        print("summary\t\t%s\nassignee\t%s\nstatus\t\t%s\nurl\t\t%s/%s\n"
              % (cur_summary, cur_assignee, cur_status,
                 '/'.join((JIRA_API_BASE, 'browse')),
                 self.jira_id))

        resolve = [x for x in self.jira_con.transitions(self.jira_id)
                   if x['name'] == "Resolve Issue"][0]
        self.jira_con.transition_issue(self.jira_id, resolve["id"],
                                       comment=comment,
                                       fixVersions=fix_versions)

        print("Successfully resolved %s!" % (self.jira_id))


class GitHubAPI(object):

    def __init__(self, project_name):
        self.github_api = ("https://api.github.com/repos/apache/{0}"
                           .format(project_name))

        token = os.environ.get('ARROW_GITHUB_API_TOKEN', None)
        if token:
            self.headers = {'Authorization': 'token {0}'.format(token)}
        else:
            self.headers = None

    def get_pr_data(self, number):
        return get_json("%s/pulls/%s" % (self.github_api, number),
                        headers=self.headers)


class CommandInput(object):
    """
    Interface to input(...) to enable unit test mocks to be created
    """

    def fail(self, msg):
        clean_up()
        raise Exception(msg)

    def prompt(self, prompt):
        return input(prompt)

    def getpass(self, prompt):
        return getpass.getpass(prompt)

    def continue_maybe(self, prompt):
        result = input("\n%s (y/n): " % prompt)
        if result.lower() != "y":
            self.fail("Okay, exiting")


class PullRequest(object):

    def __init__(self, cmd, github_api, git_remote, jira_con, number):
        self.cmd = cmd
        self.git_remote = git_remote
        self.con = jira_con
        self.number = number
        self._pr_data = github_api.get_pr_data(number)
        try:
            self.url = self._pr_data["url"]
            self.title = self._pr_data["title"]
            self.body = self._pr_data["body"]
            self.target_ref = self._pr_data["base"]["ref"]
            self.user_login = self._pr_data["user"]["login"]
            self.base_ref = self._pr_data["head"]["ref"]
        except KeyError:
            pprint.pprint(self._pr_data)
            raise
        self.description = "%s/%s" % (self.user_login, self.base_ref)

        self.jira_issue = self._get_jira()

    def show(self):
        print("\n=== Pull Request #%s ===" % self.number)
        print("title\t%s\nsource\t%s\ntarget\t%s\nurl\t%s"
              % (self.title, self.description, self.target_ref, self.url))

    @property
    def is_merged(self):
        return bool(self._pr_data["merged"])

    @property
    def is_mergeable(self):
        return bool(self._pr_data["mergeable"])

    def _get_jira(self):
        jira_id = None
        for project, regex in PR_TITLE_REGEXEN:
            m = regex.search(self.title)
            if m:
                jira_id = m.group(1)
                break

        if jira_id is None:
            options = ' or '.join('{0}-XXX'.format(project)
                                  for project in SUPPORTED_PROJECTS)
            self.cmd.fail("PR title should be prefixed by a jira id "
                          "{0}, but found {1}".format(options, self.title))

        return JiraIssue(self.con, jira_id, project, self.cmd)

    def merge(self, target_ref='master'):
        """
        merge the requested PR and return the merge hash
        """
        pr_branch_name = "%s_MERGE_PR_%s" % (BRANCH_PREFIX, self.number)
        target_branch_name = "%s_MERGE_PR_%s_%s" % (BRANCH_PREFIX,
                                                    self.number,
                                                    target_ref.upper())
        run_cmd("git fetch %s pull/%s/head:%s" % (self.git_remote,
                                                  self.number,
                                                  pr_branch_name))
        run_cmd("git fetch %s %s:%s" % (self.git_remote, target_ref,
                                        target_branch_name))
        run_cmd("git checkout %s" % target_branch_name)

        had_conflicts = False
        try:
            run_cmd(['git', 'merge', pr_branch_name, '--squash'])
        except Exception as e:
            msg = ("Error merging: %s\nWould you like to "
                   "manually fix-up this merge?" % e)
            self.cmd.continue_maybe(msg)
            msg = ("Okay, please fix any conflicts and 'git add' "
                   "conflicting files... Finished?")
            self.cmd.continue_maybe(msg)
            had_conflicts = True

        commit_authors = run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name,
                                 '--pretty=format:%an <%ae>']).split("\n")
        distinct_authors = sorted(set(commit_authors),
                                  key=lambda x: commit_authors.count(x),
                                  reverse=True)
        primary_author = distinct_authors[0]
        commits = run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name,
                          '--pretty=format:%h <%an> %s']).split("\n\n")

        merge_message_flags = []

        merge_message_flags += ["-m", self.title]
        if self.body is not None:
            merge_message_flags += ["-m", self.body]

        authors = "\n".join(["Author: %s" % a for a in distinct_authors])

        merge_message_flags += ["-m", authors]

        if had_conflicts:
            committer_name = run_cmd("git config --get user.name").strip()
            committer_email = run_cmd("git config --get user.email").strip()
            message = ("This patch had conflicts when merged, "
                       "resolved by\nCommitter: %s <%s>" %
                       (committer_name, committer_email))
            merge_message_flags += ["-m", message]

        # The string "Closes #%s" string is required for GitHub to correctly
        # close the PR
        merge_message_flags += [
            "-m",
            "Closes #%s from %s and squashes the following commits:"
            % (self.number, self.description)]
        for c in commits:
            stripped_message = strip_ci_directives(c).strip()
            merge_message_flags += ["-m", stripped_message]

        run_cmd(['git', 'commit',
                 '--no-verify',  # do not run commit hooks
                 '--author="%s"' % primary_author] +
                merge_message_flags)

        self.cmd.continue_maybe("Merge complete (local ref %s). Push to %s?"
                                % (target_branch_name, self.git_remote))

        try:
            run_cmd('git push %s %s:%s' % (self.git_remote,
                                           target_branch_name,
                                           target_ref))
        except Exception as e:
            clean_up()
            self.cmd.fail("Exception while pushing: %s" % e)

        merge_hash = run_cmd("git rev-parse %s" % target_branch_name)[:8]
        clean_up()
        print("Pull request #%s merged!" % self.number)
        print("Merge hash: %s" % merge_hash)
        return merge_hash


def cli():
    # Location of your Arrow git clone
    SEP = os.path.sep
    ARROW_HOME = os.path.abspath(__file__).rsplit(SEP, 2)[0]
    PROJECT_NAME = ARROW_HOME.rsplit(SEP, 1)[1]
    print("ARROW_HOME = " + ARROW_HOME)
    print("PROJECT_NAME = " + PROJECT_NAME)

    cmd = CommandInput()

    # ASF JIRA username
    jira_username = os.environ.get("JIRA_USERNAME")

    # ASF JIRA password
    jira_password = os.environ.get("JIRA_PASSWORD")

    if not jira_username:
        jira_username = cmd.prompt("Env JIRA_USERNAME not set, "
                                   "please enter your JIRA username:")

    if not jira_password:
        jira_password = cmd.getpass("Env JIRA_PASSWORD not set, "
                                    "please enter "
                                    "your JIRA password:")

    pr_num = input("Which pull request would you like to merge? (e.g. 34): ")

    # Remote name which points to the GitHub site
    git_remote = os.environ.get("PR_REMOTE_NAME", "apache")

    os.chdir(ARROW_HOME)

    jira_con = jira.client.JIRA({'server': JIRA_API_BASE},
                                basic_auth=(jira_username, jira_password))
    github_api = GitHubAPI(PROJECT_NAME)

    pr = PullRequest(cmd, github_api, git_remote, jira_con, pr_num)

    if pr.is_merged:
        print("Pull request %s has already been merged")
        sys.exit(0)

    if not pr.is_mergeable:
        msg = ("Pull request %s is not mergeable in its current form.\n"
               % pr_num + "Continue? (experts only!)")
        cmd.continue_maybe(msg)

    pr.show()

    cmd.continue_maybe("Proceed with merging pull request #%s?" % pr_num)

    # merged hash not used
    pr.merge()

    cmd.continue_maybe("Would you like to update the associated JIRA?")
    jira_comment = (
        "Issue resolved by pull request %s\n[%s/%s]"
        % (pr_num,
           "https://github.com/apache/" + PROJECT_NAME + "/pull",
           pr_num))

    versions, default_fix_versions = pr.jira_issue.get_candidate_fix_versions()

    default_fix_versions = ",".join(default_fix_versions)

    issue_fix_versions = cmd.prompt("Enter comma-separated "
                                    "fix version(s) [%s]: "
                                    % default_fix_versions)
    if issue_fix_versions == "":
        issue_fix_versions = default_fix_versions
    issue_fix_versions = issue_fix_versions.replace(" ", "").split(",")

    def get_version_json(version_str):
        return [x for x in versions if x.name == version_str][0].raw

    fix_versions_json = [get_version_json(v) for v in issue_fix_versions]
    pr.jira_issue.resolve(fix_versions_json, jira_comment)


if __name__ == '__main__':
    try:
        cli()
    except Exception:
        raise

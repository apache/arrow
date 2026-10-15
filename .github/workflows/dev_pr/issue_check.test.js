// Licensed to the Apache Software Foundation (ASF) under one
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

const assert = require("node:assert/strict");
const {test, mock} = require("node:test");
const checkIssue = require("./issue_check.js");

function request({assignees = [], returnedAssignees = [{login: "contributor"}],
                  title = "GH-34389: Check assignment results"} = {}) {
    const issue = {
        number: 34389,
        html_url: "https://github.com/apache/arrow/issues/34389",
        assignees,
        labels: [{name: "Component: Developer Tools"}]
    };
    const issues = {
        get: mock.fn(async () => ({data: issue})),
        addAssignees: mock.fn(async () => ({
            data: {...issue, assignees: returnedAssignees}
        })),
        createComment: mock.fn(async () => ({}))
    };
    const github = {rest: {issues}};
    const context = {
        repo: {owner: "apache", repo: "arrow"},
        payload: {
            number: 50000,
            pull_request: {title, user: {login: "contributor"}}
        }
    };
    return {github, context, issues};
}

test("reports successful assignment only after GitHub assigns the PR creator", async () => {
    const input = request();
    await checkIssue(input);

    assert.equal(input.issues.addAssignees.mock.callCount(), 1);
    assert.deepEqual(input.issues.addAssignees.mock.calls[0].arguments[0], {
        owner: "apache", repo: "arrow", issue_number: 34389,
        assignees: ["contributor"]
    });
    assert.equal(input.issues.createComment.mock.callCount(), 1);
    assert.deepEqual(input.issues.createComment.mock.calls[0].arguments[0], {
        owner: "apache", repo: "arrow", issue_number: 50000,
        body: ":warning: GitHub issue #34389 **has been automatically assigned in GitHub** to PR creator."
    });
});

for (const [description, returnedAssignees] of [
    ["silently ignores the assignment", []],
    ["returns a different assignee", [{login: "another-contributor"}]]
]) {
    test(`provides assignment instructions when GitHub ${description}`, async () => {
        const input = request({returnedAssignees});
        await checkIssue(input);

        assert.equal(input.issues.addAssignees.mock.callCount(), 1);
        assert.equal(input.issues.createComment.mock.callCount(), 1);
        const comment = input.issues.createComment.mock.calls[0].arguments[0];
        assert.equal(comment.issue_number, 50000);
        assert.doesNotMatch(comment.body, /has been automatically assigned/);
        assert.match(comment.body, /https:\/\/github.com\/apache\/arrow\/issues\/34389/);
        assert.match(comment.body, /comment/i);
        assert.match(comment.body, /assign/i);
        assert.match(comment.body, /maintainer|committer|collaborator/i);
    });
}

test("leaves an already assigned issue unchanged", async () => {
    const input = request({assignees: [{login: "another-contributor"}]});
    await checkIssue(input);

    assert.equal(input.issues.get.mock.callCount(), 1);
    assert.equal(input.issues.addAssignees.mock.callCount(), 0);
    assert.equal(input.issues.createComment.mock.callCount(), 0);
});

for (const title of ["MINOR: Fix a typo", "Fix assignment results"]) {
    test(`skips API calls for PR title '${title}'`, async () => {
        const input = request({title});
        await checkIssue(input);

        assert.equal(input.issues.get.mock.callCount(), 0);
        assert.equal(input.issues.addAssignees.mock.callCount(), 0);
        assert.equal(input.issues.createComment.mock.callCount(), 0);
    });
}

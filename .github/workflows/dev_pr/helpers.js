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

const https = require('https');

/**
 * Given the title of a PullRequest return the ID of the JIRA issue
 * @param {String} title 
 * @returns {String} the ID of the associated JIRA issue
 */
function detectJIRAID(title) {
    if (!title) {
        return null;
    }
    const matched = /^(WIP:?\s*)?((ARROW|PARQUET)-\d+)/.exec(title);
    if (!matched) {
        return null;
    }
    return matched[2];
}

/**
 * Given the title of a PullRequest checks if it contains a JIRA issue ID
 * @param {String} title 
 * @returns {Boolean} true if it starts with a JIRA ID or MINOR:
 */
function haveJIRAID(title) {
    if (!title) {
      return false;
    }
    if (title.startsWith("MINOR: ")) {
      return true;
    }
    return /^(WIP:?\s*)?(ARROW|PARQUET)-\d+/.test(title);
}

/**
 * Retrieves information about a JIRA issue.
 * @param {String} jiraID 
 * @returns {Object} the information about a JIRA issue.
 */
async function getJiraInfo(jiraID) {
    const jiraURL = `https://issues.apache.org/jira/rest/api/2/issue/${jiraID}`;

    return new Promise((resolve) => {
        https.get(jiraURL, res => {
            let data = '';

            res.on('data', chunk => { data += chunk }) 

            res.on('end', () => {
               resolve(JSON.parse(data));
            })
        })
    });
}

module.exports = {
    detectJIRAID,
    haveJIRAID,
    getJiraInfo
};
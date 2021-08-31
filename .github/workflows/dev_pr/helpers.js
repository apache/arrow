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
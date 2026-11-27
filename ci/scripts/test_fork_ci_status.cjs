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

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');

function workflow(name) {
  return JSON.parse(execFileSync('python3', [
    '-c', 'import json, sys, yaml; print(json.dumps(yaml.safe_load(sys.stdin)))',
  ], {
    input: readFileSync(path.join(__dirname, '../../.github/workflows', name)),
    encoding: 'utf8',
  }));
}
const bridgeWorkflow = workflow('fork_ci_status.yml');
const ciWorkflow = workflow('cpp.yml');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const bridge = new AsyncFunction('github', 'context', 'core', 'Date',
  bridgeWorkflow.jobs.bridge.steps[0].with.script);
const now = Date.UTC(2026, 9, 20, 12);
class Clock extends Date { static now() { return now; } }
const sha = 'a'.repeat(40);
const pr = {
  number: 10, state: 'open', updated_at: new Date(now).toISOString(),
  head: {
    sha, ref: 'feature',
    repo: { owner: { login: 'contributor' }, name: 'arrow', full_name: 'contributor/arrow' },
  },
  base: { ref: 'main' },
};
const run = (id = 1, extra = {}) => ({
  id, path: '.github/workflows/cpp.yml', event: 'push', head_sha: sha, head_branch: 'feature', run_attempt: 1,
  status: 'completed', conclusion: 'success',
  html_url: 'https://github.com/contributor/arrow/actions/runs/' + id,
  ...extra,
});

// A path-filtered C++ completion initialized this check on a previous event.
const seed = {
  id: 100, app: { slug: 'github-actions' }, external_id: 'arrow-fork-ci:10:cpp.yml',
  status: 'queued', output: {},
};

async function execute({
  event = 'schedule', checks = [seed], runs = [run()], workflowState = 'active',
  recent = [pr], slice = [pr], candidates = [pr], repo = 'arrow',
  owner = 'apache', runError, count = 1, notification = {}, checkErrors = {},
} = {}) {
  const writes = [], calls = [], failures = [];
  let sweepPage;
  const github = { rest: { checks: {}, actions: {}, pulls: {} } };
  github.rest.checks.listForRef = async () => {};
  github.rest.pulls.list = async (params) => {
    calls.push(['pulls', params]);
    if (params.per_page === 1) return {
      data: count ? [pr] : [],
      headers: count ? { link: '<https://api.github.com/pulls?per_page=1&page=' + count + '>; rel="last"' } : {},
    };
    assert.equal(params.per_page, 5);
    sweepPage = params.page;
    return { data: slice };
  };
  github.rest.actions.listWorkflowRunsForRepo = async () => {};
  github.rest.actions.listRepoWorkflows = async () => {};
  github.rest.checks.create = async (payload) => writes.push({ method: 'create', ...payload });
  github.rest.checks.update = async (payload) => writes.push({ method: 'update', ...payload });
  github.paginate = async (fn, params) => {
    if (fn === github.rest.actions.listWorkflowRunsForRepo) {
      calls.push(['runs', params]);
      assert.equal(params.owner, 'contributor');
      assert.equal(params.head_sha, sha);
      assert.equal(params.branch, 'feature');
      if (runError) throw Object.assign(new Error('API failure'), { status: runError });
      return runs;
    }
    if (fn === github.rest.actions.listRepoWorkflows) {
      calls.push(['workflows', params]);
      return workflowState ? [{ path: '.github/workflows/cpp.yml', state: workflowState }] : [];
    }
    if (fn === github.rest.checks.listForRef) {
      calls.push(['checks', params]);
      if (checkErrors[params.ref]) throw new Error(checkErrors[params.ref]);
      return checks;
    }
    assert.equal(fn, github.rest.pulls.list);
    assert.equal(params.head, 'contributor:feature');
    return candidates;
  };
  github.paginate.iterator = async function* () {
    for (let i = 0; i < recent.length; i += 100) yield { data: recent.slice(i, i + 100) };
  };
  await bridge(github, {
    repo: { owner, repo }, eventName: event,
    payload: { workflow_run: {
      path: '.github/workflows/cpp.yml', event: 'pull_request', head_sha: sha, head_branch: 'feature',
      head_repository: pr.head.repo, pull_requests: [], ...notification,
    } },
  }, { info: () => {}, setFailed: (message) => failures.push(message) }, Clock);
  return { writes, calls, sweepPage, failures };
}
async function existing(runValue = run()) {
  const { writes } = await execute({ runs: [runValue] });
  return {
    id: 100, app: { slug: 'github-actions' }, external_id: 'arrow-fork-ci:10:cpp.yml',
    ...writes[0], conclusion: writes[0].conclusion || null,
  };
}

test('CI completion discovers a fork PR even when the event PR list is empty', async () => {
  const { writes } = await execute({ event: 'workflow_run', checks: [] });
  assert.equal(writes.length, 1);
  assert.equal(writes[0].head_sha, sha);
  assert.equal(writes[0].name, 'C++ (fork)');
  assert.equal(writes[0].conclusion, 'success');
  assert.equal(writes[0].details_url, run().html_url);
});
test('notifications ignore stale branch heads, release PRs, closed PRs and deleted forks', async () => {
  for (const candidate of [
    { ...pr, head: { ...pr.head, sha: 'b'.repeat(40) } },
    { ...pr, base: { ref: 'maintenance' } },
    { ...pr, state: 'closed' },
    { ...pr, head: { ...pr.head, repo: null } },
  ]) {
    assert.deepEqual((await execute({ event: 'workflow_run', candidates: [candidate] })).writes, []);
  }
});
test('upstream pushes and merge-queue completions cannot create fork checks', async () => {
  for (const event of ['push', 'merge_group']) {
    assert.deepEqual((await execute({ event: 'workflow_run', notification: { event } })).writes, []);
  }
});
test('the bridge does nothing when copied into a fork', async () => {
  const result = await execute({ owner: 'contributor' });
  assert.deepEqual(result.calls, []);
  assert.deepEqual(result.writes, []);
});
test('only runs for the exact branch head on push/manual events are mirrored', async () => {
  const result = await execute({ runs: [
    run(8, { head_sha: 'b'.repeat(40) }),
    run(7, { head_branch: 'other' }),
    run(6, { event: 'pull_request' }),
    run(5, { event: 'workflow_dispatch' }),
    run(1),
  ] });
  assert.equal(result.writes[0].details_url, run(5).html_url);
});
for (const [state, expected] of [
  ['active', 'neutral'], ['disabled_fork', 'action_required'], [null, 'action_required'],
]) {
  test('missing run reports workflow state ' + state, async () => {
    const { writes } = await execute({ runs: [], workflowState: state });
    assert.equal(writes[0].conclusion, expected);
    assert.match(writes[0].output.summary, /Enable workflows/);
    assert.equal(JSON.parse(writes[0].output.text).run_id, null);
  });
}
test('a missing workflow endpoint is handled as missing CI', async () => {
  const { writes } = await execute({ runError: 404, workflowState: null });
  assert.equal(writes[0].conclusion, 'action_required');
});
test('permission, rate limit and server errors are not reported as missing CI', async () => {
  for (const runError of [403, 429, 500]) {
    const result = await execute({ runError });
    assert.deepEqual(result.writes, []);
    assert.deepEqual(result.failures, ['#10: API failure']);
    await assert.rejects(execute({ event: 'workflow_run', runError }), /API failure/);
  }
});
for (const conclusion of ['failure', 'cancelled', 'timed_out', 'startup_failure', 'stale']) {
  test('mirrors ' + conclusion + ' without reporting success', async () => {
    const { writes } = await execute({ runs: [run(1, { conclusion })] });
    assert.equal(writes[0].conclusion,
      ['startup_failure', 'stale'].includes(conclusion) ? 'failure' : conclusion);
  });
}
test('a stale run updates an existing check with failure, not the reserved conclusion', async () => {
  const { writes } = await execute({
    checks: [await existing()], runs: [run(1, { conclusion: 'stale' })],
  });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].conclusion, 'failure');
});
for (const source of ['recent', 'slice']) {
  test('the sweep continues after errors in the ' + source + ' PRs and reports every failure', async () => {
    const broken = ['b', 'c'].map((letter, index) => ({
      ...pr, number: 20 + index, head: { ...pr.head, sha: letter.repeat(40) },
    }));
    const result = await execute({
      recent: [], slice: [], [source]: [...broken, pr], count: 3,
      checkErrors: {
        [broken[0].head.sha]: 'First API failure',
        [broken[1].head.sha]: 'Second API failure',
      },
    });
    assert.equal(result.writes.length, 1);
    assert.equal(result.writes[0].check_run_id, seed.id);
    assert.equal(result.writes[0].conclusion, 'success');
    assert.deepEqual(result.failures, ['#20: First API failure\n#21: Second API failure']);
  });
}
test('a rerun of the same workflow run updates the existing check', async () => {
  const check = await existing(run(1, { conclusion: 'failure' }));
  const { writes } = await execute({ checks: [check], runs: [run(1, { run_attempt: 2 })] });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].check_run_id, 100);
  assert.equal(writes[0].conclusion, 'success');
  assert.equal(JSON.parse(writes[0].output.text).run_attempt, 2);
});
test('a manual run can replace a failed push run', async () => {
  const check = await existing(run(1, { conclusion: 'failure' }));
  const { writes } = await execute({
    checks: [check], runs: [run(2, { event: 'workflow_dispatch' }), run(1)],
  });
  assert.equal(writes[0].details_url, run(2).html_url);
});
test('a deleted run clears its stale success result', async () => {
  const { writes } = await execute({ checks: [await existing()], runs: [] });
  assert.equal(writes[0].method, 'update');
  assert.equal(writes[0].conclusion, 'neutral');
});
test('unchanged results do not consume check write requests', async () => {
  const check = await existing();
  // GitHub can replace details_url with the check's own URL.
  for (const details_url of [check.details_url, 'https://github.com/apache/arrow/runs/100']) {
    assert.deepEqual((await execute({ checks: [{ ...check, details_url }] })).writes, []);
  }
});
test('a same-name check from another app or PR is not overwritten', async () => {
  const check = await existing();
  for (const foreign of [
    { ...check, app: { slug: 'other-app' } },
    { ...check, external_id: 'arrow-fork-ci:20:cpp.yml' },
  ]) {
    assert.equal((await execute({ event: 'workflow_run', checks: [foreign] })).writes[0].method, 'create');
  }
});
test('recent completed checks wait for the rotating sweep', async () => {
  const { writes, calls } = await execute({ checks: [await existing()], slice: [] });
  assert.deepEqual(writes, []);
  assert.equal(calls.filter(([kind]) => kind === 'runs').length, 0);
});
test('late runs are recovered outside the sweep slice', async () => {
  const missing = await execute({ runs: [] });
  const check = { ...await existing(), ...missing.writes[0] };
  const { writes } = await execute({ checks: [check], slice: [] });
  assert.equal(writes[0].conclusion, 'success');
});
for (const status of ['queued', 'in_progress']) {
  test('polls recent ' + status + ' runs outside the sweep slice', async () => {
    const check = await existing(run(1, { status, conclusion: null }));
    const { writes } = await execute({ checks: [check], slice: [] });
    assert.equal(writes[0].conclusion, 'success');
  });
}
test('the sweep reaches old PRs beyond 500 open PRs', async () => {
  const old = { ...pr, updated_at: new Date(now - 24 * 3600 * 1000).toISOString() };
  const { writes, sweepPage } = await execute({ count: 600, recent: [old], slice: [old] });
  assert.equal(sweepPage, Math.floor(now / (15 * 60 * 1000)) % 120 + 1);
  assert.equal(writes.length, 1);
});
test('an active PR appearing in both lists is polled once', async () => {
  const { calls } = await execute();
  assert.equal(calls.filter(([kind]) => kind === 'runs').length, 1);
});
test('no open PRs means no fork API calls', async () => {
  const result = await execute({ count: 0 });
  assert.equal(result.calls.length, 1);
  assert.deepEqual(result.writes, []);
});


test('scheduled polling never initializes checks for unrelated path-filtered PRs', async () => {
  const result = await execute({ checks: [] });
  assert.deepEqual(result.writes, []);
  assert.equal(result.calls.filter(([kind]) => kind === 'runs').length, 0);
});

test('fork feedback explains the lack of merge validation and upstream fallback', async () => {
  const { writes } = await execute();
  assert.match(writes[0].output.summary, /Arrow has no merge queue/);
  assert.match(writes[0].output.summary, /CI: Upstream/);
  assert.match(writes[0].output.summary, /informational/);
});

function evaluate(expression, github) {
  expression = expression.replaceAll('needs.check-enabled', "needs['check-enabled']").replace(/^\$\{\{\s*|\s*\}\}$/g, '')
    .replace('github.event.pull_request.labels.*.name', 'github.event.pull_request.labels.map(label => label.name)');
  return new Function('github', 'contains', 'needs', 'return (' + expression + ');')(
    github, (items, value) => items.includes(value), { 'check-enabled': { outputs: { is_enabled: 'true' } } });
}
function context(extra = {}) {
  return {
    repository: 'apache/arrow', event_name: 'pull_request', base_ref: 'main',
    ref: 'refs/pull/10/merge', sha, workflow: 'C++',
    event: { pull_request: { number: 10, title: 'Fix a C++ bug', labels: [],
      head: { repo: { full_name: 'contributor/arrow' } } } },
    ...extra,
  };
}
function routesToBuilds(ctx) {
  return Object.values(ciWorkflow.jobs).map(job => Boolean(evaluate(job.if, ctx)));
}
test('all regular C++ jobs defer fork PRs to main, and allow the upstream override', () => {
  const ctx = context();
  assert.ok(routesToBuilds(ctx).every(enabled => !enabled));
  ctx.event.pull_request.labels.push({ name: 'CI: Upstream' });
  assert.ok(routesToBuilds(ctx).every(Boolean));
  ctx.event.pull_request.labels = [];
  assert.ok(routesToBuilds(ctx).every(enabled => !enabled));
});
test('fork push/manual, main push, release PRs, and same-repository PRs keep running', () => {
  for (const ctx of [
    context({ repository: 'contributor/arrow', event_name: 'push' }),
    context({ repository: 'contributor/arrow', event_name: 'workflow_dispatch' }),
    context({ event_name: 'push' }),
    context({ base_ref: 'maint-26.0.0' }),
    context({ event: { pull_request: { title: '', labels: [], head: { repo: { full_name: 'apache/arrow' } } } } }),
  ]) assert.ok(routesToBuilds(ctx).every(Boolean));
});
test('WIP suppression remains in effect even with the upstream label', () => {
  const ctx = context();
  ctx.event.pull_request.title = 'WIP fix';
  ctx.event.pull_request.labels = [{ name: 'CI: Upstream' }];
  assert.ok(routesToBuilds(ctx).every(enabled => !enabled));
});
test('PR label changes trigger routing and push path filters remain intact', () => {
  const events = ciWorkflow.on || ciWorkflow.true;
  assert.ok(events.pull_request.types.includes('labeled'));
  assert.deepEqual(events.push.paths, events.pull_request.paths);
  assert.ok(Object.hasOwn(events, 'workflow_dispatch'));
});
function group(ctx) {
  return ciWorkflow.concurrency.group.replace(/\$\{\{\s*(.*?)\s*\}\}/g,
    (_, expression) => evaluate(expression, ctx));
}
test('successive fork pushes and manual runs cancel each other only on the same branch', () => {
  const fork = context({ repository: 'contributor/arrow', event_name: 'push',
    event: { pull_request: { number: '' } }, ref: 'refs/heads/feature' });
  assert.equal(group(fork), group({ ...fork, sha: 'new', event_name: 'workflow_dispatch' }));
  assert.notEqual(group(fork), group({ ...fork, ref: 'refs/heads/other' }));
});
test('different PRs and main commits never cancel each other', () => {
  const pr = context();
  assert.notEqual(group(pr), group({ ...pr, event: { pull_request: { number: 11 } } }));
  const main = context({ event_name: 'push', event: { pull_request: { number: '' } } });
  assert.notEqual(group(main), group({ ...main, sha: 'new' }));
});
test('the privileged bridge never checks out or executes contributor code', () => {
  assert.deepEqual(bridgeWorkflow.permissions, { actions: 'read', checks: 'write', 'pull-requests': 'read' });
  assert.equal(bridgeWorkflow.jobs.bridge.steps.length, 1);
  assert.match(bridgeWorkflow.jobs.bridge.steps[0].uses, /^actions\/github-script@/);
  assert.ok((bridgeWorkflow.on || bridgeWorkflow.true).workflow_run.workflows.includes('C++'));
});

test('all ordinary build workflows follow fork routing and retain manual execution', () => {
  for (const file of ['archery', 'cpp', 'dev', 'docs', 'docs_light', 'integration',
    'matlab', 'python', 'r', 'ruby', 'release_candidate', 'verify_rc']) {
    const config = workflow(file + '.yml');
    const events = config.on || config.true;
    assert.ok(Object.hasOwn(events, 'workflow_dispatch'), file);
    assert.ok(events.pull_request.types.includes('labeled'), file);
    const jobs = file === 'verify_rc' ? [config.jobs.target] : Object.values(config.jobs);
    for (const job of jobs) {
      const ctx = context();
      assert.equal(Boolean(evaluate(job.if, ctx)), false, file);
      ctx.event.pull_request.labels.push({ name: 'CI: Upstream' });
      assert.equal(Boolean(evaluate(job.if, ctx)), true, file);
      assert.equal(Boolean(evaluate(job.if, context({
        repository: 'contributor/arrow', event_name: 'workflow_dispatch',
      }))), true, file);
      assert.equal(Boolean(evaluate(job.if, context({ base_ref: 'maint-26.0.0' }))), true, file);
      assert.equal(Boolean(evaluate(job.if, context({ event_name: 'push' }))), file !== 'docs_light', file);
    }
  }
});

test('requested Extra builds move to forks while specialized hardware stays upstream', () => {
  for (const file of ['cpp_extra', 'r_extra', 'package_linux']) {
    const config = workflow(file + '.yml');
    for (const [name, job] of Object.entries(config.jobs)) {
      if (!job.if?.includes('needs.check-enabled.outputs.is_enabled')) continue;
      const specialized = name === 'large-memory';
      assert.equal(Boolean(evaluate(job.if, context())), specialized, file + '/' + name);
      assert.equal(Boolean(evaluate(job.if, context({
        repository: 'contributor/arrow', event_name: 'workflow_dispatch',
      }))), !specialized, file + '/' + name);
      const upstream = context();
      upstream.event.pull_request.labels.push({ name: 'CI: Upstream' });
      assert.equal(Boolean(evaluate(job.if, upstream)), true, file + '/' + name);
    }
  }
});

test('different workflow completion notifications do not cancel each other', () => {
  const group = (id) => bridgeWorkflow.concurrency.group.replace(/\$\{\{\s*(.*?)\s*\}\}/g,
    (_, expression) => evaluate(expression, context({ event: { workflow_run: {
      head_repository: { full_name: 'contributor/arrow' }, head_branch: 'feature', workflow_id: id,
    } } })));
  assert.notEqual(group(1), group(2));
});

test('similarly named documentation workflows produce separate checks using one run listing', async () => {
  const checks = ['docs.yml', 'docs_light.yml'].map((file, index) => ({
    ...seed, id: 200 + index, external_id: 'arrow-fork-ci:10:' + file,
  }));
  const result = await execute({ checks, runs: [
    run(3, { path: '.github/workflows/docs.yml', conclusion: 'failure' }),
    run(4, { path: '.github/workflows/docs_light.yml' }),
    run(5),
  ] });
  assert.deepEqual(result.writes.map(check => [check.check_run_id, check.conclusion]),
    [[200, 'failure'], [201, 'success']]);
  assert.equal(result.calls.filter(([kind]) => kind === 'runs').length, 1);
  const init = await execute({ event: 'workflow_run', checks: [],
    notification: { path: '.github/workflows/docs_light.yml' }, runs: [
      run(4, { path: '.github/workflows/docs_light.yml' }),
    ],
  });
  assert.equal(init.writes.length, 1);
  assert.equal(init.writes[0].name, 'Docs Light (fork)');
});

test('unrequested Extra workflow notifications do not ask contributors to run them', async () => {
  for (const labels of [[], [{ name: 'CI: Extra: R' }], [{ name: 'CI: Extra: C++' }], [{ name: 'CI: Extra' }]]) {
    const result = await execute({ event: 'workflow_run', checks: [],
      notification: { path: '.github/workflows/cpp_extra.yml' },
      candidates: [{ ...pr, labels }], runs: [run(3, { path: '.github/workflows/cpp_extra.yml' })],
    });
    assert.equal(result.writes.length,
      labels.some(label => ['CI: Extra', 'CI: Extra: C++'].includes(label.name)) ? 1 : 0);
  }
});

test('missing workflows share their listing and unrelated workflow results cannot turn checks green', async () => {
  const result = await execute({ checks: [seed, {
    ...seed, id: 200, external_id: 'arrow-fork-ci:10:python.yml',
  }], runs: [run(10, { path: '.github/workflows/dev.yml' })] });
  assert.deepEqual(result.writes.map(check => check.conclusion), ['neutral', 'action_required']);
  assert.equal(result.calls.filter(([kind]) => kind === 'runs').length, 1);
  assert.equal(result.calls.filter(([kind]) => kind === 'workflows').length, 1);
});

test('an unrelated label cannot cancel upstream override builds already running', () => {
  const running = context();
  running.event.pull_request.labels.push({ name: 'CI: Upstream' });
  const labeled = { ...running, event: { ...running.event, action: 'labeled', label: { name: 'documentation' } } };
  assert.notEqual(group(running), group(labeled));
  assert.ok(routesToBuilds(labeled).every(enabled => !enabled));
});

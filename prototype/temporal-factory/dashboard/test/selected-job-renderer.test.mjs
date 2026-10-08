import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {runInNewContext} from 'node:vm';

const htmlUrl = new URL('../../../../docs/design/exomachina-floor.html', import.meta.url);

async function rendererHarness({dashboardRunId, runId, renderedRunId, selection = {type: 'step', id: 'review'}}) {
  const html = await readFile(htmlUrl, 'utf8');
  const start = html.indexOf('  async function updateDashboardFactory(');
  const end = html.indexOf('  async function loadFactory(', start);
  assert.ok(start >= 0 && end > start, 'updateDashboardFactory is present');

  const aggregate = {id: 'all-jobs', name: 'All jobs', multi: true, live: false};
  const old = {id: 'old-run', name: 'Old run', multi: false, live: false};
  const current = {id: 'new-run', name: 'New run', multi: false, live: false};
  const f = {
    id: 'factory-one',
    steps: [{id: 'work'}],
    edges: [],
    signals: [],
    communication: {humanInbox: {targets: []}},
    version: 'v1',
    digest: 'sha256:test',
    runs: [aggregate, old, current],
  };
  const graphKey = JSON.stringify([f.id, f.steps, f.edges, f.signals, f.communication.humanInbox.targets]);
  const previous = f.runs.find(row => row.id === renderedRunId);
  const S = {
    f,
    R: {run: previous, tMax: 0},
    sel: selection,
    follow: 'old-follow',
    track: true,
    t: 0,
    playing: false,
  };
  const store = {
    dashboardRunId,
    runId,
    sourceMode: 'recorded',
    follow: 'old-follow',
    ready: false,
  };
  const compiled = [];
  const noOp = () => {};
  const context = {
    document: {getElementById: () => ({style: {display: ''}})},
    S,
    store,
    dashboardGraphKey: graphKey,
    compile: (_factory, run) => {
      compiled.push(run);
      return {run, tMax: 10};
    },
    buildItems: noOp,
    buildStamps: noOp,
    setTicks: noOp,
    syncHUD: noOp,
    invalidate: noOp,
    lastAlarmKey: '', lastToastKey: '', lastDecKey: '', lastRailKey: '', lastHudKey: '', lastBoardKey: '',
    lastRailWall: 0, lastHudWall: 0, lastBoardWall: 0, lastCursor: 0, boardCache: null,
  };
  const update = runInNewContext(`(${html.slice(start, end).trim()})`, context);
  await update(f);
  return {S, store, compiled, aggregate, old, current};
}

test('an explicit newly observed job wins over the previous old or aggregate renderer selection', async () => {
  for (const priorRunId of ['old-run', 'all-jobs']) {
    const {S, store, compiled, current} = await rendererHarness({
      dashboardRunId: 'new-run',
      runId: priorRunId,
      renderedRunId: priorRunId,
    });
    assert.equal(S.R.run, current);
    assert.equal(compiled.at(-1), current);
    assert.equal(store.runId, 'new-run');
    assert.equal(S.sel.type, 'run');
    assert.equal(S.sel.id, null);
    assert.equal(S.follow, null);
    assert.equal(S.track, false);
  }
});

test('an explicit all-jobs aggregate remains selectable', async () => {
  const {S, store, compiled, aggregate} = await rendererHarness({
    dashboardRunId: 'all-jobs',
    runId: 'old-run',
    renderedRunId: 'old-run',
  });
  assert.equal(S.R.run, aggregate);
  assert.equal(compiled.at(-1), aggregate);
  assert.equal(store.runId, 'all-jobs');
  assert.equal(store.multi, true);
  assert.equal(S.sel.type, 'factory');
  assert.equal(S.sel.id, null);
});

test('a missing explicit job selection falls back to the unchanged existing renderer selection', async () => {
  const {S, store, compiled, old} = await rendererHarness({
    dashboardRunId: 'no-longer-present',
    runId: 'old-run',
    renderedRunId: 'old-run',
    selection: {type: 'step', id: 'review'},
  });
  assert.equal(S.R.run, old);
  assert.equal(compiled.at(-1), old);
  assert.equal(store.runId, 'old-run');
  assert.equal(S.sel.type, 'step');
  assert.equal(S.sel.id, 'review');
  assert.equal(S.follow, 'old-follow');
  assert.equal(S.track, true);
});

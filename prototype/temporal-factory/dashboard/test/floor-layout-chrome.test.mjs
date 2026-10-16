import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {runInNewContext} from 'node:vm';

const html = await readFile(new URL('../../../../docs/design/exomachina-floor.html', import.meta.url), 'utf8');
const slice = (from, to) => {
  const start = html.indexOf(from), end = html.indexOf(to, start + from.length);
  assert.ok(start >= 0 && end > start, `${from.trim()} is present`);
  return html.slice(start, end).trim();
};

function updateHarness({frameNext}) {
  const f = {id: 'factory-one', steps: [{id: 'work'}], edges: [], signals: [], communication: {humanInbox: {targets: []}}, version: 'v1', digest: 'd', runs: [{id: 'run-one', name: 'Run one', multi: false, live: false}]};
  const S = {f, R: {run: f.runs[0], tMax: 0}, sel: {type: 'run', id: null}, t: 0, playing: false, frameNext};
  const store = {dashboardRunId: 'run-one', runId: 'run-one', sourceMode: 'demo', ready: false};
  const views = [];
  const noOp = () => {};
  const update = runInNewContext(`(${slice('  async function updateDashboardFactory(', '  async function loadFactory(')})`, {
    document: {getElementById: () => ({style: {display: ''}})}, S, store,
    dashboardGraphKey: JSON.stringify([f.id, f.steps, f.edges, f.signals, f.communication.humanInbox.targets]),
    compile: (_factory, run) => ({run, tMax: 10}), buildItems: noOp, buildStamps: noOp, setTicks: noOp, syncHUD: noOp, invalidate: noOp,
    initialView: () => views.push(store.ready),
  });
  return {update, f, S, views};
}

test('a source switch frames the next rendered factory once, after it is ready, even when the graph is unchanged', async () => {
  const {update, f, S, views} = updateHarness({frameNext: true});
  await update(f);
  assert.deepEqual(views, [true], 'first view is computed after the run rail and HUD are shown');
  assert.equal(S.frameNext, false);
  await update(f);
  assert.deepEqual(views, [true], 'a later data update keeps the viewer camera');
  const unchanged = updateHarness({frameNext: false});
  await unchanged.update(unchanged.f);
  assert.deepEqual(unchanged.views, [], 'ordinary updates never reframe');
  assert.match(html, /resetDashboardSource\(source\)\{dashboardPresentationSource=source;S\.frameNext=true;/);
  assert.match(slice('  async function loadDashboardFactory(', '  async function updateDashboardFactory('), /S\.frameNext = false; initialView\(\);/);
});

test('stage resizes keep the automatic first view but never move a camera the viewer took over', () => {
  const calls = [];
  const S = {L: {}, camAuto: 'fit', track: false, focusedDirector: false};
  const reframe = runInNewContext(`(${slice('  function reframe(', '\n')})`, {
    S, gsap: {isTweening: () => false}, cam: {},
    fit: animate => calls.push(['fit', animate]), initialView: () => calls.push(['initial']),
  });
  reframe();
  S.camAuto = 'initial'; reframe();
  S.camAuto = false; reframe();
  S.camAuto = 'fit'; S.track = true; reframe();
  assert.deepEqual(calls, [['fit', false], ['initial']]);
  assert.match(html, /new ResizeObserver\(\(\) => \{ if \(!store\.ready\) return; invalidate\(3\); reframe\(\); \}\)\.observe\(stageEl\)/);
  // Every manual camera gesture hands the camera to the viewer.
  for (const gesture of [
    /moved = true; S\.camAuto = false; gsap\.killTweensOf\(cam\); stopTrack\(\);/,
    /pinch0\)\{ moved = true; S\.camAuto = false;/,
    /e\.preventDefault\(\); S\.camAuto = false; gsap\.killTweensOf\(cam\); stopTrack\(\); const r = stageEl/,
    /function zoom\(dir\)\{ S\.camAuto = false;/,
    /S\.focusedDirector = true; S\.camAuto = false;/,
  ]) assert.match(html, gesture);
  assert.match(slice('  function initialView(', '  function reframe('), /if \(fitScale\(\) >= \.34\) return fit\(false\);/);
});

test('group labels stack their visual/graph qualifier and are de-collided with route labels as label size changes', () => {
  const overlay = slice('  function buildOverlay(', '  function declutterLabels(');
  assert.match(overlay, /<span class="zq"><span class="sr-only"> · <\/span>/, 'the visual-group qualifier stays in the label text');
  assert.match(overlay, /S\.labelsDirty = true;/);
  const declutter = slice('  function declutterLabels(', '  function buildStamps(');
  assert.match(declutter, /querySelectorAll\('\.zlabel, \[data-communication-route\]'\)/);
  assert.doesNotMatch(declutter, /\.mach[^,]*\{|layoutFactory|G\.edges/, 'only presentation labels move; machines and wires stay pinned');
  assert.match(html, /if \(relabel\) declutterLabels\(\);/);
  assert.match(html, /\.zlabel \.zq\{display:block;/);
});

test('source chrome shows one styled status and single-line job chips', () => {
  assert.match(html, /\.topbar\.has-source > \.mockchip\{display:none\}/);
  assert.match(html, /topbar\.classList\.add\('has-source'\)/);
  assert.match(html, /\.exo-source select\{appearance:none;/);
  assert.match(html, /#exo-source-status\{[^}]*border:1px dashed var\(--ink-3\)/);
  assert.doesNotMatch(html, /runSelect\.style\.maxWidth=/);
  assert.match(html, /\.ticket \.jn\{[^}]*white-space:nowrap;overflow:hidden;text-overflow:ellipsis\}/);
});

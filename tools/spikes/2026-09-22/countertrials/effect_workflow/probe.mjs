import fs from 'node:fs';
import path from 'node:path';
import { ClusterWorkflowEngine, SingleRunner } from '@effect/cluster';
import { SqliteClient } from '@effect/sql-sqlite-node';
import { Activity, DurableDeferred, Workflow } from '@effect/workflow';
import { Effect, Layer, Schema } from 'effect';

const root = process.env.EFFECT_SPIKE_ROOT ?? process.cwd();
const database = path.join(root, 'state.sqlite');
const events = path.join(root, 'events.jsonl');
const mode = process.argv[2] ?? 'start';
const id = process.argv[3] ?? 'trial-1';

const record = (phase, value) => Effect.sync(() => {
  fs.appendFileSync(events, JSON.stringify({ phase, value, at: Date.now(), pid: process.pid }) + '\n');
  return value;
});

const Approval = DurableDeferred.make('review-approval', { success: Schema.String });
const Factory = Workflow.make({
  name: 'FactoryV1',
  payload: { id: Schema.String },
  idempotencyKey: ({ id }) => id,
  success: Schema.String,
});
const FactoryLive = Factory.toLayer(({ id }) => Effect.gen(function* () {
  yield* Activity.make({ name: 'prepare', success: Schema.String, execute: record('prepare', id) });
  const decision = yield* DurableDeferred.await(Approval);
  yield* Activity.make({ name: 'deliver', success: Schema.String, execute: record('deliver', `${id}:${decision}`) });
  return decision;
}));

const Sqlite = SqliteClient.layer({ filename: database });
const Runner = SingleRunner.layer({ runnerStorage: 'sql' }).pipe(Layer.provideMerge(Sqlite));
const Engine = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(Runner));
const App = FactoryLive.pipe(Layer.provideMerge(Engine));

const main = Effect.gen(function* () {
  if (mode === 'start' || mode === 'hold') {
    const executionId = yield* Factory.execute({ id }, { discard: true });
    console.log(JSON.stringify({ event: 'started', id, executionId, pid: process.pid }));
    for (let i = 0; i < 30; i++) {
      const result = yield* Factory.poll(executionId);
      if (result?._tag === 'Suspended') {
        console.log(JSON.stringify({ event: 'suspended', result: result._tag, pid: process.pid }));
        break;
      }
      yield* Effect.sleep('100 millis');
    }
    if (mode === 'hold') yield* Effect.never;
  } else if (mode === 'approve') {
    const token = yield* DurableDeferred.tokenFromPayload(Approval, { workflow: Factory, payload: { id } });
    yield* DurableDeferred.succeed(Approval, { token, value: 'accepted' });
    console.log(JSON.stringify({ event: 'approved', id, pid: process.pid }));
    const executionId = yield* Factory.executionId({ id });
    for (let i = 0; i < 120; i++) {
      const result = yield* Factory.poll(executionId);
      if (result?._tag === 'Complete') {
        console.log(JSON.stringify({ event: 'complete', result: result._tag, exit: result.exit._tag, pid: process.pid }));
        return;
      }
      yield* Effect.sleep('100 millis');
    }
    console.log(JSON.stringify({ event: 'not-complete-after-12s', pid: process.pid }));
    process.exitCode = 2;
  } else if (mode === 'idle') {
    console.log(JSON.stringify({ event: 'ready', pid: process.pid }));
    yield* Effect.never;
  } else {
    throw new Error(`Unknown mode ${mode}`);
  }
});

Effect.runPromise(main.pipe(Effect.provide(App))).catch(error => {
  console.error(error);
  process.exitCode = 1;
});

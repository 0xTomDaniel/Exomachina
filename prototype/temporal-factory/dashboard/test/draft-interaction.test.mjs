import test from "node:test";
import assert from "node:assert/strict";
import {briefInputPolicy, createDraftStore} from "../draft.mjs";

test("Live and Demo remain editable while only submission is disabled",()=>{
  for(const source of ["live","demo"])for(const reason of ["", "Unauthenticated", "Waiting for fresh snapshot", "Provider unavailable"]){
    const policy=briefInputPolicy(source,reason);
    assert.equal(policy.readOnly,false);assert.equal(policy.disabled,false);
    assert.equal(policy.submitDisabled,!!reason);assert.equal(policy.reason,reason);
  }
});
test("Recorded is focusable but read-only even if submission readiness is present",()=>{
  assert.deepEqual(briefInputPolicy("recorded"),{readOnly:true,disabled:false,submitDisabled:true,reason:"Recorded evidence is read-only."});
});
test("drafts retain edits independently per source and factory without persistence or submission",()=>{
  const store=createDraftStore(),live={source:"live",factoryId:"factory-one"},demo={source:"demo",factoryId:"factory-one"},other={source:"live",factoryId:"factory-two"};
  store.set(live,"qa-live keyboard paste");store.set(demo,"qa-demo");store.set(other,"qa-other");
  assert.equal(store.get(live),"qa-live keyboard paste");assert.equal(store.get(demo),"qa-demo");assert.equal(store.get(other),"qa-other");
  assert.equal(store.get({source:"recorded",factoryId:"factory-one"}),"");
  assert.throws(()=>store.set(live,"x".repeat(4001)),/bounded/);
  assert.equal(store.get(live),"qa-live keyboard paste");
});

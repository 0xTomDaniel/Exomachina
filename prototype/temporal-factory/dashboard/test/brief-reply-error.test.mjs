import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {runInNewContext} from 'node:vm';

const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
const start=html.indexOf('  const DIRECTOR_FAILURE_TEXT='),end=html.indexOf('  const report=text=>',start);
const briefReplyError=runInNewContext(html.slice(start,end)+'\nbriefReplyError',{});

test('a failed Director model call reads as no job started, not a refusal',()=>{
 const parts=[{kind:'data',data:{error:'Director issued no accepted command',director_turn:{accepted:[],failure:'RuntimeError',model_calls:1,tool_calls:0}}}];
 const text=briefReplyError(parts);
 assert.match(text,/^No job started: the Director could not act because the model provider returned an error \(RuntimeError\)/);
 assert.match(briefReplyError([{data:{error:'x',director_turn:{accepted:[],failure:'SubscriptionQuotaExhausted'}}}]),/usage limit/);
});

test('other factory errors pass through and success has no error',()=>{
 assert.equal(briefReplyError([{data:{error:'factory permits one active job; another original Task is busy'}}]),'factory permits one active job; another original Task is busy');
 assert.equal(briefReplyError([{data:{accepted_command:{op:'start'}}}]),null);
 assert.equal(briefReplyError(undefined),null);
});

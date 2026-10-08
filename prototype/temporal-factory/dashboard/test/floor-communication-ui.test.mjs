import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
const html=await readFile(new URL('../../../../docs/design/exomachina-floor.html',import.meta.url),'utf8');
test('shared floor always exposes inbox and independent incident view navigation',()=>{
 assert.ok(html.includes("db.dataset.humanInbox='';"));
 assert.ok(html.includes("channelButton.dataset.incidentChannel='';"));
 assert.ok(html.includes('grow(G.incidentView.x, G.incidentView.y)'));
 assert.ok(html.includes('grow(G.incidentView.x+G.incidentView.w, G.incidentView.y+G.incidentView.h+24)'));
 assert.ok(html.includes("window.EXO_DASHBOARD_OPEN_DECISIONS?.('human')"));
 assert.ok(html.includes("window.EXO_DASHBOARD_OPEN_DECISIONS?.('incidents')"));
 assert.ok(!html.includes('if (mine.length){'));
 assert.ok(html.includes('No observed pending human items.'));
 assert.ok(html.includes('.app > .stage{grid-row:3}'));
 assert.ok(html.includes('.app > .transport{grid-row:4}'));
 assert.ok(html.includes('Origin ${row.node??\'unreported\'}'));
});
test('navigation and declared references are distinct from pinned work edges',()=>{
 assert.ok(html.includes("linkLabel.dataset.communicationRoute='presentation_navigation'"));
 assert.ok(html.includes("label.dataset.communicationRoute='declared_human'"));
 assert.ok(html.includes('View link · no message'));
 assert.ok(html.includes('Dashed communication reference; not an observed message or work belt.'));
 assert.ok(html.includes('Incident recovery and undeclared human escalation are unavailable.'));
});

test('local session recovery uses only the server-declared loopback login route',()=>{
 assert.ok(html.includes("if(bootstrap.qaSessionLogin==='/qa/login')"));
 assert.ok(html.includes("login.href='/qa/login'"));
 assert.ok(html.includes("login.textContent='Open local session'"));
 assert.ok(html.includes("if(loginLink)loginLink.hidden=mode!=='live'"));
});

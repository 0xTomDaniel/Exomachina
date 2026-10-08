import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { runInNewContext } from "node:vm";

// Carrier gems on the Floor: the pure helpers are plain code inside the page script;
// evaluate that slice and exercise them directly.
const html = await readFile(new URL("../../../../docs/design/exomachina-floor.html", import.meta.url), "utf8");
const start = html.indexOf("  /* ---------- carriers and gems ----------"), end = html.indexOf("  /* end carriers and gems helpers */", start);
assert.ok(start > 0 && end > start, "gem helper block is present");
const vm = runInNewContext(`${html.slice(start, end)}\n({ carrierGems, carrierRows, gemSpots, gemOfKinds, GEM_CUT, GEM_MAX })`, { Infinity, Math, String });
// Results cross the vm realm; compare them as plain data.
const plain = fn => (...args) => { const out = fn(...args); return out == null ? out : JSON.parse(JSON.stringify(out)); };
const gems = { ...vm, carrierGems:plain(vm.carrierGems), carrierRows:plain(vm.carrierRows), gemSpots:plain(vm.gemSpots) };
const recorded = (n, ready = i => 10 + i) => ({ evidence:"recorded", sockets:n, digest_match:true, items:Array.from({ length:n }, (_, i) => ({ index:i, source:"artifact", part_kinds:[["text", "data", "raw", "url"][i % 4]], media_type:"text/plain", byte_length:2048, digest:"abcdef012345", ready_at:"2026-10-07T12:00:10.000Z", ready_t:ready(i), gem:["sapphire", "emerald", "amethyst", "topaz"][i % 4], evidence:"recorded" })) });

test("cut is the primary signal: each content type has its own cut", () => {
  assert.deepEqual({ ...gems.GEM_CUT }, { sapphire:"round", emerald:"square", amethyst:"marquise", topaz:"triangle", diamond:"diamond", unknown:"rough" });
  assert.equal(new Set(Object.values(gems.GEM_CUT)).size, 6);
  assert.equal(gems.gemOfKinds("artifact", ["data", "text"]), "emerald", "mixed parts use the first part kind");
  assert.equal(gems.gemOfKinds("message", ["text"]), "diamond");
});

test("a filling carrier shows dark empty sockets, then each gem drops in with a brief sparkle", () => {
  const c = { ...recorded(3), filling:true, sockets:4 };
  const before = gems.carrierGems(c, 9.5, false);
  assert.equal(before.sockets.length, 4);
  assert.ok(before.sockets.every(s => !s.filled), "all sockets are empty before the first ready time");
  const drop = gems.carrierGems(c, 11.1, false);
  assert.deepEqual(drop.sockets.map(s => s.filled), [true, true, false, false]);
  assert.ok(drop.sockets[1].sparkle > 0.7 && drop.sockets[0].sparkle === 0, "only the gem that just dropped sparkles");
  assert.equal(drop.quality, "flawless");
});

test("more than four items show four gems and +N; low zoom collapses the gems to the badge", () => {
  const c = recorded(7);
  const near = gems.carrierGems(c, 100, false);
  assert.equal(near.sockets.length, gems.GEM_MAX); assert.equal(near.more, 3); assert.equal(near.badge, "+3");
  assert.equal(gems.carrierGems(recorded(4), 100, false).badge, "", "exactly four needs no badge");
  const far = gems.carrierGems(c, 100, true);
  assert.equal(far.sockets.length, 0); assert.equal(far.badge, "7");
  assert.equal(gems.carrierGems(c, 12.5, true).badge, "3/7", "a filling carrier at low zoom shows ready over total");
});

test("evidence is gem quality: inferred carriers show one cloudy socket, mismatched digests chip the gems", () => {
  const inferred = gems.carrierGems({ evidence:"inferred", sockets:1, items:[] }, 0, false);
  assert.deepEqual(inferred.sockets.map(s => [s.gem, s.filled]), [["unknown", true]]);
  assert.equal(inferred.quality, "inferred");
  assert.equal(gems.carrierGems({ ...recorded(1), digest_match:false }, 50, false).quality, "mismatch");
  assert.equal(gems.carrierGems({ evidence:"illustrative", sockets:1, items:[{ gem:"emerald", ready_t:0 }] }, 1, false).quality, "illustrative");
  assert.equal(gems.carrierGems(null, 0, false), null);
});

test("the inspector lists only allowlisted, content-free item fields", () => {
  const c = recorded(2);
  // Content that somehow reached a carrier is never read by the inspector.
  Object.assign(c.items[0], { text:"SECRET-TEXT", name:"secret.md", url:"https://secret.example", metadata:{ k:"SECRET" }, description:"SECRET", artifactId:"SECRET-ID", filename:"secret.md", data:{ SECRET:1 } });
  Object.assign(c, { text:"SECRET-TEXT", metadata:{ SECRET:1 }, handoff_id:"h-1", revision:2, consumers:[{ node:"quality", consumed_at:"2026-10-07T12:00:20.000Z", digest_match:true }] });
  const rows = gems.carrierRows(c), text = JSON.stringify(rows);
  assert.equal(/SECRET|secret\.md/.test(text), false);
  assert.match(text, /Sapphire, round · text · artifact · text · text\/plain · 2\.0 KB · digest abcdef012345… · ready 2026-10-07T12:00:10\.000Z/);
  assert.match(text, /Recorded hand-off h-1 · revision 2 · 2 items/);
  assert.match(text, /Flawless/);
  assert.deepEqual(gems.carrierRows({ evidence:"inferred", sockets:1, items:[] }), [["Gems", "One cloudy socket · contents not recorded; hand-off inferred from pinned graph order"]]);
  assert.match(JSON.stringify(gems.carrierRows({ evidence:"illustrative", sockets:1, items:[{ gem:"emerald", text:"SECRET" }] })), /Illustrative Demo gems[^S]*Emerald, square · data"\]\]$/);
});

test("sockets sit on the rim of every carrier shape, clear of the centre label", () => {
  for (const shape of ["circle", "square", "triangle", "hexagon", "pentagon", "capsule"]) {
    const spots = gems.gemSpots(shape, 4);
    assert.equal(spots.length, 4);
    assert.ok(spots.every(p => Math.hypot(p.x, p.y) >= 8), `${shape} sockets are on the rim`);
    for (let i = 1; i < 4; i++) assert.ok(Math.hypot(spots[i].x - spots[i - 1].x, spots[i].y - spots[i - 1].y) >= 8, `${shape} sockets do not overlap`);
  }
});

test("control edges are thin lines without chevrons or items, and gem facets are cached textures", () => {
  assert.ok(html.includes("if (e.control){ const pts = e.path.pts;"), "control edges draw as a thin line");
  assert.ok(html.includes("if (!e.control && (activeEdges.has(e.id)||activityBelts.includes(e)))"), "control edges never animate as belts");
  assert.ok(html.includes("control:e.kind === 'control'"));
  assert.ok(html.includes("if (gemCache.has(key)) return gemCache.get(key);"), "gem textures are cached per cut and quality");
  assert.ok(html.includes("textures = []; gemCache = new Map();"), "theme rebuild drops cached gem textures");
  assert.ok(html.includes("case 'verdict': R.verdicts.push(e); if (it) it.seals.push("), "gate verdicts seal the item they name");
});

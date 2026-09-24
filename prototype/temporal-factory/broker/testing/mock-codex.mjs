// Test-only loopback Codex Responses SSE fixture. All reasoning and tokens are synthetic.
import http from "node:http";
import fs from "node:fs";
import zlib from "node:zlib";

function options(argv) {
	if (argv.length === 1 && argv[0] === "--self-check") return { selfCheck: true };
	const opts = {};
	for (let i = 0; i < argv.length; i += 2) {
		if (!argv[i]?.startsWith("--") || !argv[i + 1]) throw new Error("usage: mock-codex.mjs --port P --record FILE --script authoring|interleaved|runaway|director [--draft FILE] [--hold-ms N]");
		opts[argv[i].slice(2)] = argv[i + 1];
	}
	const port = Number(opts.port);
	if (!Number.isInteger(port) || !(opts.script === "director" ? port >= 46300 && port <= 46349 : port >= 46120 && port <= 46149) || !opts.record || !["authoring", "interleaved", "runaway", "director"].includes(opts.script) || (opts.script === "authoring" && !opts.draft) || (opts["hold-ms"] !== undefined && (!Number.isSafeInteger(Number(opts["hold-ms"])) || Number(opts["hold-ms"]) < 0 || opts.script !== "runaway"))) throw new Error("invalid fixture options");
	return opts;
}
const opts = options(process.argv.slice(2));
const PORT = Number(opts.port);
const LOG = opts.record;
const FIRST_DRAFT = opts.draft ? JSON.parse(fs.readFileSync(opts.draft, "utf8")) : null;
const HOLD_MS = Number(opts["hold-ms"] ?? 0);
const emitted = new Map(); // prompt_cache_key -> ordered response segments
let seq = 0, dirStarts = 0, holds = 0;
const log = (o) => fs.appendFileSync(LOG, JSON.stringify({ t: Date.now(), ...o }) + "\n");
const frag = (s) => { const a = Math.floor(s.length / 3), b = Math.floor((2 * s.length) / 3); return [s.slice(0, a), s.slice(a, b), s.slice(b)]; };

function check(body, headers) {
	const p = [];
	if (headers["session-id"] && body.prompt_cache_key && headers["session-id"] !== body.prompt_cache_key) p.push("session-id/prompt_cache_key mismatch");
	if (headers.originator !== "exomachina") p.push(`originator=${headers.originator}`);
	if (!/^Bearer .+\..+\..+$/.test(headers.authorization || "")) p.push("bearer");
	if (headers["chatgpt-account-id"] !== "acct_synthetic") p.push("account");
	if (body.store !== false || body.stream !== true) p.push("store/stream");
	if (!body.include?.includes("reasoning.encrypted_content")) p.push("include");
	const input = body.input || [];
	if (input.length < 1 || input[0]?.role !== "user") p.push("missing original user turn");
	const learnedOutputs = [];
	let cursor = 1; // first input is the original user turn
	for (const r of emitted.get(headers["session-id"] ?? body.prompt_cache_key) || []) {
		const remaining = input.slice(cursor);
		if (!remaining.length) { p.push(`${r.rid}: missing history segment`); break; }
		const thinking = remaining[0];
		if (thinking?.type !== "reasoning" || thinking.id !== r.reasoning.id || thinking.encrypted_content !== r.reasoning.enc) {
			const misplaced = remaining.findIndex((i) => i.type === "reasoning" && i.id === r.reasoning.id && i.encrypted_content === r.reasoning.enc);
			p.push(misplaced > 0 ? `${r.rid}: reasoning replayed after its calls` : `${r.rid}: encrypted reasoning ${r.reasoning.id} not replayed`);
			break;
		}
		cursor++;
		for (const c of r.calls) {
			const item = input[cursor++];
			if (item?.type !== "function_call" || item.id !== c.id || item.call_id !== c.call_id || item.name !== c.name || item.arguments !== c.arguments) {
				p.push(`${r.rid}: tool identity/arguments/order not replayed exactly`);
				break;
			}
		}
		if (p.length) break;
		for (const c of r.calls) {
			const item = input[cursor++];
			if (item?.type !== "function_call_output" || item.call_id !== c.call_id) {
				p.push(`${r.rid}: tool output/order not replayed exactly`);
				break;
			}
			if (c.output !== undefined && item.output !== c.output) {
				p.push(`${r.rid}: tool output body changed on replay`);
				break;
			}
			learnedOutputs.push([c, item.output]);
		}
		if (p.length) break;
	}
	const tail = input.slice(cursor);
	if (!p.length && (tail.length > 1 || tail.some((i) => i.role !== "user"))) p.push("unexpected extra history item");
	if (!p.length) for (const [call, output] of learnedOutputs) call.output = output;
	return p;
}

function turn(res, model, session, reasoningId, calls, text) {
	const rid = `resp_${++seq}`, enc = `ENC_${reasoningId}_${seq}`;
	const reasoning = { type: "reasoning", id: reasoningId, summary: [{ type: "summary_text", text: `plan ${reasoningId}` }] };
	const items = [];
	const ev = [{ type: "response.created", response: { id: rid, object: "response", model, status: "in_progress", output: [] } }];
	if (reasoningId) {
		ev.push({ type: "response.output_item.added", output_index: 0, item: { ...reasoning, summary: [] } });
		ev.push({ type: "response.reasoning_summary_text.delta", item_id: reasoningId, output_index: 0, summary_index: 0, delta: `plan ${reasoningId}` });
		ev.push({ type: "response.output_item.done", output_index: 0, item: reasoning }); // NO encrypted_content here
		items.push({ ...reasoning, encrypted_content: enc });
	}
	const record = { reasoning: { id: reasoningId, enc }, calls: [] };
	const streams = [];
	for (const c of calls) {
		const idx = items.length, id = `fc_${c.call_id}`, args = JSON.stringify(c.args);
		const done = { type: "function_call", id, call_id: c.call_id, name: c.name, arguments: args, status: "completed" };
		ev.push({ type: "response.output_item.added", output_index: idx, item: { ...done, arguments: "", status: "in_progress" } });
		streams.push({ idx, id, args, done, deltas: frag(args) });
		items.push(done); record.calls.push({ call_id: c.call_id, id, name: c.name, arguments: args });
	}
	// Start every call first, then alternate fragments across calls before ending them.
	for (let part = 0; part < 3; part++) for (const s of streams) {
		ev.push({ type: "response.function_call_arguments.delta", item_id: s.id, output_index: s.idx, delta: s.deltas[part] });
	}
	for (const s of streams) {
		ev.push({ type: "response.function_call_arguments.done", item_id: s.id, output_index: s.idx, arguments: s.args });
		ev.push({ type: "response.output_item.done", output_index: s.idx, item: s.done });
	}
	if (text !== undefined) {
		const idx = items.length, msg = { type: "message", id: `msg_${seq}`, role: "assistant", status: "completed", content: [{ type: "output_text", text, annotations: [] }] };
		ev.push({ type: "response.output_item.added", output_index: idx, item: { ...msg, status: "in_progress", content: [] } });
		ev.push({ type: "response.content_part.added", item_id: msg.id, output_index: idx, content_index: 0, part: { type: "output_text", text: "", annotations: [] } });
		ev.push({ type: "response.output_text.delta", item_id: msg.id, output_index: idx, content_index: 0, delta: text });
		ev.push({ type: "response.output_item.done", output_index: idx, item: msg });
		items.push(msg);
	}
	ev.push({ type: "response.completed", response: { id: rid, object: "response", model, status: "completed", output: items, usage: { input_tokens: 40, output_tokens: 12, total_tokens: 52, input_tokens_details: { cached_tokens: 0 }, output_tokens_details: { reasoning_tokens: 3 } } } });
	if (calls.length) {
		record.rid = rid;
		emitted.set(session, [...(emitted.get(session) || []), record]);
	}
	if (!res.headersSent) res.writeHead(200, { "content-type": "text/event-stream" });
	for (const e of ev) res.write(`event: ${e.type}\ndata: ${JSON.stringify(e)}\n\n`);
	res.end();
	return { rid, calls: calls.map((c) => c.call_id), reasoning: reasoningId, text,
		fragments: ev.filter((e) => e.type === "response.function_call_arguments.delta").map((e) => e.item_id) };
}

const outputOf = (input, name) => {
	const call = [...input].reverse().find((i) => i.type === "function_call" && i.name === name);
	const out = call && input.find((i) => i.type === "function_call_output" && i.call_id === call.call_id);
	return out && JSON.parse(out.output);
};

function reject(res, base, reason) {
	log({ kind: "rejected", ...base, problems: [reason] });
	res.writeHead(400, { "content-type": "application/json" });
	res.end(JSON.stringify({ error: { message: reason } }));
}

function selfCheck() {
	const session = "fixture-self-check";
	const calls = [
		{ type: "function_call", id: "fc_a", call_id: "a", name: "describe_vocabulary", arguments: "{}" },
		{ type: "function_call", id: "fc_b", call_id: "b", name: "validate_draft", arguments: '{"template_json":"draft"}' },
	];
	emitted.set(session, [{ rid: "resp_1", reasoning: { id: "rs_1", enc: "ENC_rs_1" }, calls: calls.map((c, i) => ({ ...c, output: `result_${i}` })) }]);
	const user = { role: "user", content: [{ type: "input_text", text: "fixture" }] };
	const reasoning = { type: "reasoning", id: "rs_1", encrypted_content: "ENC_rs_1" };
	const outputs = calls.map((c, i) => ({ type: "function_call_output", call_id: c.call_id, output: `result_${i}` }));
	const good = [user, reasoning, ...calls, ...outputs];
	const body = (input) => ({ prompt_cache_key: session, store: false, stream: true, include: ["reasoning.encrypted_content"], input });
	const headers = { originator: "exomachina", authorization: "Bearer a.b.c", "chatgpt-account-id": "acct_synthetic", "session-id": session };
	const cases = [
		["drop_reasoning", [user, ...calls, ...outputs], "resp_1: encrypted reasoning rs_1 not replayed"],
		["reasoning_after_calls", [user, ...calls, reasoning, ...outputs], "resp_1: reasoning replayed after its calls"],
		["drop_segment", [user], "resp_1: missing history segment"],
		["permute_calls", [user, reasoning, calls[1], calls[0], ...outputs], "resp_1: tool identity/arguments/order not replayed exactly"],
		["drop_output", good.slice(0, -1), "resp_1: tool output/order not replayed exactly"],
		["change_output", [...good.slice(0, -1), { ...outputs[1], output: "changed" }], "resp_1: tool output body changed on replay"],
	];
	if (check(body(good), headers).length) throw new Error("valid replay rejected");
	const other = { ...headers, "session-id": "other-session" };
	if (check({ ...body([user]), prompt_cache_key: "other-session" }, other).length) throw new Error("independent session rejected");
	if (check({ ...body(good), prompt_cache_key: "other-session" }, other)[0] !== "unexpected extra history item") throw new Error("cross-session history accepted");
	for (const [name, input, expected] of cases) {
		const actual = check(body(input), headers);
		if (actual.length !== 1 || actual[0] !== expected) throw new Error(`${name}: expected ${expected}; got ${actual.join("; ")}`);
	}
	console.log(JSON.stringify({ passed: true, controls: cases.map(([name, , reason]) => ({ name, reason })) }));
}

const server = http.createServer((req, res) => {
	const chunks = [];
	req.on("data", (c) => chunks.push(c));
	req.on("end", () => {
		let raw = Buffer.concat(chunks);
		if ((req.headers["content-encoding"] || "").includes("zstd")) raw = zlib.zstdDecompressSync(raw);
		let body;
		try { body = JSON.parse(raw.toString()); } catch { return reject(res, {}, "invalid JSON request body"); }
		const headers = {
			originator: req.headers.originator ?? null,
			"user-agent": req.headers["user-agent"] ?? null,
			"chatgpt-account-id": req.headers["chatgpt-account-id"] ?? null,
			"session-id": req.headers["session-id"] ?? null,
			authorization: req.headers.authorization ? "present" : "absent",
		};
		log({ kind: "request", method: req.method, url: req.url, headers, body });
		const tools = (body.tools || []).map((t) => t.name);
		const input = body.input || [];
		const session = req.headers["session-id"] ?? body.prompt_cache_key;
		const problems = check(body, req.headers);
		const base = { originator: req.headers.originator, ua: req.headers["user-agent"], session_id: session, tools, input_types: input.map((i) => i.type || `role:${i.role}`), replayed_reasoning: input.filter((i) => i.type === "reasoning").map((i) => i.id), call_ids: input.filter((i) => i.type === "function_call").map((i) => i.call_id) };
		if (problems.length) return reject(res, base, problems.join("; "));
		const hasOut = input.some((i) => i.type === "function_call_output");
		if (opts.script === "director") {
			if (!["start_research", "inspect_run", "decide_wait"].every((name) => tools.includes(name))) return reject(res, base, "Director tools missing");
			const brief = input[0]?.content?.map((c) => c.text || "").join("") || "";
			const inspected = outputOf(input, "inspect_run");
			const started = outputOf(input, "start_research");
			const decided = outputOf(input, "decide_wait");
			if (decided || started) return log({ kind: "director", ...base, reply: turn(res, body.model, session, null, [], "Director tool response received") });
			if (inspected) {
				const run = inspected.run || {};
				return log({ kind: "director", ...base, reply: turn(res, body.model, session, `rs_director_${seq + 1}`, [{ call_id: `call_decide_${seq + 1}`, name: "decide_wait", args: { action: "abort", revision: run.current_revision || "", sha256: run.current_sha256 || "", rationale: "repair exhausted" } }]) });
			}
			if (/answer the director wait|abort the waiting run/i.test(brief)) return log({ kind: "director", ...base, reply: turn(res, body.model, session, `rs_director_${seq + 1}`, [{ call_id: `call_inspect_${seq + 1}`, name: "inspect_run", args: {} }]) });
			const question = brief.match(/Research (.*?);/i)?.[1] || brief;
			const outcome_mode = brief.match(/outcome_mode\s*[:=]\s*["']?([a-z_]+)/i)?.[1] || "never";
			return log({ kind: "director", ...base, reply: turn(res, body.model, session, `rs_director_${seq + 1}`, [{ call_id: `call_start_${seq + 1}`, name: "start_research", args: { question, outcome_mode } }]) });
		}
		if (opts.script === "runaway") {
			if (!tools.includes("describe_vocabulary") || !tools.includes("validate_draft")) return reject(res, base, "runaway authoring tools missing");
			const lastOutput = [...input].reverse().find((i) => i.type === "function_call_output");
			const lastCall = lastOutput && input.find((i) => i.type === "function_call" && i.call_id === lastOutput.call_id);
			const name = lastCall?.name === "describe_vocabulary" ? "validate_draft" : "describe_vocabulary";
			res.once("close", () => log({ kind: "stream", script: "runaway", session, aborted: !res.writableEnded }));
			const reply = () => {
				if (res.destroyed) return;
				const call = { call_id: `call_runaway_${seq + 1}`, name, args: name === "validate_draft" ? { template_json: JSON.stringify(FIRST_DRAFT ?? { schema: 1 }) } : {} };
				log({ kind: "runaway", ...base, reply: turn(res, body.model, session, `rs_runaway_${seq + 1}`, [call]) });
			};
			if (HOLD_MS) {
				res.writeHead(200, { "content-type": "text/event-stream" });
				res.flushHeaders();
				setTimeout(reply, HOLD_MS);
			} else reply();
			return;
		}
		if (opts.script === "authoring" || (opts.script === "interleaved" && tools.includes("validate_draft"))) {
			if (!tools.includes("describe_vocabulary") || !tools.includes("validate_draft") || !tools.includes("submit_draft")) return reject(res, base, "authoring tools missing");
			const vocabulary = outputOf(input, "describe_vocabulary");
			const submitted = outputOf(input, "submit_draft"), validated = outputOf(input, "validate_draft");
			if (submitted?.ok) return log({ kind: "authoring", ...base, reply: turn(res, body.model, session, null, [], "authoring complete") });
			if (submitted) return reject(res, base, "submitted draft was not accepted");
			if (validated) {
				if (opts.script === "interleaved" && !FIRST_DRAFT) return log({ kind: "interleaved", ...base, reply: turn(res, body.model, session, null, [], "interleaved complete") });
				const error = validated.errors?.find((e) => e.message === "route must cover each typed value" && e.path === "child.nodes.route_scope.cases");
				const miss = error?.missing_cases;
				if (validated.ok || !Array.isArray(miss) || miss.length === 0) return reject(res, base, "expected defective route validation feedback");
				const fixed = structuredClone(FIRST_DRAFT);
				for (const m of miss) {
					if (!vocabulary?.route_values?.["join.route_status"]?.includes(m)) return reject(res, base, "feedback requested undeclared route case");
					fixed.child.nodes.route_scope.cases[m] = "draft_unresolved";
				}
				return log({ kind: "authoring", ...base, feedback: validated.errors, reply: turn(res, body.model, session, "rs_author_3", [{ call_id: "call_submit_1", name: "submit_draft", args: { template_json: JSON.stringify(fixed) } }]) });
			}
			if (opts.script === "interleaved" && !vocabulary) return log({ kind: "interleaved", ...base, reply: turn(res, body.model, session, "rs_author_1", [
				{ call_id: "call_vocab_1", name: "describe_vocabulary", args: {} },
				{ call_id: "call_validate_1", name: "validate_draft", args: { template_json: JSON.stringify(FIRST_DRAFT ?? { schema: 1 }) } }]) });
			if (vocabulary) return log({ kind: "authoring", ...base, reply: turn(res, body.model, session, "rs_author_2", [
				{ call_id: "call_validate_1", name: "validate_draft", args: { template_json: JSON.stringify(FIRST_DRAFT) } }]) });
			return log({ kind: "authoring", ...base, reply: turn(res, body.model, session, "rs_author_1", [
				{ call_id: "call_vocab_1", name: "describe_vocabulary", args: {} }]) });
		}
		if (opts.script === "interleaved" && tools.includes("fixture_command")) {
			if (!hasOut) {
				dirStarts++;
				const command = input.filter((i) => i.role === "user").at(-1).content.map((c) => c.text).join("");
				return log({ kind: "director", ...base, reply: turn(res, body.model, session, `rs_dir_${dirStarts}`, [{ call_id: `call_dir_${dirStarts}`, name: "fixture_command", args: { command_json: command } }]) });
			}
			if (holds === 0) { // hold the post-tool reply open so the orchestrator can SIGKILL the broker mid-turn
				holds++;
				log({ kind: "hold", ...base });
				res.writeHead(200, { "content-type": "text/event-stream" });
				return res.write(`event: response.created\ndata: ${JSON.stringify({ type: "response.created", response: { id: "resp_hold", status: "in_progress", output: [] } })}\n\n`);
			}
			const lastOut = [...input].reverse().find((i) => i.type === "function_call_output");
			return log({ kind: "director", ...base, reply: turn(res, body.model, session, null, [], lastOut.output) });
		}
		reject(res, base, "unsupported tool set for selected script");
	});
});
if (opts.selfCheck) selfCheck();
else server.listen(PORT, "127.0.0.1", () => console.log(JSON.stringify({ mock: PORT, script: opts.script })));

// Test-only `node --import` preload. Never records a credential or contacts a remote host.
import fs from "node:fs";

const record = process.env.EXO_MOCK_OAUTH_RECORD;
if (!record) throw new Error("EXO_MOCK_OAUTH_RECORD is required for mock OAuth refresh");
const originalFetch = globalThis.fetch;
let grants = 0;
const jwtPart = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");

globalThis.fetch = async (input, init) => {
	const url = new URL(input instanceof Request ? input.url : input);
	if (url.href === "https://auth.openai.com/oauth/token") {
		const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();
		const body = init?.body ?? (input instanceof Request ? await input.clone().text() : "");
		const params = new URLSearchParams(body);
		if (method !== "POST" || params.get("grant_type") !== "refresh_token" || !params.get("refresh_token")) {
			throw new Error("mock OAuth allows only refresh_token grants");
		}
		grants++;
		const account = "acct_synthetic";
		const access = `${jwtPart({ alg: "none", typ: "JWT" })}.${jwtPart({ exp: Math.floor(Date.now() / 1000) + 3600, "https://api.openai.com/auth": { chatgpt_account_id: account }, "https://api.openai.com/auth.chatgpt_account_id": account, mock_rotation: grants })}.signature`;
		const refresh = `synthetic_refresh_${process.pid}_${grants}`;
		fs.appendFileSync(record, JSON.stringify({ kind: "refresh_token", sequence: grants, account, credential_present: true }) + "\n", { mode: 0o600 });
		return new Response(JSON.stringify({ access_token: access, refresh_token: refresh, expires_in: 3600, token_type: "Bearer" }), {
			status: 200, headers: { "content-type": "application/json" },
		});
	}
	if (!["localhost", "127.0.0.1", "::1", "[::1]"].includes(url.hostname)) {
		throw new Error(`mock OAuth blocked non-loopback fetch: ${url.origin}`);
	}
	return originalFetch(input, init);
};

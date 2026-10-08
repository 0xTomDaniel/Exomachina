import { validateBundle, verifyArtifactBytes } from "../contract.mjs";

function cursorFrame(frame) {
  if (frame.op === "snapshot") return false; // the atomic bundle snapshot is always emitted first.
  return ["event", "checkpoint", "resumed", "resync_required", "error"].includes(frame.op);
}

export function createRecordedAdapter(bundle, { artifactBytes = new Map(), artifactRefs = [], fetcher = globalThis.fetch, onFrame = () => {} } = {}) {
  validateBundle(bundle);
  const refs=new Map();
  for(const ref of artifactRefs){
    if(!ref||Object.keys(ref).some(k=>!['run_id','revision','sha256','url'].includes(k))||!ref.run_id||!ref.revision||! /^[0-9a-f]{64}$/.test(ref.sha256)||typeof ref.url!=='string')throw new Error('invalid recorded artifact reference');
    const url=new URL(ref.url,globalThis.location?.href??'http://localhost/');
    if(globalThis.location&&url.origin!==globalThis.location.origin)throw new Error('recorded artifact URLs must be same-origin');
    refs.set(`${ref.run_id}:${ref.revision}:${ref.sha256}`,url.toString());
  }
  const frames = bundle.frames.filter(cursorFrame);
  let index = 0;
  const listeners = new Set();
  const emit = frame => { onFrame(frame); for (const listener of listeners) listener(frame); };
  return {
    source:"recorded",
    provenance:structuredClone(bundle.provenance),
    async discover() { return [{ id:bundle.snapshot.state.factory.id, name:bundle.snapshot.state.factory.name, schema_version:1, provenance:structuredClone(bundle.provenance) }]; },
    async snapshot(factoryId = bundle.snapshot.state.factory.id) {
      if (factoryId !== bundle.snapshot.state.factory.id) throw new Error("factory is not in this recording");
      return structuredClone(bundle.snapshot);
    },
    observe(factoryId = bundle.snapshot.state.factory.id, afterCursor = null, runId = null, onObservedFrame = null) {
      if (factoryId !== bundle.snapshot.state.factory.id) throw new Error("factory is not in this recording");
      const filtered = frames.filter(frame => frame.op !== "event" || !runId || frame.event.data.run_id === runId);
      index = afterCursor == null ? 0 : filtered.findIndex(frame => frame.op === "event" && frame.cursor === afterCursor) + 1;
      if (index < 0) index = 0;
      const listener = frame => { onObservedFrame?.(frame); }; listeners.add(listener);
      emit({ op:"snapshot", snapshot:structuredClone(bundle.snapshot) });
      let localIndex = index;
      return {
        next() { const frame=filtered[localIndex++]; if (frame) emit(structuredClone(frame)); return frame ?? null; },
        play() { while (localIndex < filtered.length) this.next(); },
        close() { listeners.delete(listener); },
      };
    },
    async command() { return { lifecycle:"rejected", reason:"recordings are read-only" }; },
    async submit() { return { lifecycle:"rejected", reason:"recordings are read-only" }; },
    async inspect_artifact(runId, revision, sha256) {
      const key=`${runId}:${revision}:${sha256}`;
      let bytes=artifactBytes instanceof Map ? artifactBytes.get(key) : artifactBytes[key];
      if(bytes==null&&refs.has(key)&&typeof fetcher==='function'){
        const response=await fetcher(refs.get(key),{credentials:'same-origin',headers:{Accept:'application/octet-stream, text/markdown'}});
        if(!response.ok)return {available:false,reason:`recorded artifact request failed (${response.status})`};
        bytes=await response.arrayBuffer();if(artifactBytes instanceof Map)artifactBytes.set(key,bytes);else artifactBytes[key]=bytes;
      }
      if (bytes == null) return { available:false, reason:"artifact bytes were not included in this recording" };
      const check=await verifyArtifactBytes(bytes,sha256);
      return { available:check.valid, bytes, sha256:check.actual, valid:check.valid, reason:check.valid?"":"recorded artifact digest mismatch" };
    },
    get position() { return index; },
    seek(position) { index=Math.max(0,Math.min(frames.length,Number(position)||0)); return index; },
    get length() { return frames.length; },
  };
}

export async function loadRecordedAdapter({ url, bundle, artifactManifestUrl, artifactRefs = [], fetcher = globalThis.fetch, credentials = "same-origin", onFrame } = {}) {
  if (!bundle) {
    if (!url || typeof fetcher !== "function") throw new Error("recording URL is not configured");
    const response=await fetcher(url,{credentials,headers:{Accept:"application/json"}});
    if (!response.ok) throw new Error(`recording request failed (${response.status})`);
    bundle=await response.json();
  }
  if(artifactManifestUrl){
    const response=await fetcher(artifactManifestUrl,{credentials,headers:{Accept:"application/json"}});
    if(!response.ok)throw new Error(`recorded artifact manifest failed (${response.status})`);
    const manifest=await response.json();if(!manifest||manifest.schema_version!==1||!Array.isArray(manifest.artifacts))throw new Error('invalid recorded artifact manifest');
    artifactRefs=manifest.artifacts;
  }
  return createRecordedAdapter(bundle,{artifactRefs,fetcher,onFrame});
}

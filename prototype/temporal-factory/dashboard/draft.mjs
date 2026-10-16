// UI draft state is independent of authority to submit. Nothing is persisted,
// sent to an Adapter, or shared between observation sources or factories.
export function briefInputPolicy(source, submissionReason = "") {
  const readOnly = source === "recorded" || !["live", "demo"].includes(source);
  const reason = source === "recorded" ? "Recorded evidence is read-only." : String(submissionReason || "");
  return {readOnly, disabled:false, submitDisabled:readOnly || !!reason, reason};
}

export function createDraftStore() {
  const drafts = new Map();
  const key = ({source, factoryId}) => JSON.stringify([source, factoryId ?? null]);
  return {
    get(selection) { return drafts.get(key(selection)) ?? ""; },
    set(selection, value) {
      if(typeof value !== "string" || value.length > 4000)throw new TypeError("Draft must be a bounded string");
      drafts.set(key(selection), value);
    },
  };
}

/** Build a command only from an observed current wait and verified candidate. */
export function makeWaitCommand({factoryId,run,action,commandId,inspected,nowMs=Date.now()}) {
  const state=run?.state;
  const role={'awaiting-director':'director','awaiting-human':'human'}[state?.phase];
  if(!role||state.wait_role!==role||state.state!=='input-required')throw new Error('No current authoritative wait facts are available.');
  const deadline=typeof state.wait_deadline==='string'?Date.parse(state.wait_deadline):NaN;
  if(!Number.isFinite(nowMs)||!Number.isFinite(deadline)||nowMs>=deadline)throw new Error('The current wait deadline is unavailable or expired.');
  if(!Array.isArray(state.permitted_actions)||!state.permitted_actions.includes(action))throw new Error('This action is not permitted by the current wait.');
  if(!run.task?.id||!run.task?.context_id)throw new Error('The original Task binding is unavailable.');
  const candidate=run.quality?.at(-1);
  const artifact=run.artifacts?.at(-1);
  if(candidate?.accepted!==false||!candidate.artifact_revision||!candidate.artifact_sha256||artifact?.artifact_revision!==candidate.artifact_revision||artifact?.artifact_sha256!==candidate.artifact_sha256)throw new Error('The current rejected candidate binding is unavailable.');
  if(inspected?.valid!==true||inspected.sha256!==candidate.artifact_sha256)throw new Error('The exact candidate bytes have not been verified.');
  return {op:'command',factory_id:factoryId,command_id:commandId,task_id:run.task.id,context_id:run.task.context_id,action,expected_state:state.state,expected_revision:candidate.artifact_revision,expected_sha256:candidate.artifact_sha256};
}

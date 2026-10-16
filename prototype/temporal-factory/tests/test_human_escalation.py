"""Synthetic pinned-policy/decision tests; no engine, model or payment calls."""
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import factory
import definition

class HumanPolicyTest(unittest.TestCase):
    def test_optional_policy_is_strict(self):
        doc=json.loads((Path(__file__).resolve().parents[1]/'definitions/report-template.json').read_text())['child']
        name=next(k for k,n in doc['nodes'].items() if n['type']=='director_wait')
        bindings={name:{'role':role,'approved':True} for name,role in (
            ('research_findings','capability'),('research_risks','capability'),
            ('synthesizer','capability'),('quality','quality'),('release','release'))}
        validate=lambda value:definition._definition(value,{},bindings,parent=False)
        validate(doc);doc['nodes'][name]['human']={'actor':'human-user','timeout_seconds':5};validate(doc)
        for policy in ({'actor':'human user','timeout_seconds':5},
                       {'actor':'human-user','timeout_seconds':True},
                       {'actor':'human-user','timeout_seconds':0},
                       {'actor':'human-user','timeout_seconds':3601},
                       {'actor':'human-user'},
                       {'actor':'human-user','timeout_seconds':5,'approve':True}):
            bad=deepcopy(doc);bad['nodes'][name]['human']=policy
            with self.assertRaises(ValueError):validate(bad)

class HumanWaitTest(unittest.IsolatedAsyncioTestCase):
    def instance(self):
        run=factory.FactoryRun();run.run_id='run';run.definition_digest='definition'
        run.director_identity='director';run.owner_epoch=1;run.current={'revision':'r3','sha256':'candidate'}
        return run
    def command(self,action,actor,id):
        return dict(command_id=id,action=action,actor=actor,run='run',definition_digest='definition',
                    revision='r3',sha256='candidate',epoch=1)
    async def execute(self,expire):
        run=self.instance();observed=[];now=datetime(2026,10,3,tzinfo=timezone.utc)
        async def waiting(predicate,*,timeout):
            observed.append((run.phase,timeout.total_seconds()))
            if run.phase=='awaiting-director':
                self.assertEqual(run.permitted_actions,['abort','escalate'])
                value=self.command('escalate','director','escalate')
            else:
                self.assertEqual(run.status()['decision_actor'],'human-user')
                self.assertEqual(run.applied_decisions,{'escalate':'escalate-recorded'})
                with self.assertRaises(ValueError):run.validate_director_command(self.command('abort','director','wrong-actor'))
                if expire:raise asyncio.TimeoutError
                value=self.command('abort','human-user','human')
            run.validate_director_command(value);run.director_command(value);self.assertTrue(predicate())
            with self.assertRaises(ValueError):run.validate_director_command(self.command('abort',value['actor'],'conflict'))
        doc={'start':'wait','nodes':{'wait':dict(type='director_wait',reason='repair_exhausted',next='abort',human=dict(actor='human-user',timeout_seconds=5)),
                                   'abort':dict(type='abort')}}
        with patch.object(factory,'verify_closure'),patch.object(factory.workflow,'now',return_value=now), \
             patch.object(factory.workflow,'wait_condition',side_effect=waiting):
            result=await run.run_node(doc,{'bindings':{}},dict(closure={},wait_seconds=10))
            with self.assertRaises(ValueError):run.validate_director_command(self.command('abort','human-user','late'))
        self.assertFalse(result['released']);self.assertEqual(observed,[('awaiting-director',10),('awaiting-human',5)])
        return run,result
    async def test_escalation_human_abort_and_concurrent_conflict(self):
        run,result=await self.execute(False)
        self.assertEqual(result['status'],'aborted')
        self.assertEqual(run.applied_decisions,{'escalate':'escalate-recorded','human':'abort-recorded'})
        self.assertEqual(run.completed,['wait:escalate','wait:abort'])
    async def test_expiry_rejects_late_human(self):
        run,result=await self.execute(True)
        self.assertEqual(result['status'],'expired');self.assertEqual(run.phase,'human-expired')

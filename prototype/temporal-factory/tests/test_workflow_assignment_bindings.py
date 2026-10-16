"""Synthetic execution envelope checks; zero engine/provider/payment calls."""
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch, call
from uuid import UUID, uuid4
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import factory

class WorkflowAssignmentBindingsTest(unittest.IsolatedAsyncioTestCase):
    def test_legacy_envelope_unchanged(self):
        run = factory.FactoryRun()
        value = {'assignment_id': 'legacy:quality', 'attempt': 2}
        with patch.object(factory.workflow, 'uuid4', side_effect=AssertionError('legacy UUID')):
            self.assertIs(run._assignment_input('review', value), value)
        self.assertEqual(value, {'assignment_id': 'legacy:quality', 'attempt': 2})

    def test_assignment_stable_attempts_distinct_branches_separate(self):
        run = factory.FactoryRun(); run._explicit_assignment_bindings = True
        with patch.object(factory.workflow, 'uuid4', side_effect=[uuid4() for _ in range(5)]):
            a = run._assignment_input('research', {'attempt': 1}, branch='findings')
            b = run._assignment_input('research', {'attempt': 2}, branch='findings')
            c = run._assignment_input('research', {}, branch='risks')
        self.assertEqual(a['assignment_id'], b['assignment_id'])
        self.assertNotEqual(a['attempt_id'], b['attempt_id'])
        self.assertNotEqual(a['assignment_id'], c['assignment_id'])
        self.assertEqual(b['attempt'], 2)
        for row in (a,b,c):
            self.assertEqual(row['node'], 'research')
            UUID(row['assignment_id']); UUID(row['attempt_id'])
            self.assertNotEqual(row['assignment_id'], row['attempt_id'])

    def test_factory_binding_comes_only_from_director_authority(self):
        run=factory.FactoryRun();run._explicit_assignment_bindings=True
        run._explicit_factory_binding=True;run.director_identity='actual-factory'
        with patch.object(factory.workflow,'uuid4',side_effect=[uuid4(),uuid4()]):
            row=run._assignment_input('research',{'run':'different-opaque-run'})
        self.assertEqual(row['factory_id'],'actual-factory')
        run.director_identity=''
        with self.assertRaises(ValueError):run._assignment_input('research',{})

    async def test_run_patch_guard(self):
        for enabled in (False, True):
            run = factory.FactoryRun()
            value = dict(run='run', definition_digest='definition', package_digest='package',
                         document={'nodes': {}}, package={'run_inputs': {}},
                         closure={'manifest': {'package_digest': 'package'}},
                         director={'identity': 'director', 'epoch': 1}, run_inputs={},
                         run_inputs_digest='inputs', authorized_actor='owner', input_authority={}, wait_seconds=10)
            with patch.object(factory, 'verify_closure', return_value='manifest'), \
                 patch.object(factory, 'validate_run_inputs', return_value={}), \
                 patch.object(factory, 'digest', return_value='inputs'), \
                 patch.object(factory.workflow, 'patched', return_value=enabled) as guard, \
                 patch.object(run, 'run_node', new=AsyncMock(return_value={} )):
                await run.run(value)
            self.assertEqual(guard.call_args_list,[call('exo-explicit-assignment-bindings-v1'),call('exo-explicit-factory-binding-v1'),call('exo-handoff-records-v1'),call('exo-quality-findings-handoff-v1')])
            self.assertEqual(run._handoff_records,enabled)
            self.assertEqual(run._explicit_factory_binding,enabled)
            self.assertEqual(run._explicit_assignment_bindings, enabled)

    async def test_actual_activity_call_sites(self):
        run=factory.FactoryRun();run.run_id='run';run.definition_digest='definition'
        run.run_inputs={'question':'synthetic only'};run._explicit_assignment_bindings=True
        bindings={'research':{'role':'capability','url':'http://127.0.0.1:1','identity':'research'},
                  'synthesis':{'role':'capability','identity':'synthesis'},'quality':{'role':'quality','identity':'quality'},
                  'release':{'role':'capability','url':'http://127.0.0.1:1','identity':'release'}}
        package={'bindings':bindings,'evidence_packet':{}}
        branch=lambda name: dict(service='research',result_type=name,capability=name)
        document={'start':'research','nodes':{
            'research':dict(type='parallel',branches={'findings':branch('findings'),'risks':branch('risks')},next='join'),
            'join':dict(type='join',branches=['findings','risks'],next='draft'),
            'draft':dict(type='synthesize',service='synthesis',next='review'),
            'review':dict(type='quality',next='deliver'),
            'deliver':dict(type='release',service='release',next='done'),'done':dict(type='complete')}}
        closure={'contracts':{name:{} for name in bindings},'manifest':{'quality_policy_digest':'policy'},'quality_policy':{}}
        calls=[]
        async def execute(fn,value):
            calls.append((fn,value))
            if fn is factory.assign:return {}
            if fn is factory.typed_join:return {}
            if fn is factory.synthesize:return dict(revision='r1',sha256='candidate',content='{}')
            if fn is factory.review:return dict(task_id='quality-task',artifact=dict(accepted=True))
            if fn is factory.release:return dict(receipt_id='synthetic')
            raise AssertionError('unexpected activity')
        with patch.object(factory,'verify_closure'),patch.object(factory,'_activity',side_effect=execute), \
             patch.object(factory.workflow,'uuid4',side_effect=[uuid4() for _ in range(8)]):
            result=await run.run_node(document,package,{'closure':closure})
        self.assertEqual(result['status'],'accepted')
        model_inputs=[v for fn,v in calls if fn in {factory.assign,factory.synthesize,factory.review}]
        self.assertEqual([v['node'] for v in model_inputs],['research','research','draft','review'])
        self.assertEqual(len({v['assignment_id'] for v in model_inputs}),4)
        self.assertEqual(len({v['attempt_id'] for v in model_inputs}),4)
        self.assertEqual(model_inputs[-1]['attempt'],1)
        for value in model_inputs:UUID(value['assignment_id']);UUID(value['attempt_id'])

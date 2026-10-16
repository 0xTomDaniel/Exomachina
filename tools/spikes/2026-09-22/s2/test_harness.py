import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from harness import Harness, Rejected


def test_one_package_two_roles_and_accepted_completion(tmp_path):
    ordinary = Harness(tmp_path / 'ordinary', 'capability')
    director = Harness(tmp_path / 'director', 'factory')
    async def exercise():
        capability = await ordinary.invoke({'op': 'start', 'key': 'c1', 'brief': 'hello'})
        pending = await director.invoke({'op': 'start', 'key': 'f1', 'brief': 'report'})
        assert capability['state'] == 'completed'
        assert pending['state'] == 'input-required'
        assert pending['accepted_output'] is None
        result = await director.invoke({'op': 'decide', 'key': 'd1', 'run_id': pending['id']})
        assert result['state'] == 'completed'
        assert result['accepted_output']['reviewer'] == 'fixed-evaluator-fixture'
        assert result['accepted_output']['definition'] == 'fixture.factory@1'
        assert ordinary.identity != director.identity
        assert capability['id'] != pending['id']
    asyncio.run(exercise())


def test_restart_fences_old_incarnation_and_restores_pending_work(tmp_path):
    first = Harness(tmp_path / 'director', 'factory')
    run = first.command({'op': 'start', 'key': 'f1', 'brief': 'report'})
    second = Harness(tmp_path / 'director', 'factory')
    assert first.identity == second.identity
    assert second.incarnation == first.incarnation + 1
    assert second.command({'op': 'inspect', 'run_id': run['id']}) == run
    with pytest.raises(Rejected, match='stale incarnation'):
        first.command({'op': 'decide', 'key': 'stale', 'run_id': run['id']})
    with pytest.raises(Rejected, match='unauthorized'):
        second.command({'op': 'decide', 'key': 'bad', 'run_id': run['id']}, actor='observer')
    assert second.command({'op': 'decide', 'key': 'good', 'run_id': run['id']})['state'] == 'completed'


def test_concurrent_repeated_assignment_has_one_logical_identity(tmp_path):
    harness = Harness(tmp_path, 'factory')
    command = {'op': 'start', 'key': 'same', 'brief': 'report'}
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(lambda _: harness.command(command), range(24)))
    assert len({result['id'] for result in results}) == 1
    assert len(harness.command({'op': 'list'})['runs']) == 1
    with pytest.raises(Rejected, match='idempotency key conflict'):
        harness.command(command | {'brief': 'different'})


def test_child_correlation_is_owned_durable_and_bounded(tmp_path):
    parent = Harness(tmp_path, 'factory')
    run = parent.command({'op': 'start', 'key': 'p1', 'brief': 'parent'})
    child = {'url': 'http://127.0.0.1:1234', 'key': parent.identity + ':' + run['id']}
    attached = parent.command({'op': 'prepare_child', 'key': 'child-intent', 'run_id': run['id'], 'child': child})
    assert attached['child'] == child
    restarted = Harness(tmp_path, 'factory')
    assert restarted.command({'op': 'inspect', 'run_id': run['id']})['child'] == child
    with pytest.raises(Rejected, match='child already assigned'):
        restarted.command({'op': 'prepare_child', 'key': 'other-child', 'run_id': run['id'],
                           'child': child | {'key': 'different'}})

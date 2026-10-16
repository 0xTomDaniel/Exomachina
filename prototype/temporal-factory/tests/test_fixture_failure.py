"""Synthetic fixture errors remain errors across the Strands stream adapter."""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from harness_server import ToolCallingModelFixture


class FixtureFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_tool_is_safe_json_failure(self):
        events = [event async for event in ToolCallingModelFixture().stream([
            {'content': [{'toolResult': {'status': 'error', 'content': [
                {'text': 'RuntimeError: internal environment details'}]}}]}])]
        text = ''.join(event.get('contentBlockDelta', {}).get('delta', {}).get('text', '')
                       for event in events)
        self.assertEqual(json.loads(text), {'error': 'fixture tool execution failed'})
        self.assertNotIn('internal environment', text)

    async def test_successful_tool_keeps_exact_json(self):
        payload = {'run_id': 'synthetic-run', 'accepted_command': 'start'}
        events = [event async for event in ToolCallingModelFixture().stream([
            {'content': [{'toolResult': {'status': 'success', 'content': [
                {'json': payload}]}}]}])]
        text = ''.join(event.get('contentBlockDelta', {}).get('delta', {}).get('text', '')
                       for event in events)
        self.assertEqual(json.loads(text), payload)

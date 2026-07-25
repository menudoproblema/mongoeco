import ast
from pathlib import Path
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
_INTERNAL_MATCH_CALLERS = (
    'src/mongoeco/engines/semantic_core.py',
    'src/mongoeco/engines/memory.py',
    'src/mongoeco/engines/sqlite.py',
)


class ExecutionContextArchitectureTests(unittest.TestCase):
    def test_engine_match_plan_calls_explicitly_forward_execution_variables(self):
        missing: list[str] = []
        for relative_path in _INTERNAL_MATCH_CALLERS:
            path = PROJECT_ROOT / relative_path
            tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                target = node.func
                if not (
                    isinstance(target, ast.Attribute)
                    and target.attr == 'match_plan'
                    and isinstance(target.value, ast.Name)
                    and target.value.id == 'QueryEngine'
                ):
                    continue
                if not any(keyword.arg == 'variables' for keyword in node.keywords):
                    missing.append(f'{relative_path}:{node.lineno}')
        self.assertEqual(missing, [])

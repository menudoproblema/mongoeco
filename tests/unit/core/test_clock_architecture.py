import ast
import json
from pathlib import Path
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = PROJECT_ROOT / 'src' / 'mongoeco'
ALLOWLIST_PATH = PROJECT_ROOT / 'tests' / 'fixtures' / 'clock_call_allowlist.json'


def _call_name(node: ast.Call) -> str | None:
    function = node.func
    if isinstance(function, ast.Name) and function.id == 'utc_bson_now':
        return 'utc_bson_now'
    if not isinstance(function, ast.Attribute):
        return None
    if (
        function.attr in {'now', 'utcnow'}
        and (
            isinstance(function.value, ast.Name) and function.value.id == 'datetime'
            or (
                isinstance(function.value, ast.Attribute)
                and isinstance(function.value.value, ast.Name)
                and function.value.value.id == 'datetime'
                and function.value.attr == 'datetime'
            )
        )
    ):
        return f'datetime.{function.attr}'
    if (
        isinstance(function.value, ast.Name)
        and function.value.id == 'time'
        and function.attr in {'time', 'monotonic'}
    ):
        return f'time.{function.attr}'
    return None


class ClockArchitectureTests(unittest.TestCase):
    def test_direct_clock_reads_are_explicitly_classified(self):
        allowlist = json.loads(ALLOWLIST_PATH.read_text(encoding='utf-8'))
        observed: dict[str, set[str]] = {name: set() for name in allowlist}
        unexpected: list[str] = []
        for path in SOURCE_ROOT.rglob('*.py'):
            relative = path.relative_to(PROJECT_ROOT).as_posix()
            tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = _call_name(node)
                if name is None:
                    continue
                observed.setdefault(name, set()).add(relative)
                if relative not in allowlist.get(name, {}):
                    unexpected.append(f'{relative}:{node.lineno} ({name})')
        self.assertEqual(unexpected, [])
        for name, modules in allowlist.items():
            self.assertEqual(set(modules), observed.get(name, set()), name)

    def test_expression_context_is_not_minted_implicitly_outside_its_module(self):
        violations: list[str] = []
        for path in SOURCE_ROOT.rglob('*.py'):
            relative = path.relative_to(PROJECT_ROOT).as_posix()
            if relative == 'src/mongoeco/core/expression_context.py':
                continue
            tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                if isinstance(node.func, ast.Name) and node.func.id == 'ensure_expression_context':
                    if not node.args or (
                        isinstance(node.args[0], ast.Constant) and node.args[0].value is None
                    ):
                        violations.append(f'{relative}:{node.lineno} ensure_expression_context')
                if isinstance(node.func, ast.Name) and node.func.id == 'ExpressionExecutionContext':
                    if not any(keyword.arg == 'now' for keyword in node.keywords):
                        violations.append(f'{relative}:{node.lineno} ExpressionExecutionContext')
        self.assertEqual(violations, [])

    def test_ttl_purge_callers_pass_the_operation_clock_explicitly(self):
        targets = {
            'src/mongoeco/engines/memory.py': '_purge_expired_documents_locked',
            'src/mongoeco/engines/sqlite.py': '_purge_expired_documents_sync',
        }
        missing: list[str] = []
        for relative, method_name in targets.items():
            path = PROJECT_ROOT / relative
            tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                if not isinstance(node.func, ast.Attribute) or node.func.attr != method_name:
                    continue
                if isinstance(node.func.value, ast.Name) and node.func.value.id == 'self':
                    if not any(keyword.arg == 'now' for keyword in node.keywords):
                        missing.append(f'{relative}:{node.lineno}')
        self.assertEqual(missing, [])

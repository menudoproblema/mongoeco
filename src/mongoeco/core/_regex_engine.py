"""Execute MQL patterns with an explicitly selected, process-stable backend."""

from __future__ import annotations

import os
import re

from dataclasses import dataclass
from functools import cache, lru_cache
from typing import Any

from mongoeco.core.server_errors import server_error


_BACKEND = os.environ.get("MONGOECO_REGEX_BACKEND", "python")


@cache
def _pcre2() -> Any:
    try:
        import pcre2  # noqa: PLC0415 - optional backend is imported only on explicit use
    except ImportError as error:
        message = "PCRE2 was requested; install mongoeco[pcre2] and restart the process"
        raise RuntimeError(message) from error
    return pcre2


@dataclass(frozen=True, slots=True)
class _PCREPattern:
    compiled: Any
    operator: str | None

    @property
    def pattern(self):
        return self.compiled.pattern

    def _failure(self, error):
        if self.operator is not None:
            message = (
                "Error occurred while executing the regular expression in "
                f"{self.operator}. Result code: {error}"
            )
            raise server_error(message, code=51156) from error

    def search(self, text):
        try:
            return self.compiled.search(text)
        except _pcre2().LibraryError as error:
            return self._failure(error)

    def finditer(self, text):
        try:
            yield from self.compiled.finditer(text)
        except _pcre2().LibraryError as error:
            self._failure(error)


@lru_cache(maxsize=256)
def _compile_pcre2(pattern, flags, operator):
    backend = _pcre2()
    native_flags = backend.ASCII
    for python_flag, name in (
        (re.IGNORECASE, "IGNORECASE"),
        (re.MULTILINE, "MULTILINE"),
        (re.DOTALL, "DOTALL"),
        (re.VERBOSE, "VERBOSE"),
    ):
        if flags & python_flag:
            native_flags |= getattr(backend, name)
    try:
        return _PCREPattern(backend.compile(pattern, native_flags), operator)
    except backend.PatternError as error:
        reason = str(error).partition("; ")[2] or str(error)
        raise re.error(reason) from error


def compile_pattern(
    pattern: str, flags: int = 0, *, operator: str | None = None
) -> Any:
    """Keep validation/diagnostic ownership in the caller; normalize syntax errors."""
    if _BACKEND == "python":
        return re.compile(pattern, flags)
    if _BACKEND != "pcre2":
        message = f"Unknown MONGOECO_REGEX_BACKEND: {_BACKEND!r}"
        raise RuntimeError(message)
    return _compile_pcre2(pattern, flags, operator)

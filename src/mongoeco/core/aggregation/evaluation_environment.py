from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mongoeco.core.expression_context import ensure_expression_context


if TYPE_CHECKING:
    from collections.abc import Mapping

    from mongoeco.types import Document


class EvaluationEnvironment(dict[str, Any]):
    """Explicit aggregation frame with stable root and rebindable current."""

    def __init__(
        self,
        bindings: Mapping[str, Any] | None = None,
        *,
        root: Document | None = None,
        current: Document | None = None,
    ) -> None:
        super().__init__(bindings or {})
        self.root = root
        self.current = current
        if root is not None:
            self['ROOT'] = root
        if current is not None:
            self['CURRENT'] = current

    @property
    def is_bound(self) -> bool:
        return self.root is not None and self.current is not None

    def with_lexicals(self) -> EvaluationEnvironment:
        return EvaluationEnvironment(
            self,
            root=self.root,
            current=self.current,
        )

    def with_current(self, document: Document) -> EvaluationEnvironment:
        return EvaluationEnvironment(
            self,
            root=self.root if self.root is not None else document,
            current=document,
        )


def environment_for_document(
    document: Document,
    variables: Mapping[str, Any] | None,
) -> EvaluationEnvironment:
    if isinstance(variables, EvaluationEnvironment):
        return (
            variables
            if variables.is_bound
            else variables.with_current(document)
        )
    context = ensure_expression_context(variables)
    return EvaluationEnvironment(
        {**context, 'ROOT': document, 'CURRENT': document},
        root=document,
        current=document,
    )


def scoped_environment(
    variables: Mapping[str, Any] | None,
) -> EvaluationEnvironment:
    if isinstance(variables, EvaluationEnvironment):
        return variables.with_lexicals()
    return EvaluationEnvironment(variables or {})

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mongoeco.core.expression_context import ensure_expression_context


if TYPE_CHECKING:
    from collections.abc import Mapping

    from mongoeco.types import Document


class EvaluationEnvironment(dict[str, Any]):
    """Explicit aggregation frame with stable root and rebindable current."""

    def __init__(  # noqa: PLR0913 - complete expression frame metadata
        self,
        bindings: Mapping[str, Any] | None = None,
        *,
        root: Document | None = None,
        current: Document | None = None,
        collation=None,
        preserve_numeric_conversion_types: bool = False,
        unavailable_system_variables=frozenset(),
    ) -> None:
        super().__init__(bindings or {})
        self.root = root
        self.collation = collation
        self.preserve_numeric_conversion_types = preserve_numeric_conversion_types
        self.unavailable_system_variables = unavailable_system_variables
        if root is not None:
            self["ROOT"] = root
        if current is not None:
            self["CURRENT"] = current

    @property
    def current(self):
        return self.get("CURRENT")

    @property
    def is_bound(self) -> bool:
        return self.root is not None

    def with_lexicals(self) -> EvaluationEnvironment:
        return EvaluationEnvironment(
            self,
            root=self.root,
            current=self.current,
            collation=self.collation,
            preserve_numeric_conversion_types=self.preserve_numeric_conversion_types,
            unavailable_system_variables=self.unavailable_system_variables,
        )

    def with_current(self, document: Document) -> EvaluationEnvironment:
        return EvaluationEnvironment(
            self,
            root=self.root if self.root is not None else document,
            current=document,
            collation=self.collation,
            preserve_numeric_conversion_types=self.preserve_numeric_conversion_types,
            unavailable_system_variables=self.unavailable_system_variables,
        )


def environment_for_document(
    document: Document,
    variables: Mapping[str, Any] | None,
) -> EvaluationEnvironment:
    if isinstance(variables, EvaluationEnvironment):
        return (
            variables
            if variables.is_bound
            else EvaluationEnvironment(
                variables,
                root=document,
                current=variables.get("CURRENT", document),
                collation=variables.collation,
                preserve_numeric_conversion_types=variables.preserve_numeric_conversion_types,
                unavailable_system_variables=variables.unavailable_system_variables,
            )
        )
    context = ensure_expression_context(variables)
    return EvaluationEnvironment(
        context,
        root=document,
        current=context.bindings.get("CURRENT", document),
    )


def scoped_environment(
    variables: Mapping[str, Any] | None,
) -> EvaluationEnvironment:
    if isinstance(variables, EvaluationEnvironment):
        return variables.with_lexicals()
    return EvaluationEnvironment(variables or {})


def aggregation_environment(variables, collation=None) -> EvaluationEnvironment:
    """Carry comparison policy outside user-visible lexical bindings."""
    environment = (
        scoped_environment(ensure_expression_context(variables))
        if not isinstance(variables, EvaluationEnvironment)
        else variables.with_lexicals()
    )
    if collation is not None:
        environment.collation = collation
    return environment

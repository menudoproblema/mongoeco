import unittest

from mongoeco.core.aggregation.extensions import (
    get_registered_aggregation_expression_operator,
    get_registered_aggregation_stage,
    register_aggregation_expression_operator,
    register_aggregation_stage,
    registered_aggregation_expression_operator,
    registered_aggregation_stage,
    unregister_aggregation_expression_operator,
    unregister_aggregation_stage,
)


class AggregationExtensionRegistryTests(unittest.TestCase):
    def test_expression_registry_round_trips_and_validates_names(self):
        handler = lambda document, spec, variables, dialect, context: None

        with self.assertRaises(ValueError):
            register_aggregation_expression_operator("echo", handler)

        register_aggregation_expression_operator("$echo_test", handler)
        try:
            self.assertIs(get_registered_aggregation_expression_operator("$echo_test"), handler)
        finally:
            unregister_aggregation_expression_operator("$echo_test")

        self.assertIsNone(get_registered_aggregation_expression_operator("$echo_test"))

    def test_stage_registry_supports_decorator_registration(self):
        @registered_aggregation_stage("$decorate_test")
        def _handler(documents, spec, context):
            return documents

        try:
            self.assertIs(get_registered_aggregation_stage("$decorate_test"), _handler)
        finally:
            unregister_aggregation_stage("$decorate_test")

        self.assertIsNone(get_registered_aggregation_stage("$decorate_test"))

    def test_expression_registry_supports_decorator_registration(self):
        @registered_aggregation_expression_operator("$echo_decorated")
        def _handler(document, spec, variables, dialect, context):
            return spec

        try:
            self.assertIs(
                get_registered_aggregation_expression_operator("$echo_decorated"),
                _handler,
            )
        finally:
            unregister_aggregation_expression_operator("$echo_decorated")

    def test_stage_registry_validates_names(self):
        handler = lambda documents, spec, context: documents

        with self.assertRaises(ValueError):
            register_aggregation_stage("decorate", handler)

import unittest

from mongoeco.compat import _catalog_export as compat_catalog_export
from mongoeco.compat._catalog_models import OperationOptionSupport, OptionSupportStatus
from mongoeco.cxp import capabilities as cxp_capabilities
from mongoeco.cxp import contracts as cxp_contracts
from mongoeco.cxp import handshake as cxp_handshake
from mongoeco.cxp import telemetry as cxp_telemetry
from mongoeco.engines import _shared_runtime as shared_runtime


class CxpSmokeCoverageTests(unittest.TestCase):
    def test_reexported_cxp_contract_modules_expose_expected_symbols(self) -> None:
        self.assertTrue(hasattr(cxp_contracts, 'CapabilityProvider'))
        self.assertTrue(hasattr(cxp_contracts, 'AsyncTelemetryStreamProvider'))
        self.assertTrue(hasattr(cxp_handshake, 'HandshakeRequest'))
        self.assertTrue(hasattr(cxp_handshake, 'negotiate_protocol_version'))
        self.assertTrue(hasattr(cxp_telemetry, 'TelemetrySnapshot'))
        self.assertTrue(hasattr(cxp_telemetry, 'TelemetryBuffer'))

    def test_shared_runtime_module_reexports_shared_helpers(self) -> None:
        self.assertTrue(hasattr(shared_runtime, 'merge_profile_collection_names'))
        self.assertTrue(hasattr(shared_runtime, 'merge_profile_database_names'))
        self.assertTrue(hasattr(shared_runtime, 'profile_namespace_options'))
        self.assertTrue(hasattr(shared_runtime, 'build_search_index_documents'))

    def test_metadata_helpers_normalize_nested_values(self) -> None:
        value = {
            'tuple': ('a', {'b': ('c', 'd')}),
            1: ['x', {'y': ('z',)}],
        }

        normalized = cxp_capabilities._normalize_json_like(value)

        self.assertEqual(
            normalized,
            {
                'tuple': ['a', {'b': ['c', 'd']}],
                '1': ['x', {'y': ['z']}],
            },
        )

    def test_metadata_to_document_rejects_non_mapping_payloads(self) -> None:
        class _ScalarMetadata:
            pass

        original_to_builtins = cxp_capabilities.msgspec.to_builtins

        def _fake_to_builtins(_value: object) -> object:
            return ['not', 'a', 'mapping']

        cxp_capabilities.msgspec.to_builtins = _fake_to_builtins
        try:
            with self.assertRaises(TypeError):
                cxp_capabilities._metadata_to_document(_ScalarMetadata())  # type: ignore[arg-type]
        finally:
            cxp_capabilities.msgspec.to_builtins = original_to_builtins

    def test_legacy_runtime_subset_projection_validates_mapping_shape(self) -> None:
        original_export = cxp_capabilities.export_cxp_capability_catalog

        def _bad_export() -> dict[str, object]:
            return {'capabilities': ['not-a-mapping']}

        cxp_capabilities.export_cxp_capability_catalog = _bad_export
        try:
            with self.assertRaises(TypeError):
                cxp_capabilities.export_legacy_runtime_subset_catalog()
        finally:
            cxp_capabilities.export_cxp_capability_catalog = original_export

    def test_compat_cxp_projection_exports_delegate_to_canonical_cxp_helpers(self) -> None:
        self.assertEqual(
            compat_catalog_export.export_cxp_profile_catalog(),
            cxp_capabilities.export_cxp_profile_catalog(),
        )
        self.assertEqual(
            compat_catalog_export.export_cxp_profile_support_catalog(),
            cxp_capabilities.export_cxp_profile_support_catalog(),
        )
        self.assertEqual(
            compat_catalog_export.export_cxp_operation_catalog(),
            cxp_capabilities.export_cxp_operation_catalog(),
        )

        original_export = compat_catalog_export.export_cxp_catalog
        compat_catalog_export.export_cxp_catalog = lambda: {'broken': True}
        try:
            self.assertEqual(
                compat_catalog_export.export_cxp_profile_catalog(),
                cxp_capabilities.export_cxp_profile_catalog(),
            )
            self.assertEqual(
                compat_catalog_export.export_cxp_profile_support_catalog(),
                cxp_capabilities.export_cxp_profile_support_catalog(),
            )
            self.assertEqual(
                compat_catalog_export.export_cxp_operation_catalog(),
                cxp_capabilities.export_cxp_operation_catalog(),
            )
        finally:
            compat_catalog_export.export_cxp_catalog = original_export

    def test_profile_support_catalog_export_matches_canonical_projection(self) -> None:
        self.assertEqual(
            cxp_capabilities.export_cxp_profile_support_catalog(),
            cxp_capabilities.export_cxp_capability_catalog()['profileSupport'],
        )

    def test_operation_catalog_export_covers_unknown_capabilities_and_invalid_entries(self) -> None:
        self.assertEqual(
            cxp_capabilities._compatible_profile_names_for_capability('unknown'),
            [],
        )

        original_metadata = cxp_capabilities._MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA
        cxp_capabilities._MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA = {
            **original_metadata,
            'broken': {'operationMetadata': ['not-a-mapping']},
            'broken-entry': {
                'operationMetadata': {
                    'demo': ['not-a-mapping'],
                }
            },
        }
        try:
            operation_catalog = cxp_capabilities.export_cxp_operation_catalog()
        finally:
            cxp_capabilities._MONGOECO_PUBLIC_CXP_CAPABILITY_METADATA = original_metadata

        self.assertNotIn('demo', operation_catalog)
        self.assertEqual(
            operation_catalog['find'][0]['capabilityName'],
            'read',
        )

    def test_operation_option_metadata_covers_all_support_statuses(self) -> None:
        original_catalog = cxp_capabilities.OPERATION_OPTION_SUPPORT_CATALOG
        cxp_capabilities.OPERATION_OPTION_SUPPORT_CATALOG = {
            "demo": {
                "effective": OperationOptionSupport(
                    status=OptionSupportStatus.EFFECTIVE
                ),
                "accepted": OperationOptionSupport(
                    status=OptionSupportStatus.ACCEPTED_NOOP
                ),
                "unsupported": OperationOptionSupport(
                    status=OptionSupportStatus.UNSUPPORTED
                ),
            }
        }
        try:
            metadata = cxp_capabilities._operation_option_metadata("demo")
        finally:
            cxp_capabilities.OPERATION_OPTION_SUPPORT_CATALOG = original_catalog
        self.assertEqual(metadata["supportedOptions"], ["effective"])
        self.assertEqual(metadata["acceptedNoopOptions"], ["accepted"])
        self.assertEqual(metadata["unsupportedOptions"], ["unsupported"])

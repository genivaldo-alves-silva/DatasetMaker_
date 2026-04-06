#!/usr/bin/env python3
"""
Testes da Fase 5 (validação de voucher/country/species).

Uso:
  python -m get_taxon_ref_.test_phase5_validation
"""

from dataclasses import dataclass

import pandas as pd

from get_taxon_ref_.phase5_validation import Phase5Validator


@dataclass
class DummyEnrichment:
    gb_accession: str
    source: str
    found_voucher: str | None = None
    found_country: str | None = None
    found_species: str | None = None


def _base_row(**overrides):
    row = {
        "voucher": "",
        "geo_loc_name": "",
        "Species": "Fomitiporia sp.",
    }
    row.update(overrides)
    return pd.Series(row)


def test_voucher_fill_when_missing(validator: Phase5Validator):
    row = _base_row(voucher="")
    enr = DummyEnrichment("JQ087932", "pdf_table", found_voucher="MUCL 52488")
    result = validator.validate(enr, row, voucher_dict={})

    assert result.voucher.status == "accepted"
    assert result.voucher.value == "MUCL 52488"


def test_voucher_conflict_updates_dict(validator: Phase5Validator):
    row = _base_row(voucher="CBS 123")
    voucher_dict = {"CBS 123": ["CBS:123"]}
    enr = DummyEnrichment("JQ087932", "pdf_table", found_voucher="MUCL 52488")
    result = validator.validate(enr, row, voucher_dict=voucher_dict)

    assert result.voucher.status == "deferred"
    assert result.voucher_conflict is True
    assert result.voucher_dict_updated is True
    assert "MUCL 52488" in voucher_dict["CBS 123"]


def test_country_ambiguous_not_applied(validator: Phase5Validator):
    row = _base_row(geo_loc_name="")
    enr = DummyEnrichment("KC136220", "pdf_table", found_country="Guinea")
    result = validator.validate(enr, row, voucher_dict={})

    assert result.country.status in {"deferred", "accepted", "rejected"}
    # Regra principal: se vier ambíguo, deve capturar candidatos e não aceitar.
    if result.country_candidates:
        assert result.country.status == "deferred"
        assert result.country.reason == "ambiguous_country"


def test_species_only_updates_if_incomplete(validator: Phase5Validator):
    row_incomplete = _base_row(Species="Fomitiporia sp.")
    enr = DummyEnrichment("JQ087932", "pdf_table", found_species="Fomitiporia cupressicola")
    result = validator.validate(enr, row_incomplete, voucher_dict={})
    assert result.species.status == "accepted"

    row_complete = _base_row(Species="Fomitiporia cupressicola")
    result2 = validator.validate(enr, row_complete, voucher_dict={})
    assert result2.species.status == "deferred"
    assert result2.species.reason == "existing_species_complete"


def test_reject_citation_marker_as_voucher(validator: Phase5Validator):
    row = _base_row(voucher="")
    enr = DummyEnrichment("JQ087932", "pdf_table", found_voucher="[40]")
    result = validator.validate(enr, row, voucher_dict={})

    assert result.voucher.status == "rejected"
    assert result.voucher.reason == "citation_marker"


def test_reject_non_fungal_species_candidate(validator: Phase5Validator):
    row = _base_row(Species="Fomitiporia sp.")
    enr = DummyEnrichment("JQ087932", "pdf_table", found_species="Arabidopsis thaliana")

    original_check = validator._is_fungal_taxon
    try:
        validator._is_fungal_taxon = lambda _: False
        result = validator.validate(enr, row, voucher_dict={})
        assert result.species.status == "rejected"
        assert result.species.reason == "non_fungal_taxon"
    finally:
        validator._is_fungal_taxon = original_check


def test_species_voucher_is_removed_before_fungal_check(validator: Phase5Validator):
    row = _base_row(Species="Wrightoporia sp. B1a0905EM2CC429")
    enr = DummyEnrichment("JQ087932", "pdf_table", found_species="Wrightoporia sp. KUC20110922-37")

    called = {"count": 0}

    original_check = validator._is_fungal_taxon
    try:
        def _fail_if_called(_name):
            called["count"] += 1
            return True

        validator._is_fungal_taxon = _fail_if_called
        result = validator.validate(enr, row, voucher_dict={})

        # Após remover voucher embutido, continua sendo "Genus sp." (incompleto),
        # então não deve chegar ao filtro fúngico.
        assert result.species.status == "rejected"
        assert result.species.reason == "candidate_incomplete"
        assert called["count"] == 0
    finally:
        validator._is_fungal_taxon = original_check


def run_tests() -> tuple[int, int]:
    validator = Phase5Validator()

    tests = [
        test_voucher_fill_when_missing,
        test_voucher_conflict_updates_dict,
        test_country_ambiguous_not_applied,
        test_species_only_updates_if_incomplete,
        test_reject_citation_marker_as_voucher,
        test_reject_non_fungal_species_candidate,
        test_species_voucher_is_removed_before_fungal_check,
    ]

    passed = 0
    failed = 0

    for test_fn in tests:
        try:
            test_fn(validator)
            print(f"PASS: {test_fn.__name__}")
            passed += 1
        except AssertionError as exc:
            print(f"FAIL: {test_fn.__name__} -> {exc}")
            failed += 1
        except Exception as exc:
            print(f"ERROR: {test_fn.__name__} -> {exc}")
            failed += 1

    print("=" * 60)
    print(f"Phase5 tests: {passed} passed / {failed} failed")
    print("=" * 60)
    return passed, failed


if __name__ == "__main__":
    _, failed = run_tests()
    raise SystemExit(1 if failed else 0)

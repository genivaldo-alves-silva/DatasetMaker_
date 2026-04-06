#!/usr/bin/env python3
"""
Testes rápidos para Fase 6 (fallback country) e Fase 7 (consolidação).

Uso:
  python -m get_taxon_ref_.test_phase6_phase7
"""

import pandas as pd

from get_taxon_ref_ import phase6_gbif_fallback as p6
from get_taxon_ref_ import phase7_consolidation as p7


def test_phase6_with_stubbed_gbif():
    df = pd.DataFrame(
        {
            "Species": ["A sp.", "A robusta"],
            "voucher": ["CBS 123", "X-999"],
            "geo_loc_name": ["", "Brazil"],
            "ITS": ["AB123456", "AB999999"],
        }
    )

    voucher_dict = {"CBS123": ["CBS 123", "CBS:123"]}

    original_gbif = p6._query_gbif_country
    original_idigbio = p6._query_idigbio_country

    try:
        p6._query_gbif_country = (
            lambda v, species=None, timeout=12, include_publishing_country=False: "Mexico" if "CBS" in v else None
        )
        p6._query_idigbio_country = lambda v, timeout=12: None

        out, report = p6.fill_missing_countries_with_fallbacks(df, voucher_dict=voucher_dict)

        assert out.loc[0, "geo_loc_name"] == "Mexico"
        assert report.countries_filled == 1
        assert report.gbif_hits == 1
    finally:
        p6._query_gbif_country = original_gbif
        p6._query_idigbio_country = original_idigbio


def test_phase7_consolidation_merges_rows():
    df = pd.DataFrame(
        {
            "Species": ["A sp.", "A robusta", "B robusta"],
            "voucher": ["CBS 123", "CBS:123", "X-9"],
            "geo_loc_name": ["", "Mexico", "Brazil"],
            "ITS": ["AB123456", "", "CD999999"],
            "TEF1": ["", "EF777777", ""],
            "seqITS": ["ATCG", "", "GGTT"],
        }
    )

    voucher_dict = {
        "CBS123": ["CBS 123", "CBS:123"],
        "X9": ["X-9"],
    }

    out, report = p7.consolidate_rows_by_voucher_dict(df, voucher_dict=voucher_dict)

    # Duas linhas do cluster CBS123 viram uma.
    assert len(out) == 2
    assert report.rows_removed == 1
    assert report.clusters_with_merges == 1

    merged_candidates = out[out["voucher"].astype(str).str.replace(" ", "", regex=False).str.replace(":", "", regex=False).str.upper() == "CBS123"]
    assert not merged_candidates.empty
    merged = merged_candidates.iloc[0]
    assert merged["geo_loc_name"] == "Mexico"
    assert merged["ITS"] == "AB123456"
    assert merged["TEF1"] == "EF777777"


def test_phase6_publishing_country_is_opt_in_only():
    item = {
        "country": None,
        "countryCode": None,
        "publishingCountry": "GB",
    }

    assert p6._extract_country_from_gbif_item(item) is None
    assert p6._extract_country_from_gbif_item(item, include_publishing_country=True) in {"United Kingdom", "GB"}


def test_phase6_matches_composite_voucher_across_fields():
    item = {
        "catalogNumber": "74360",
        "collectionCode": "FLOR",
        "institutionCode": "UFSC",
        "scientificName": "Diacanthodes Singer",
    }

    assert p6._gbif_item_matches_voucher(item, "FLOR 74360", species="Diacanthodes sp.") is True


def run_tests() -> tuple[int, int]:
    tests = [
        test_phase6_with_stubbed_gbif,
        test_phase7_consolidation_merges_rows,
        test_phase6_publishing_country_is_opt_in_only,
        test_phase6_matches_composite_voucher_across_fields,
    ]

    passed = 0
    failed = 0

    for test_fn in tests:
        try:
            test_fn()
            print(f"PASS: {test_fn.__name__}")
            passed += 1
        except AssertionError as exc:
            print(f"FAIL: {test_fn.__name__} -> {exc}")
            failed += 1
        except Exception as exc:
            print(f"ERROR: {test_fn.__name__} -> {exc}")
            failed += 1

    print("=" * 60)
    print(f"Phase6/7 tests: {passed} passed / {failed} failed")
    print("=" * 60)
    return passed, failed


if __name__ == "__main__":
    _, failed = run_tests()
    raise SystemExit(1 if failed else 0)

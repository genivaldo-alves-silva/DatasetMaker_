#!/usr/bin/env python3
"""
Testes da Fase 6.5 (country por parser narrativo de PDF).

Uso:
  python -m get_taxon_ref_.test_phase6_5_pdf_country
"""

from pathlib import Path

import pandas as pd

from get_taxon_ref_.phase2_articles_db import ArticleRecord
from get_taxon_ref_ import phase6_5_pdf_country as p65


class FakeArticlesDB:
    def __init__(self, article: ArticleRecord | None):
        self.article = article

    def find_by_gb_accession(self, gb_accession: str):
        # Simula resolucao por accession para qualquer GB da linha.
        return self.article


def test_phase6_5_fills_missing_country_from_pdf_pairs():
    df = pd.DataFrame(
        {
            "voucher": ["Robledo 1891"],
            "geo_loc_name": [""],
            "ITS": ["KC136220"],
        }
    )

    article = ArticleRecord(doi="10.1000/test", pdf_path=str(Path(__file__)))
    db = FakeArticlesDB(article)

    original_extract = p65._extract_pdf_pairs
    try:
        p65._extract_pdf_pairs = lambda _pdf: [
            {
                "voucher": "Robledo 1891",
                "voucher_norm": p65.normalize_voucher("Robledo 1891"),
                "country": "Argentina",
                "confidence": "medium",
                "method": "regex",
                "section_start": 10,
            }
        ]

        out, report, audit = p65.fill_missing_countries_from_pdf_prose(df, db, voucher_dict={})

        assert out.loc[0, "geo_loc_name"] == "Argentina"
        assert report.countries_filled == 1
        assert report.pdf_rows_with_matches == 1
        assert len(audit) == 1
        assert audit.iloc[0]["status"] == "applied"
        assert audit.iloc[0]["gb_accession"] == "KC136220"
    finally:
        p65._extract_pdf_pairs = original_extract


def test_phase6_5_marks_ambiguous_and_does_not_fill():
    df = pd.DataFrame(
        {
            "voucher": ["Robledo 1891"],
            "geo_loc_name": [""],
            "ITS": ["KC136220"],
        }
    )

    article = ArticleRecord(doi="10.1000/test", pdf_path=str(Path(__file__)))
    db = FakeArticlesDB(article)

    original_extract = p65._extract_pdf_pairs
    try:
        p65._extract_pdf_pairs = lambda _pdf: [
            {
                "voucher": "Robledo 1891",
                "voucher_norm": p65.normalize_voucher("Robledo 1891"),
                "country": "Argentina",
                "confidence": "medium",
                "method": "regex",
                "section_start": 10,
            },
            {
                "voucher": "Robledo 1891",
                "voucher_norm": p65.normalize_voucher("Robledo 1891"),
                "country": "Brazil",
                "confidence": "medium",
                "method": "regex",
                "section_start": 12,
            },
        ]

        out, report, audit = p65.fill_missing_countries_from_pdf_prose(df, db, voucher_dict={})

        assert out.loc[0, "geo_loc_name"] == ""
        assert report.countries_filled == 0
        assert report.pdf_rows_ambiguous == 1
        assert len(audit) == 1
        assert audit.iloc[0]["status"] == "ambiguous"
    finally:
        p65._extract_pdf_pairs = original_extract


def test_phase6_5_no_pdf_in_db_status():
    df = pd.DataFrame(
        {
            "voucher": ["Robledo 1891"],
            "geo_loc_name": [""],
            "ITS": ["KC136220"],
        }
    )

    # Artigo sem pdf_path deve cair em no_pdf_in_db.
    article = ArticleRecord(doi="10.1000/test", pdf_path="")
    db = FakeArticlesDB(article)

    out, report, audit = p65.fill_missing_countries_from_pdf_prose(df, db, voucher_dict={})

    assert out.loc[0, "geo_loc_name"] == ""
    assert report.no_pdf_in_db == 1
    assert len(audit) == 1
    assert audit.iloc[0]["status"] == "no_pdf_in_db"


def test_phase6_5_skips_when_country_already_present():
    df = pd.DataFrame(
        {
            "voucher": ["Robledo 1891"],
            "geo_loc_name": ["Argentina"],
            "ITS": ["KC136220"],
        }
    )

    db = FakeArticlesDB(None)
    out, report, audit = p65.fill_missing_countries_from_pdf_prose(df, db, voucher_dict={})

    assert out.loc[0, "geo_loc_name"] == "Argentina"
    assert report.total_missing_before == 0
    assert report.rows_processed == 0
    assert audit.empty


def test_phase6_5_matches_country_by_accession_context_when_voucher_empty():
    df = pd.DataFrame(
        {
            "voucher": [""],
            "geo_loc_name": [""],
            "ITS": ["KC136220.1"],
        }
    )

    article = ArticleRecord(doi="10.1000/test", pdf_path=str(Path(__file__)))
    db = FakeArticlesDB(article)

    original_extract = p65._extract_pdf_pairs
    original_extract_accession = p65._extract_accession_country_pairs_from_pdf
    try:
        p65._extract_pdf_pairs = lambda _pdf: []
        p65._extract_accession_country_pairs_from_pdf = lambda _pdf, _gbs: [
            {
                "gb_accession_norm": "KC136220",
                "country": "Argentina",
                "confidence": "low",
                "method": "pdf_accession_context",
                "section_start": 10,
            }
        ]

        out, report, audit = p65.fill_missing_countries_from_pdf_prose(df, db, voucher_dict={})

        assert out.loc[0, "geo_loc_name"] == "Argentina"
        assert report.countries_filled == 1
        assert len(audit) == 1
        assert audit.iloc[0]["status"] == "applied"
        assert audit.iloc[0]["method"] == "pdf_accession_context"
    finally:
        p65._extract_pdf_pairs = original_extract
        p65._extract_accession_country_pairs_from_pdf = original_extract_accession


def test_phase6_5_uses_llm_fallback_when_regex_has_no_match():
    df = pd.DataFrame(
        {
            "voucher": ["Robledo 1891"],
            "geo_loc_name": [""],
            "ITS": ["KC136220"],
        }
    )

    article = ArticleRecord(doi="10.1000/test", pdf_path=str(Path(__file__)))
    db = FakeArticlesDB(article)

    original_extract_regex = p65._extract_pdf_pairs
    original_extract_llm = p65._extract_pdf_pairs_llm
    try:
        p65._extract_pdf_pairs = lambda _pdf: []
        p65._extract_pdf_pairs_llm = lambda *args, **kwargs: [
            {
                "voucher": "Robledo 1891",
                "voucher_norm": p65.normalize_voucher("Robledo 1891"),
                "country": "Argentina",
                "confidence": "low",
                "method": "llm",
                "section_start": 20,
            }
        ]

        out, report, audit = p65.fill_missing_countries_from_pdf_prose(
            df,
            db,
            voucher_dict={},
            enable_llm_fallback=True,
            llm_api_key="test-key",
        )

        assert out.loc[0, "geo_loc_name"] == "Argentina"
        assert report.countries_filled == 1
        assert report.llm_fallback_hits == 1
        assert len(audit) == 1
        assert audit.iloc[0]["method"] == "llm"
    finally:
        p65._extract_pdf_pairs = original_extract_regex
        p65._extract_pdf_pairs_llm = original_extract_llm


def run_tests() -> tuple[int, int]:
    tests = [
        test_phase6_5_fills_missing_country_from_pdf_pairs,
        test_phase6_5_marks_ambiguous_and_does_not_fill,
        test_phase6_5_no_pdf_in_db_status,
        test_phase6_5_skips_when_country_already_present,
        test_phase6_5_matches_country_by_accession_context_when_voucher_empty,
        test_phase6_5_uses_llm_fallback_when_regex_has_no_match,
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
    print(f"Phase6.5 tests: {passed} passed / {failed} failed")
    print("=" * 60)
    return passed, failed


if __name__ == "__main__":
    _, failed = run_tests()
    raise SystemExit(1 if failed else 0)

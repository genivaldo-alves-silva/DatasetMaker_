#!/usr/bin/env python3
"""
Testes da Fase 2 (ArticlesDatabase).

Uso:
  python -m get_taxon_ref_.test_phase2_articles_db
"""

from pathlib import Path
import tempfile

import numpy as np
import pandas as pd

from get_taxon_ref_.phase2_articles_db import ArticlesDatabase


def test_lookup_handles_numpy_array_accessions():
    with tempfile.TemporaryDirectory() as tmp:
        db = ArticlesDatabase(Path(tmp))

        db._index_df = pd.DataFrame(
            [
                {
                    "doi": "10.1000/test",
                    "title": "Test",
                    "year": 2024,
                    "journal": "J",
                    "gb_accessions": np.array(["MK913641", "MK913637"], dtype=object),
                    "species_mentioned": [],
                    "vouchers_found": [],
                    "countries_found": [],
                    "has_gb_table": True,
                    "has_supplementary": False,
                    "pdf_downloaded": True,
                    "json_path": "",
                    "processed_date": "",
                }
            ]
        )

        assert db.has_gb_accession("MK913641") is True


def test_stats_counts_array_accessions():
    with tempfile.TemporaryDirectory() as tmp:
        db = ArticlesDatabase(Path(tmp))

        db._index_df = pd.DataFrame(
            [
                {
                    "doi": "10.1000/test",
                    "title": "Test",
                    "year": 2024,
                    "journal": "J",
                    "gb_accessions": np.array(["A1", "A2", "A3"], dtype=object),
                    "species_mentioned": [],
                    "vouchers_found": [],
                    "countries_found": [],
                    "has_gb_table": True,
                    "has_supplementary": False,
                    "pdf_downloaded": True,
                    "json_path": "",
                    "processed_date": "",
                }
            ]
        )

        stats = db.stats()
        assert stats["total_gb_codes"] == 3


def run_tests() -> tuple[int, int]:
    tests = [
        test_lookup_handles_numpy_array_accessions,
        test_stats_counts_array_accessions,
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
    print(f"Phase2 tests: {passed} passed / {failed} failed")
    print("=" * 60)
    return passed, failed


if __name__ == "__main__":
    _, failed = run_tests()
    raise SystemExit(1 if failed else 0)

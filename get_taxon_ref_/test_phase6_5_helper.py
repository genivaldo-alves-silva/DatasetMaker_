#!/usr/bin/env python3
"""
Testes da extração narrativa (phase6_5_helper).

Uso:
    python -m get_taxon_ref_.test_phase6_5_helper
"""

from get_taxon_ref_.phase6_5_helper import (
    extract_pairs_regex,
    find_specimen_sections,
)


def test_typification_country_region_block_not_truncated():
    lines = [
        "MycoBank accession. MB 830968.",
        "Typification.",
        "COLOMBIA.",
        "CESAR:",
        "Valledupar,",
        "Santuario de Vida Silvestre Los Besotes, Tropical Dry",
        "Forest, 1034030.900N, 73160 61.600W, 692m asl, 15 Sep",
        "2012, Palacio 105 (holotype, HUA185578), GenBank",
        "accession numbers: ITS ¼ MK913640, 28S¼ MK913636.",
        "Etymology. Referring to the neotropical region.",
    ]

    sections = find_specimen_sections(lines, "mock.pdf")
    assert len(sections) >= 1

    joined = "\n".join(s.text for s in sections)
    assert "Palacio 105" in joined
    assert "MK913640" in joined

    pairs = []
    for s in sections:
        pairs.extend(extract_pairs_regex("mock.pdf", s))

    assert any(p.voucher == "Palacio 105" and p.country == "Colombia" for p in pairs)


def run_tests() -> tuple[int, int]:
    tests = [
        test_typification_country_region_block_not_truncated,
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
    print(f"Phase4 prose tests: {passed} passed / {failed} failed")
    print("=" * 60)
    return passed, failed


if __name__ == "__main__":
    _, failed = run_tests()
    raise SystemExit(1 if failed else 0)

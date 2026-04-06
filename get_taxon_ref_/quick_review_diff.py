#!/usr/bin/env python3
"""
Gera CSVs de revisão rápida entre parquet original e parquet enriquecido.

Saídas:
1) <prefix>_modified_enriched.csv
   - linhas do enriched que tiveram alteração relevante vs original.
2) <prefix>_modified_original.csv
   - as mesmas linhas, mas no estado do parquet original.
3) <prefix>_removed_from_enriched.csv
   - linhas que existiam no original e não existem no enriched (ex.: consolidação).
4) <prefix>_summary.txt
    - resumo numérico do diff.
5) <prefix>_delta_only.csv
    - diferenças explícitas por linha/campo (valor original vs enriquecido).

Regra de criação de arquivos de diferença:
- Se NÃO houver mudança importante (Species/voucher/country/title)
  E NÃO houver linhas removidas por consolidação,
  os arquivos de diferença NÃO são criados.

Uso:
  python -m get_taxon_ref_.quick_review_diff \
    --original <orig.parquet> \
    --enriched <enriched.parquet> \
    --out-dir <dir_saida> \
    --prefix Wrightoporia_review
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_MARKERS = ["ITS", "28S", "TEF1", "RPB1", "RPB2", "nrSSU", "nrLSU"]
DEFAULT_COMPARE = ["Species", "voucher", "geo_loc_name", "country", "title"]


def _norm(v) -> str:
    s = str(v).strip()
    if s.lower() in {"", "nan", "none"}:
        return ""
    return s


def _build_signature(row: pd.Series, marker_cols: list[str]) -> tuple:
    pairs = []
    for c in marker_cols:
        v = _norm(row.get(c, ""))
        if v:
            pairs.append((c, v))
    return tuple(pairs)


def _pick_marker_columns(df: pd.DataFrame) -> list[str]:
    cols = []
    for c in DEFAULT_MARKERS:
        if c in df.columns:
            cols.append(c)
    return cols


def _pick_compare_columns(orig: pd.DataFrame, enr: pd.DataFrame) -> list[str]:
    cols = []
    for c in DEFAULT_COMPARE:
        if c in orig.columns and c in enr.columns:
            cols.append(c)
    return cols


def _pick_audit_columns(enr: pd.DataFrame) -> list[str]:
    cols = []
    for c in ["validation_source", "validation_notes", "voucher_conflict", "country_candidates"]:
        if c in enr.columns and c not in cols:
            cols.append(c)
    return cols


def _first_non_empty_row(rows: list[pd.Series], marker_cols: list[str]) -> pd.Series:
    # Preferir linha com mais marcadores preenchidos
    best = rows[0]
    best_n = sum(1 for c in marker_cols if _norm(best.get(c, "")))
    for r in rows[1:]:
        n = sum(1 for c in marker_cols if _norm(r.get(c, "")))
        if n > best_n:
            best = r
            best_n = n
    return best


def generate_review_csvs(
    original_path: Path,
    enriched_path: Path,
    out_dir: Path,
    prefix: str,
) -> dict:
    orig = pd.read_parquet(original_path)
    enr = pd.read_parquet(enriched_path)

    marker_cols = _pick_marker_columns(orig)
    marker_cols = [c for c in marker_cols if c in enr.columns]
    if not marker_cols:
        raise ValueError("Nenhuma coluna marcador encontrada para assinatura (ex.: ITS, 28S, TEF1).")

    compare_cols = _pick_compare_columns(orig, enr)
    audit_cols = _pick_audit_columns(enr)

    # índice por assinatura (permite múltiplas linhas por assinatura)
    orig_map: dict[tuple, list[pd.Series]] = {}
    enr_map: dict[tuple, list[pd.Series]] = {}

    for _, row in orig.iterrows():
        k = _build_signature(row, marker_cols)
        if k:
            orig_map.setdefault(k, []).append(row)

    for _, row in enr.iterrows():
        k = _build_signature(row, marker_cols)
        if k:
            enr_map.setdefault(k, []).append(row)

    common_keys = sorted(set(orig_map) & set(enr_map), key=lambda x: str(x))
    removed_keys = sorted(set(orig_map) - set(enr_map), key=lambda x: str(x))

    modified_enr_rows = []
    modified_orig_rows = []
    delta_rows = []
    audit_only_rows = []

    for k in common_keys:
        o = _first_non_empty_row(orig_map[k], marker_cols)
        e = _first_non_empty_row(enr_map[k], marker_cols)

        changed = False
        for c in compare_cols:
            ov = _norm(o.get(c, ""))
            ev = _norm(e.get(c, ""))
            if ov != ev:
                changed = True
                break

        if changed:
            enr_row = e.to_dict()
            orig_row = o.to_dict()
            enr_row["_review_signature"] = str(k)
            orig_row["_review_signature"] = str(k)
            modified_enr_rows.append(enr_row)
            modified_orig_rows.append(orig_row)

            for c in compare_cols:
                ov = _norm(o.get(c, ""))
                ev = _norm(e.get(c, ""))
                if ov != ev:
                    delta_rows.append(
                        {
                            "_review_signature": str(k),
                            "field": c,
                            "original_value": ov,
                            "enriched_value": ev,
                        }
                    )
        else:
            # Captura mudanças apenas de auditoria para o summary.
            for c in audit_cols:
                ov = _norm(o.get(c, ""))
                ev = _norm(e.get(c, ""))
                if ov != ev:
                    audit_only_rows.append(str(k))
                    break

    removed_rows = []
    for k in removed_keys:
        # todas as linhas originais desse grupo foram removidas no enriched
        for r in orig_map[k]:
            d = r.to_dict()
            d["_review_signature"] = str(k)
            removed_rows.append(d)

    out_dir.mkdir(parents=True, exist_ok=True)

    mod_enr_path = out_dir / f"{prefix}_modified_enriched.csv"
    mod_orig_path = out_dir / f"{prefix}_modified_original.csv"
    removed_path = out_dir / f"{prefix}_removed_from_enriched.csv"
    delta_path = out_dir / f"{prefix}_delta_only.csv"
    summary_path = out_dir / f"{prefix}_summary.txt"

    # Evita manter arquivos antigos quando não há diferenças importantes.
    for stale in [mod_enr_path, mod_orig_path, removed_path, delta_path]:
        if stale.exists():
            stale.unlink()

    has_important_diffs = bool(modified_enr_rows or removed_rows)
    if has_important_diffs:
        pd.DataFrame(modified_enr_rows).to_csv(mod_enr_path, index=False)
        pd.DataFrame(modified_orig_rows).to_csv(mod_orig_path, index=False)
        pd.DataFrame(removed_rows).to_csv(removed_path, index=False)
        pd.DataFrame(delta_rows).to_csv(delta_path, index=False)

    summary = {
        "original_rows": len(orig),
        "enriched_rows": len(enr),
        "delta_rows": len(orig) - len(enr),
        "signatures_common": len(common_keys),
        "signatures_removed": len(removed_keys),
        "rows_modified": len(modified_enr_rows),
        "rows_removed": len(removed_rows),
        "delta_cells": len(delta_rows),
        "audit_only_rows": len(set(audit_only_rows)),
        "has_important_diffs": has_important_diffs,
        "marker_columns": marker_cols,
        "compare_columns": compare_cols,
        "audit_columns": audit_cols,
        "modified_enriched_csv": str(mod_enr_path) if has_important_diffs else "",
        "modified_original_csv": str(mod_orig_path) if has_important_diffs else "",
        "removed_csv": str(removed_path) if has_important_diffs else "",
        "delta_csv": str(delta_path) if has_important_diffs else "",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        for k, v in summary.items():
            f.write(f"{k}: {v}\n")

    summary["summary_txt"] = str(summary_path)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gerar CSVs de revisão rápida original vs enriched.")
    parser.add_argument("--original", required=True, help="Caminho do parquet original")
    parser.add_argument("--enriched", required=True, help="Caminho do parquet enriquecido")
    parser.add_argument("--out-dir", required=True, help="Diretório de saída")
    parser.add_argument("--prefix", default="review", help="Prefixo dos arquivos de saída")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = generate_review_csvs(
        original_path=Path(args.original),
        enriched_path=Path(args.enriched),
        out_dir=Path(args.out_dir),
        prefix=args.prefix,
    )
    print("Review files generated:")
    if summary["has_important_diffs"]:
        print(summary["modified_enriched_csv"])
        print(summary["modified_original_csv"])
        print(summary["removed_csv"])
        print(summary["delta_csv"])
    else:
        print("No important differences detected. Diff files were not created.")
    print(summary["summary_txt"])


if __name__ == "__main__":
    main()

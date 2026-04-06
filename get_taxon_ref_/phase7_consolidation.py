"""
Fase 7: Consolidação de linhas por voucher_dict

Reorganiza linhas quando múltiplos vouchers representam o mesmo espécime,
unificando marcadores por cluster de voucher.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class Phase7Report:
    clusters_with_merges: int = 0
    rows_removed: int = 0
    fields_merged: int = 0


def normalize_token(value: str) -> str:
    return re.sub(r"[\s:\-_,.;()\[\]{}]", "", (value or "")).lower()


def is_missing(value) -> bool:
    if pd.isna(value):
        return True
    txt = str(value).strip()
    return not txt or txt.lower() in {"none", "nan"}


def _build_alias_to_canonical(voucher_dict: Optional[dict]) -> dict[str, str]:
    alias_to_canonical: dict[str, str] = {}
    if not isinstance(voucher_dict, dict):
        return alias_to_canonical

    for canonical, values in voucher_dict.items():
        canonical = str(canonical)
        alias_to_canonical[normalize_token(canonical)] = canonical

        if isinstance(values, list):
            for v in values:
                alias_to_canonical[normalize_token(str(v))] = canonical

    return alias_to_canonical


def _infer_merge_columns(df: pd.DataFrame) -> list[str]:
    # Conservador: consolida genes, sequências e alguns metadados observados.
    cols = []
    for col in df.columns:
        low = col.lower()
        if low.startswith("seq"):
            cols.append(col)
            continue

        if low in {
            "its", "tef1", "rpb1", "rpb2", "nrssu", "nrlsu", "28s", "18s",
            "mtssu", "tub", "atp6", "coi", "lsu", "ssu", "rpb", "tef"
        }:
            cols.append(col)
            continue

        if low in {"species", "geo_loc_name", "country", "title", "type_material", "host"}:
            cols.append(col)

    return cols


def consolidate_rows_by_voucher_dict(
    df: pd.DataFrame,
    voucher_dict: Optional[dict],
    voucher_col: str = "voucher",
    merge_columns: Optional[list[str]] = None,
) -> tuple[pd.DataFrame, Phase7Report]:
    """
    Consolida linhas que pertencem ao mesmo cluster de voucher.

    Regras:
    - Só consolida quando duas ou mais linhas caem no mesmo canonical voucher.
    - Mantém a primeira linha do cluster e funde campos vazios a partir das demais.
    - Não sobrescreve valores já preenchidos na linha base.
    """
    report = Phase7Report()

    if df.empty or voucher_col not in df.columns or not isinstance(voucher_dict, dict):
        return df, report

    alias_to_canonical = _build_alias_to_canonical(voucher_dict)
    if not alias_to_canonical:
        return df, report

    merge_columns = merge_columns or _infer_merge_columns(df)
    merge_columns = [c for c in merge_columns if c in df.columns and c != voucher_col]

    groups: dict[str, list[int]] = {}

    for idx in df.index:
        voucher_raw = df.at[idx, voucher_col]
        if is_missing(voucher_raw):
            continue

        key_norm = normalize_token(str(voucher_raw))
        canonical = alias_to_canonical.get(key_norm)
        if not canonical:
            continue

        groups.setdefault(canonical, []).append(idx)

    if not groups:
        return df, report

    to_drop = []

    for canonical, idxs in groups.items():
        if len(idxs) <= 1:
            continue

        report.clusters_with_merges += 1
        base_idx = idxs[0]

        # padroniza voucher da linha base para o canonical
        if is_missing(df.at[base_idx, voucher_col]) or normalize_token(str(df.at[base_idx, voucher_col])) != normalize_token(canonical):
            df.at[base_idx, voucher_col] = canonical

        for src_idx in idxs[1:]:
            for col in merge_columns:
                if is_missing(df.at[base_idx, col]) and not is_missing(df.at[src_idx, col]):
                    df.at[base_idx, col] = df.at[src_idx, col]
                    report.fields_merged += 1

            to_drop.append(src_idx)

    if to_drop:
        df = df.drop(index=to_drop)
        report.rows_removed = len(to_drop)

    # Mantém índice limpo para evitar buracos após merges.
    df = df.reset_index(drop=True)

    return df, report


__all__ = [
    "Phase7Report",
    "consolidate_rows_by_voucher_dict",
]

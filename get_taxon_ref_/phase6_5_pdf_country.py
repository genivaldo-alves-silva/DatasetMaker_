"""
Fase 6.5: Fallback de country via parser narrativo de PDF

Executa APOS a Fase 6 (GBIF/iDigBio) e ANTES da Fase 7 (consolidacao).
Objetivo: preencher apenas countries ainda vazios, sem sobrescrever valores existentes.

Fonte: seções narrativas de especimes no PDF (ex.: Material examined, Specimens studied).
Saida adicional: auditoria por par (voucher, gb_accession) -> country.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from .phase2_articles_db import ArticlesDatabase
from .phase6_5_helper import (
    LLM_API_KEY,
    _extract_country_candidates,
    extract_pairs_llm,
    extract_pairs_regex,
    find_specimen_sections,
    find_specimen_sections_continuous,
    load_pdf_lines,
    normalize_voucher,
    _collapse_lines_to_text,
)
from .phase6_gbif_fallback import (
    _build_voucher_alias_map,
    _voucher_candidates,
    is_country_empty,
)

logger = logging.getLogger(__name__)


@dataclass
class Phase6_5Report:
    total_missing_before: int = 0
    rows_processed: int = 0
    countries_filled: int = 0
    pdf_rows_with_matches: int = 0
    pdf_rows_ambiguous: int = 0
    no_pdf_in_db: int = 0
    parse_errors: int = 0
    pairs_extracted_total: int = 0
    llm_fallback_hits: int = 0


def _extract_row_gb_codes(row: pd.Series) -> list[str]:
    """Extrai accessions GenBank de uma linha de forma conservadora."""
    gb_re = re.compile(r"^[A-Z]{1,3}\d{5,8}(?:\.\d+)?$", re.IGNORECASE)
    codes: list[str] = []

    for col, raw in row.items():
        if pd.isna(raw):
            continue
        txt = str(raw).strip()
        if not txt:
            continue

        # Tokeniza para aceitar celulas com separadores.
        tokens = re.split(r"[|,;\s]+", txt)
        for token in tokens:
            token = token.strip()
            if not token:
                continue
            token_clean = re.sub(r"[()\[\]{}]", "", token)
            if gb_re.match(token_clean):
                if token_clean not in codes:
                    codes.append(token_clean)

    return codes


def _gb_lookup_candidates(accession: str) -> list[str]:
    """Return DB lookup candidates for an accession, including versionless form."""
    acc = str(accession or "").strip()
    if not acc:
        return []

    candidates = [acc]
    base = acc.split(".")[0]
    if base and base not in candidates:
        candidates.append(base)
    return candidates


def _normalize_accession(value: str) -> str:
    """Normalize accession for lightweight comparisons across sources."""
    return str(value or "").strip().upper().split(".")[0]


def _extract_accession_set(values: list[str]) -> set[str]:
    out = set()
    for v in values:
        nv = _normalize_accession(v)
        if nv:
            out.add(nv)
    return out


def _voucher_candidates_from_article_for_gb(article, gb_codes: list[str]) -> list[str]:
    """Deprecated: voucher recovery via accession context was removed to reduce false positives."""
    return []


def _extract_accession_country_pairs_from_pdf(pdf_path: Path, gb_codes: list[str]) -> list[dict]:
    """Fallback for voucher-empty rows: match accession in narrative sections and infer country.

    Conservative policy:
    - consider only sections that explicitly contain the accession token;
    - accept only when exactly one country candidate is found in that section.
    """
    lines = load_pdf_lines(pdf_path)
    if not lines:
        return []

    sections = find_specimen_sections(lines, pdf_path.name)
    if not sections:
        text = _collapse_lines_to_text(lines)
        sections = find_specimen_sections_continuous(text, pdf_path.name)

    targets = _extract_accession_set(gb_codes)
    if not targets:
        return []

    out: list[dict] = []
    seen = set()

    for section in sections:
        section_text = str(section.text or "")
        if not section_text:
            continue

        countries = _extract_country_candidates(section_text)
        if len(countries) != 1:
            continue
        country = countries[0]

        for gb in targets:
            pat = re.compile(rf"(?<![A-Z0-9]){re.escape(gb)}(?:\.\d+)?(?![A-Z0-9])", re.IGNORECASE)
            if not pat.search(section_text):
                continue

            key = (gb, country.lower())
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "gb_accession_norm": gb,
                    "country": country,
                    "confidence": "low",
                    "method": "pdf_accession_context",
                    "section_start": section.start_line,
                }
            )

    return out


def _extract_pdf_pairs(pdf_path: Path) -> list[dict]:
    """Retorna pares extraidos do PDF no formato compacto para matching."""
    lines = load_pdf_lines(pdf_path)
    if not lines:
        return []

    sections = find_specimen_sections(lines, pdf_path.name)
    if not sections:
        text = _collapse_lines_to_text(lines)
        sections = find_specimen_sections_continuous(text, pdf_path.name)

    extracted: list[dict] = []
    for section in sections:
        for pair in extract_pairs_regex(pdf_path.name, section):
            extracted.append(
                {
                    "voucher": pair.voucher,
                    "voucher_norm": normalize_voucher(pair.voucher),
                    "country": pair.country,
                    "confidence": pair.confidence,
                    "method": pair.method,
                    "section_start": pair.section_start,
                }
            )

    # Deduplicacao por voucher_norm+country para evitar inflar ambiguidades.
    dedup: list[dict] = []
    seen = set()
    for p in extracted:
        key = (p["voucher_norm"], str(p["country"]).strip().lower())
        if key in seen:
            continue
        seen.add(key)
        dedup.append(p)

    return dedup


def _extract_pdf_pairs_llm(
    pdf_path: Path,
    candidate_vouchers: list[str],
    llm_model: str,
    llm_api_key: str,
    llm_timeout: int,
    llm_retries: int,
) -> list[dict]:
    """Extract voucher-country pairs from PDF using LLM fallback."""
    lines = load_pdf_lines(pdf_path)
    if not lines:
        return []

    sections = find_specimen_sections(lines, pdf_path.name)
    if not sections:
        text = _collapse_lines_to_text(lines)
        sections = find_specimen_sections_continuous(text, pdf_path.name)

    extracted: list[dict] = []
    for section in sections:
        llm_pairs = extract_pairs_llm(
            pdf_name=pdf_path.name,
            section=section,
            model=llm_model,
            api_key=llm_api_key,
            candidate_vouchers=candidate_vouchers,
            timeout=llm_timeout,
            retries=llm_retries,
        )
        for pair in llm_pairs:
            extracted.append(
                {
                    "voucher": pair.voucher,
                    "voucher_norm": normalize_voucher(pair.voucher),
                    "country": pair.country,
                    "confidence": pair.confidence,
                    "method": pair.method,
                    "section_start": pair.section_start,
                }
            )

    dedup: list[dict] = []
    seen = set()
    for p in extracted:
        key = (p["voucher_norm"], str(p["country"]).strip().lower())
        if key in seen:
            continue
        seen.add(key)
        dedup.append(p)

    return dedup


def fill_missing_countries_from_pdf_prose(
    df: pd.DataFrame,
    articles_db: ArticlesDatabase,
    voucher_dict: Optional[dict] = None,
    country_col: Optional[str] = None,
    voucher_col: str = "voucher",
    enable_accession_context_fallback: bool = True,
    enable_llm_fallback: bool = True,
    llm_model: str = "meta-llama/llama-3.1-8b-instruct:free",
    llm_api_key: Optional[str] = None,
    llm_timeout: int = 45,
    llm_retries: int = 2,
) -> tuple[pd.DataFrame, Phase6_5Report, pd.DataFrame]:
    """
    Fase 6.5: preencher countries faltantes a partir de PDF narrativo.

    Regras:
    - So processa linhas com country vazio.
    - Nao sobrescreve country existente.
    - Pareamento principal por voucher da linha (e aliases do voucher_dict).
    - Gera auditoria por par (voucher, gb_accession) antes da Fase 7.
    """
    if df.empty:
        audit = pd.DataFrame(
            columns=[
                "row_index",
                "doi",
                "pdf_path",
                "voucher",
                "gb_accession",
                "country_extracted",
                "confidence",
                "method",
                "status",
                "note",
            ]
        )
        return df, Phase6_5Report(), audit

    df = df.copy()
    report = Phase6_5Report()

    if country_col is None:
        country_col = "geo_loc_name" if "geo_loc_name" in df.columns else "country"

    if country_col not in df.columns or voucher_col not in df.columns:
        return df, report, pd.DataFrame()

    missing_idx = [idx for idx in df.index if is_country_empty(df.at[idx, country_col])]
    report.total_missing_before = len(missing_idx)

    if not missing_idx:
        return df, report, pd.DataFrame()

    alias_map = _build_voucher_alias_map(voucher_dict)
    pdf_pairs_cache: dict[str, list[dict]] = {}
    llm_pairs_cache: dict[tuple[str, tuple[str, ...]], list[dict]] = {}
    counted_pdf_keys: set[str] = set()
    llm_api_key = llm_api_key or LLM_API_KEY or ""

    audit_rows: list[dict] = []

    for idx in missing_idx:
        report.rows_processed += 1

        voucher_raw = "" if pd.isna(df.at[idx, voucher_col]) else str(df.at[idx, voucher_col]).strip()
        candidate_vouchers = _voucher_candidates(voucher_raw, alias_map)
        candidate_norms = {normalize_voucher(v) for v in candidate_vouchers if str(v).strip()}

        gb_codes = _extract_row_gb_codes(df.loc[idx])
        if not gb_codes:
            audit_rows.append(
                {
                    "row_index": idx,
                    "doi": "",
                    "pdf_path": "",
                    "voucher": voucher_raw,
                    "gb_accession": "",
                    "country_extracted": "",
                    "confidence": "",
                    "method": "",
                    "status": "no_gb_code",
                    "note": "line_without_accession",
                }
            )
            continue

        # Resolve artigo via primeiro GB encontrado na linha.
        article = None
        matched_gb = None
        for gb in gb_codes:
            for gb_candidate in _gb_lookup_candidates(gb):
                article = articles_db.find_by_gb_accession(gb_candidate)
                if article:
                    matched_gb = gb_candidate
                    break
            if article:
                break

        if not article or not article.pdf_path:
            report.no_pdf_in_db += 1
            for gb in gb_codes:
                audit_rows.append(
                    {
                        "row_index": idx,
                        "doi": article.doi if article else "",
                        "pdf_path": article.pdf_path if article else "",
                        "voucher": voucher_raw,
                        "gb_accession": gb,
                        "country_extracted": "",
                        "confidence": "",
                        "method": "",
                        "status": "no_pdf_in_db",
                        "note": "article_or_pdf_not_available",
                    }
                )
            continue

        pdf_path = Path(article.pdf_path)
        if not pdf_path.exists():
            report.no_pdf_in_db += 1
            for gb in gb_codes:
                audit_rows.append(
                    {
                        "row_index": idx,
                        "doi": article.doi,
                        "pdf_path": str(pdf_path),
                        "voucher": voucher_raw,
                        "gb_accession": gb,
                        "country_extracted": "",
                        "confidence": "",
                        "method": "",
                        "status": "no_pdf_in_db",
                        "note": "pdf_path_missing_on_disk",
                    }
                )
            continue

        cache_key = str(pdf_path)
        if cache_key not in pdf_pairs_cache:
            try:
                pdf_pairs_cache[cache_key] = _extract_pdf_pairs(pdf_path)
            except Exception as exc:
                report.parse_errors += 1
                logger.warning("Fase 6.5 parse error em %s: %s", pdf_path, exc)
                pdf_pairs_cache[cache_key] = []

        pdf_pairs = pdf_pairs_cache[cache_key]
        if cache_key not in counted_pdf_keys:
            report.pairs_extracted_total += len(pdf_pairs)
            counted_pdf_keys.add(cache_key)

        # Fallback 2: linha sem voucher -> usa accession para match no parser narrativo
        # e inferir country de forma conservadora (sem recuperar voucher por contexto).
        accession_country_pairs = []
        if enable_accession_context_fallback and not candidate_norms:
            accession_country_pairs = _extract_accession_country_pairs_from_pdf(pdf_path, gb_codes)

        matched_pairs = []
        if candidate_norms:
            matched_pairs = [p for p in pdf_pairs if p.get("voucher_norm") in candidate_norms]

        # Fallback 3: se regex falhar, tenta LLM no mesmo PDF (quando API key disponivel).
        if not matched_pairs and enable_llm_fallback and llm_api_key:
            llm_key = (cache_key, tuple(sorted(candidate_norms)))
            if llm_key not in llm_pairs_cache:
                llm_pairs_cache[llm_key] = _extract_pdf_pairs_llm(
                    pdf_path=pdf_path,
                    candidate_vouchers=candidate_vouchers,
                    llm_model=llm_model,
                    llm_api_key=llm_api_key,
                    llm_timeout=llm_timeout,
                    llm_retries=llm_retries,
                )

            llm_pairs = llm_pairs_cache[llm_key]
            if candidate_norms:
                matched_pairs = [p for p in llm_pairs if p.get("voucher_norm") in candidate_norms]
            elif len(llm_pairs) == 1:
                # Sem voucher e com unico candidato LLM no artigo: aceita como fallback conservador.
                matched_pairs = llm_pairs

            if matched_pairs:
                report.llm_fallback_hits += 1

        # Sem voucher: usa pares accession->country do parser narrativo.
        if not matched_pairs and accession_country_pairs:
            countries = {
                str(p.get("country", "")).strip()
                for p in accession_country_pairs
                if str(p.get("country", "")).strip()
            }

            if len(countries) == 1:
                country_value = next(iter(countries))
                if is_country_empty(df.at[idx, country_col]):
                    df.at[idx, country_col] = country_value
                    report.countries_filled += 1
                    report.pdf_rows_with_matches += 1
                    status = "applied"
                else:
                    status = "skipped_existing_country"

                for gb in gb_codes:
                    audit_rows.append(
                        {
                            "row_index": idx,
                            "doi": article.doi,
                            "pdf_path": str(pdf_path),
                            "voucher": voucher_raw,
                            "gb_accession": gb,
                            "country_extracted": country_value,
                            "confidence": "low",
                            "method": "pdf_accession_context",
                            "status": status,
                            "note": "matched_by_accession_in_narrative_section",
                        }
                    )
                continue

            if len(countries) > 1:
                report.pdf_rows_ambiguous += 1
                country_list = "|".join(sorted(countries))
                for gb in gb_codes:
                    audit_rows.append(
                        {
                            "row_index": idx,
                            "doi": article.doi,
                            "pdf_path": str(pdf_path),
                            "voucher": voucher_raw,
                            "gb_accession": gb,
                            "country_extracted": country_list,
                            "confidence": "low",
                            "method": "pdf_accession_context",
                            "status": "ambiguous",
                            "note": "multiple_countries_for_accession_context",
                        }
                    )
                continue

        # Se nao houver voucher na linha, nao tenta inferir agressivamente.
        if not matched_pairs:
            for gb in gb_codes:
                audit_rows.append(
                    {
                        "row_index": idx,
                        "doi": article.doi,
                        "pdf_path": str(pdf_path),
                        "voucher": voucher_raw,
                        "gb_accession": gb,
                        "country_extracted": "",
                        "confidence": "",
                        "method": "pdf_prose_regex",
                        "status": "no_match",
                        "note": "no_voucher_country_match_in_pdf_or_llm",
                    }
                )
            continue

        countries = {str(p.get("country", "")).strip() for p in matched_pairs if str(p.get("country", "")).strip()}

        if len(countries) == 1:
            country_value = next(iter(countries))
            if is_country_empty(df.at[idx, country_col]):
                df.at[idx, country_col] = country_value
                report.countries_filled += 1
                report.pdf_rows_with_matches += 1
                status = "applied"
            else:
                status = "skipped_existing_country"

            # Usa o primeiro match como representativo de metodo/confianca.
            rep = matched_pairs[0]
            for gb in gb_codes:
                audit_rows.append(
                    {
                        "row_index": idx,
                        "doi": article.doi,
                        "pdf_path": str(pdf_path),
                        "voucher": voucher_raw,
                        "gb_accession": gb,
                        "country_extracted": country_value,
                        "confidence": rep.get("confidence", "medium"),
                        "method": rep.get("method", "pdf_prose_regex"),
                        "status": status,
                        "note": f"matched_via_{matched_gb or gb}",
                    }
                )
        else:
            report.pdf_rows_ambiguous += 1
            country_list = "|".join(sorted(countries))
            for gb in gb_codes:
                audit_rows.append(
                    {
                        "row_index": idx,
                        "doi": article.doi,
                        "pdf_path": str(pdf_path),
                        "voucher": voucher_raw,
                        "gb_accession": gb,
                        "country_extracted": country_list,
                        "confidence": "low",
                        "method": "pdf_prose_regex",
                        "status": "ambiguous",
                        "note": "multiple_countries_for_same_voucher",
                    }
                )

    audit_df = pd.DataFrame(audit_rows)
    return df, report, audit_df


__all__ = [
    "Phase6_5Report",
    "fill_missing_countries_from_pdf_prose",
]

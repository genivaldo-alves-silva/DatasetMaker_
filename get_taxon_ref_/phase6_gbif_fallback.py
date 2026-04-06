"""
Fase 6: Fallback GBIF/iDigBio para country

Preenche countries faltantes usando vouchers como chave de busca.
Prioridade desta fase: só roda para linhas ainda sem country após fases 3-5.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import requests

try:
    import pycountry
except Exception:  # pragma: no cover
    pycountry = None

logger = logging.getLogger(__name__)

GBIF_OCCURRENCE_URL = "https://api.gbif.org/v1/occurrence/search"
IDIGBIO_SEARCH_URL = "https://search.idigbio.org/v2/search/records"

DEFAULT_TIMEOUT = 12


@dataclass
class Phase6Report:
    total_missing_before: int = 0
    rows_processed: int = 0
    countries_filled: int = 0
    gbif_hits: int = 0
    idigbio_hits: int = 0


def normalize_voucher_token(value: str) -> str:
    return re.sub(r"[\s:\-_,.;()\[\]{}]", "", (value or "")).lower()


def is_country_empty(value) -> bool:
    if pd.isna(value):
        return True
    txt = str(value).strip()
    return not txt or txt.lower() in {"none", "nan"}


def _build_voucher_alias_map(voucher_dict: Optional[dict]) -> dict[str, list[str]]:
    """Mapeia qualquer alias normalizado para lista completa de variantes do cluster."""
    alias_map: dict[str, list[str]] = {}
    if not isinstance(voucher_dict, dict):
        return alias_map

    for canonical, values in voucher_dict.items():
        cluster = [str(canonical)]
        if isinstance(values, list):
            cluster.extend(str(v) for v in values)

        unique_cluster = []
        seen = set()
        for token in cluster:
            token = token.strip()
            if not token:
                continue
            if token not in seen:
                seen.add(token)
                unique_cluster.append(token)

        for token in unique_cluster:
            alias_map[normalize_voucher_token(token)] = unique_cluster

    return alias_map


def _voucher_candidates(voucher: str, alias_map: dict[str, list[str]]) -> list[str]:
    if not voucher:
        return []

    voucher = voucher.strip()
    candidates = [voucher]

    cluster = alias_map.get(normalize_voucher_token(voucher), [])
    for token in cluster:
        if token not in candidates:
            candidates.append(token)

    return candidates


def _query_gbif_country(
    voucher: str,
    species: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT,
    include_publishing_country: bool = False,
) -> Optional[str]:
    voucher = (voucher or "").strip()
    if not voucher:
        return None

    queries: list[dict] = [
        {"catalogNumber": voucher, "limit": 50},
        {"catalogNumber": voucher.replace(" ", ""), "limit": 50},
        {"catalogNumber": voucher.replace("-", ""), "limit": 50},
    ]

    # Se houver padrão "PREFIX 12345", tenta com institutionCode + catalogNumber.
    m = re.match(r"^([A-Za-z]{2,10})[\s\-:_]+([A-Za-z0-9]+)$", voucher)
    if m:
        prefix, number = m.group(1), m.group(2)
        queries.append({"institutionCode": prefix.upper(), "catalogNumber": number, "limit": 50})
        queries.append({"collectionCode": prefix.upper(), "catalogNumber": number, "limit": 50})

    # Último recurso: busca textual ampla, mas com filtro rigoroso por voucher.
    queries.append({"q": voucher, "limit": 100})

    for params in queries:
        try:
            resp = requests.get(GBIF_OCCURRENCE_URL, params=params, timeout=timeout)
            if resp.status_code != 200:
                continue

            data = resp.json()
            results = data.get("results", [])
            for item in results:
                if not _gbif_item_matches_voucher(item, voucher, species=species):
                    continue
                country = _extract_country_from_gbif_item(
                    item,
                    include_publishing_country=include_publishing_country,
                )
                if country:
                    return country
        except Exception as exc:
            logger.debug("GBIF query falhou para '%s' params=%s: %s", voucher, params, exc)

    return None


def _gbif_item_matches_voucher(item: dict, voucher: str, species: Optional[str] = None) -> bool:
    """Valida se o registro GBIF realmente corresponde ao voucher consultado."""
    target = normalize_voucher_token(voucher)
    if not target:
        return False

    alpha_chunks = [c.lower() for c in re.findall(r"[A-Za-z]+", voucher)]
    digit_chunks = re.findall(r"\d+", voucher)
    has_alpha = bool(alpha_chunks)
    has_digit = bool(digit_chunks)

    fields = [
        item.get("catalogNumber"),
        item.get("collectionCode"),
        item.get("institutionCode"),
        item.get("occurrenceID"),
        item.get("otherCatalogNumbers"),
        item.get("recordNumber"),
        item.get("fieldNumber"),
    ]

    # Alguns vouchers vêm de forma composta (ex.: "FLOR 74360"), com letras
    # e números distribuídos entre campos diferentes (collectionCode + catalogNumber).
    if has_alpha and has_digit:
        merged = normalize_voucher_token("".join(str(v) for v in fields if v is not None))
        if merged and all(ch in merged for ch in alpha_chunks) and all(ch in merged for ch in digit_chunks):
            if _gbif_item_matches_species(item, species):
                return True

    for val in fields:
        if val is None:
            continue
        text = str(val)
        norm = normalize_voucher_token(text)
        if not norm:
            continue

        # Caso mais seguro: match exato após normalização.
        if target == norm:
            return True

        # Match composto: precisa conter os blocos alfabéticos E numéricos.
        text_low = text.lower()
        if has_alpha and has_digit:
            if all(ch in text_low for ch in alpha_chunks) and all(ch in text_low for ch in digit_chunks):
                if _gbif_item_matches_species(item, species):
                    return True
            continue

        # Voucher com parte alfabética apenas: requer token inteiro.
        if has_alpha and not has_digit:
            for ch in alpha_chunks:
                if re.search(rf"\b{re.escape(ch)}\b", text_low):
                    if _gbif_item_matches_species(item, species):
                        return True
            continue

        # Voucher numérico puro: só aceita igualdade exata (já checada acima).

    return False


def _gbif_item_matches_species(item: dict, species: Optional[str]) -> bool:
    """Se espécie informada, exige compatibilidade mínima por gênero."""
    genus = _extract_genus(species)
    if not genus:
        return True

    sci = str(item.get("scientificName") or "").lower()
    if not sci:
        return True
    return genus.lower() in sci


def _extract_genus(species: Optional[str]) -> Optional[str]:
    if not species:
        return None
    first = str(species).strip().split()[0] if str(species).strip() else ""
    if not first or len(first) <= 2 or first.endswith('.'):
        return None
    if not re.match(r"^[A-Za-z-]+$", first):
        return None
    return first


def _extract_country_from_gbif_item(item: dict, include_publishing_country: bool = False) -> Optional[str]:
    country = (item.get("country") or "").strip()
    if country:
        return country

    # Política conservadora: usa countryCode apenas.
    # publishingCountry indica país publicador do dataset, não necessariamente
    # local de coleta, então fica desabilitado por padrão.
    code = (item.get("countryCode") or "").strip()
    if not code and include_publishing_country:
        code = (item.get("publishingCountry") or "").strip()

    if code and len(code) == 2:
        if pycountry is not None:
            try:
                return pycountry.countries.get(alpha_2=code.upper()).name
            except Exception:
                pass
        return code.upper()

    verbatim = (item.get("verbatimCountry") or "").strip()
    if verbatim:
        return verbatim

    return None


def _query_idigbio_country(voucher: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[str]:
    # Busca simples por catalognumber. Endpoint/shape pode variar entre versões,
    # por isso o parser é tolerante.
    payload = {
        "rq": {
            "catalognumber": voucher
        },
        "limit": 20,
    }

    try:
        resp = requests.post(IDIGBIO_SEARCH_URL, json=payload, timeout=timeout)
        if resp.status_code != 200:
            return None

        data = resp.json()
        items = data.get("items", [])
        for item in items:
            rec = item.get("indexTerms", {}) if isinstance(item, dict) else {}
            # campos usuais: country / dwc:country
            raw_country = rec.get("country") or rec.get("dwc:country") or ""
            if isinstance(raw_country, list):
                raw_country = raw_country[0] if raw_country else ""
            country = str(raw_country).strip()
            if country:
                return country
    except Exception as exc:
        logger.debug("iDigBio query falhou para '%s': %s", voucher, exc)

    return None


def fill_missing_countries_with_fallbacks(
    df: pd.DataFrame,
    voucher_dict: Optional[dict] = None,
    country_col: Optional[str] = None,
    voucher_col: str = "voucher",
    use_idigbio: bool = True,
    include_publishing_country: bool = False,
) -> tuple[pd.DataFrame, Phase6Report]:
    """
    Fase 6: preenche countries faltantes consultando GBIF/iDigBio por voucher.

    Returns:
        (df_atualizado, report)
    """
    if df.empty:
        return df, Phase6Report()

    df = df.copy()
    report = Phase6Report()

    if country_col is None:
        country_col = "geo_loc_name" if "geo_loc_name" in df.columns else "country"

    if country_col not in df.columns or voucher_col not in df.columns:
        return df, report

    missing_idx = [idx for idx in df.index if is_country_empty(df.at[idx, country_col])]
    report.total_missing_before = len(missing_idx)

    if not missing_idx:
        return df, report

    alias_map = _build_voucher_alias_map(voucher_dict)
    query_cache: dict[str, Optional[str]] = {}

    for idx in missing_idx:
        report.rows_processed += 1

        voucher_raw = df.at[idx, voucher_col]
        if pd.isna(voucher_raw):
            continue

        candidates = _voucher_candidates(str(voucher_raw), alias_map)
        if not candidates:
            continue

        found_country = None
        found_source = None

        for cand in candidates:
            ckey = f"gbif::{cand}"
            species_value = str(df.at[idx, "Species"]) if "Species" in df.columns else str(df.at[idx, "species"]) if "species" in df.columns else None

            if ckey not in query_cache:
                query_cache[ckey] = _query_gbif_country(
                    cand,
                    species=species_value,
                    include_publishing_country=include_publishing_country,
                )
            country = query_cache[ckey]
            if country:
                found_country = country
                found_source = "gbif"
                break

            if use_idigbio:
                ikey = f"idigbio::{cand}"
                if ikey not in query_cache:
                    query_cache[ikey] = _query_idigbio_country(cand)
                country = query_cache[ikey]
                if country:
                    found_country = country
                    found_source = "idigbio"
                    break

        if found_country:
            df.at[idx, country_col] = found_country
            report.countries_filled += 1
            if found_source == "gbif":
                report.gbif_hits += 1
            elif found_source == "idigbio":
                report.idigbio_hits += 1

    return df, report


__all__ = [
    "Phase6Report",
    "fill_missing_countries_with_fallbacks",
]

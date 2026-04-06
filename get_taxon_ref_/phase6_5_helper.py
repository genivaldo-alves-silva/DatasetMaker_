"""
Prototype benchmark for country extraction from taxonomic prose sections.

Goal
- Recover country values when table extraction has missing country.
- Compare deterministic regex extraction vs OpenRouter LLM extraction.
- Keep this as an isolated experiment before production integration.

Usage examples
1) Single PDF, regex only:
    python -m get_taxon_ref_.phase6_5_helper \
       --pdf get_taxon_ref_/downloads/10.1080_14772000.2020.1776784.pdf

2) Small batch + LLM benchmark:
    python -m get_taxon_ref_.phase6_5_helper \
       --pdf get_taxon_ref_/downloads/10.1080_14772000.2020.1776784.pdf \
       --pdf /path/to/other.pdf \
       --run-llm

3) Evaluate against gold CSV (columns: pdf,voucher,country):
    python -m get_taxon_ref_.phase6_5_helper \
       --pdf ... --run-llm --gold-csv /path/gold.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from dotenv import load_dotenv

from .phase4_pdf_extraction_v2 import extract_text_lines, resolve_country, resolve_country_extended

logger = logging.getLogger(__name__)


# --- Configuracao de ambiente (mesmo padrao de gb_handle.py) ---
# Usa o diretorio do script e carrega .env no diretorio pai.
base_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(base_dir, "..", ".env")
load_dotenv(dotenv_path)

# API key padrao vem da variavel de ambiente OPENROUTER_API_KEY.
LLM_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")


SECTION_START_PATTERNS = [
    re.compile(r"\bspecimens?\s+stud(?:ied|y)\b", re.IGNORECASE),
    re.compile(r"\bspecimens?\s+examined\b", re.IGNORECASE),
    re.compile(r"\bmaterials?\s+examined\b", re.IGNORECASE),
    re.compile(r"\bexamined\s+materials?\b", re.IGNORECASE),
    # Heading-oriented patterns to avoid matching generic mentions in
    # figure legends (e.g., "Type voucher specimens are indicated...").
    re.compile(r"(?:^|[.;]\s+)typification\s*[:.]", re.IGNORECASE),
    re.compile(r"(?:^|[.;]\s+)type\s*[:.]", re.IGNORECASE),
    re.compile(r"(?:^|[.;]\s+)holotype\s*[:.]", re.IGNORECASE),
    re.compile(r"\badditional\s+materials?\s+examined\b", re.IGNORECASE),
]

SECTION_STOP_PATTERNS = [
    re.compile(r"^(discussion|results?|conclusions?|references?)\b", re.IGNORECASE),
    re.compile(r"^table\s+\d+\b", re.IGNORECASE),
    re.compile(r"^figure\s+\d+\b", re.IGNORECASE),
    re.compile(r"^acknowledg", re.IGNORECASE),
    re.compile(r"^remarks?\b", re.IGNORECASE),
]

# Short taxon heading, e.g. "Diacanthodes coffeae (Wakef.) Robledo ...".
TAXON_HEADING_RE = re.compile(r"^[A-Z][a-z]+\s+[a-z][a-z\-]+\b")

# Surname + collection number (or code) patterns.
# Matches common strings such as "Robledo 1891", "Ryvarden 11293", "MES-4560".
VOUCHER_PATTERNS = [
    re.compile(r"\b([A-Z][A-Za-z'\-]+)\s+(\d{2,7}[A-Za-z]?)\b"),
    re.compile(r"\b([A-Z]{2,})[-\s:]?(\d{2,7}(?:\.\d+)?)\b"),
]

# Expands compact forms: "Robledo 1891, 1876" -> ["Robledo 1891", "Robledo 1876"]
COMPACT_SERIES_RE = re.compile(
    r"\b([A-Z][A-Za-z'\-]+)\s+(\d{2,7}[A-Za-z]?)(\s*,\s*\d{2,7}[A-Za-z]?)+\b"
)

# Country marker often appears as upper-case headline in specimen prose.
UPPER_COUNTRY_PREFIX_RE = re.compile(r"^([A-Z][A-Z\s\-]{2,})\s*[,;:]")

NON_COLLECTOR_TOKENS = {
    "Ibid",
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
}


def _is_non_collector_label(label: str) -> bool:
    """Return True when a label is likely not a collector name."""
    cleaned = re.sub(r"[^A-Za-z]", "", (label or "")).strip()
    if not cleaned:
        return True

    lower = cleaned.lower()
    # Month/date fragments are common false positives in specimen prose.
    if lower in {
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"
    }:
        return True

    return cleaned in NON_COLLECTOR_TOKENS


@dataclass
class SectionBlock:
    pdf: str
    title_line: int
    start_line: int
    end_line: int
    title: str
    text: str


@dataclass
class ExtractedPair:
    pdf: str
    method: str  # regex | llm
    section_start: int
    voucher: str
    country: str
    confidence: str
    context: str


def _normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def normalize_voucher(s: str) -> str:
    """Normalize voucher text for matching/evaluation."""
    compact = re.sub(r"[\s:/\-.,;()\[\]{}]", "", (s or "")).upper()
    return compact


def load_pdf_lines(pdf_path: Path, use_pdfplumber_fallback: bool = True) -> List[str]:
    """Read PDF text lines with PyMuPDF first; fallback to pdfplumber if requested."""
    try:
        return extract_text_lines(pdf_path)
    except Exception as exc:
        if not use_pdfplumber_fallback:
            raise
        logger.warning("PyMuPDF extraction failed for %s: %s", pdf_path.name, exc)

    try:
        import pdfplumber

        lines: List[str] = []
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for line in text.split("\n"):
                    line = line.strip()
                    if line:
                        lines.append(line)
        return lines
    except Exception as exc:
        logger.error("pdfplumber fallback failed for %s: %s", pdf_path.name, exc)
        return []


def find_specimen_sections(lines: Sequence[str], pdf_name: str) -> List[SectionBlock]:
    """Locate narrative specimen sections and return textual blocks."""
    sections: List[SectionBlock] = []
    i = 0
    n = len(lines)

    while i < n:
        line = _normalize_space(lines[i])
        if not line:
            i += 1
            continue

        next_line = _normalize_space(lines[i + 1]) if i + 1 < n else ""
        merged_2 = _normalize_space(f"{line} {next_line}")

        # Many PDFs split headings over 2 lines: "Specimen" + "studied.".
        if any(p.search(line) or p.search(merged_2) for p in SECTION_START_PATTERNS):
            start = i
            title = line
            heading_is_specimen_context = bool(
                re.search(r"typification|specimens?|materials?|holotype|paratype", merged_2, re.IGNORECASE)
            )
            j = i + 1
            while j < n:
                candidate = _normalize_space(lines[j])
                if any(p.search(candidate) for p in SECTION_STOP_PATTERNS):
                    break
                # Early stop if a new section-like heading appears and line is short.
                if len(candidate) < 60 and candidate.endswith(":") and j > i + 3:
                    # If this line already carries narrative payload (dates,
                    # vouchers, commas, periods), keep scanning.
                    if any(ch in candidate for ch in (",", ".")) or bool(re.search(r"\d", candidate)):
                        j += 1
                        continue

                    # Do not stop on locality fragments commonly found inside
                    # specimen prose, e.g., "Salta:", "Jujuy:", "Central:".
                    prev = _normalize_space(lines[j - 1]) if j - 1 >= 0 else ""
                    next_ = _normalize_space(lines[j + 1]) if j + 1 < n else ""
                    locality_label = bool(re.match(r"^[A-Z][A-Za-z\-\s]{1,30}:$", candidate))
                    locality_context = (
                        "ibid" in prev.lower()
                        or bool(next_.endswith(","))
                        or bool(re.match(r"^[A-Z][A-Z\s\-]{2,}\.?$", prev))
                        or bool(re.search(r"typification", title, re.IGNORECASE))
                        or bool(
                            next_
                            and re.search(
                                r"parque|sendero|national|nacional|asl|soil|forest|province|state|department|dept\.?|municipality|county|reserva|santuario",
                                next_,
                                re.IGNORECASE,
                            )
                        )
                    )
                    looks_like_locality = locality_label and locality_context
                    if not looks_like_locality:
                        break
                # Stop at likely new species/taxon heading to avoid bleeding into
                # subsequent sections.
                if (
                    j > i + 3
                    and len(candidate) < 140
                    and TAXON_HEADING_RE.match(candidate)
                    and not heading_is_specimen_context
                    and not re.search(r"\d", candidate)
                    and not candidate.endswith(",")
                ):
                    break
                j += 1

            text = " ".join(_normalize_space(x) for x in lines[start:j] if _normalize_space(x))
            sections.append(
                SectionBlock(
                    pdf=pdf_name,
                    title_line=i,
                    start_line=start,
                    end_line=j,
                    title=title,
                    text=text,
                )
            )
            i = j
            continue

        i += 1

    return sections


def _collapse_lines_to_text(lines: Sequence[str]) -> str:
    """Collapse extracted lines into one continuous text stream."""
    cleaned = [_normalize_space(str(x).replace("\n", " ")) for x in lines]
    cleaned = [x for x in cleaned if x]
    return " ".join(cleaned)


def find_specimen_sections_continuous(text: str, pdf_name: str) -> List[SectionBlock]:
    """Fallback section finder over continuous text (newline collapsed)."""
    normalized = _normalize_space(text)
    if not normalized:
        return []

    sections: List[SectionBlock] = []
    starts: List[tuple[int, str]] = []

    for pattern in SECTION_START_PATTERNS:
        for m in pattern.finditer(normalized):
            starts.append((m.start(), m.group(0)))

    if not starts:
        return []

    starts.sort(key=lambda x: x[0])
    for idx, (start_pos, title) in enumerate(starts):
        next_start = starts[idx + 1][0] if idx + 1 < len(starts) else len(normalized)
        tail = normalized[start_pos:next_start]

        stop_pos = len(tail)
        for stop_pattern in SECTION_STOP_PATTERNS:
            stop_match = stop_pattern.search(tail)
            if stop_match:
                stop_pos = min(stop_pos, stop_match.start())

        block_text = _normalize_space(tail[:stop_pos])
        if not block_text:
            continue

        sections.append(
            SectionBlock(
                pdf=pdf_name,
                title_line=-1,
                start_line=-1,
                end_line=-1,
                title=title,
                text=block_text,
            )
        )

    return sections


def _extract_country_candidates(text: str) -> List[str]:
    """Return country candidates from n-grams in a text fragment."""
    cleaned = _normalize_space(text)
    if not cleaned:
        return []

    found: List[str] = []

    prefix_match = UPPER_COUNTRY_PREFIX_RE.match(cleaned)
    if prefix_match:
        prefix_country = _resolve_country_token(prefix_match.group(1).title())
        if prefix_country:
            found.append(prefix_country)

    tokens = re.split(r"\s+", cleaned)
    max_n = min(4, len(tokens))
    for n in range(max_n, 0, -1):
        for i in range(0, len(tokens) - n + 1):
            gram = " ".join(tokens[i : i + n]).strip(" ,;:.()[]")
            country = _resolve_country_token(gram)
            if country and country not in found:
                found.append(country)

    return found


def _split_into_clauses(text: str) -> List[str]:
    """Split section text into clauses while preserving Ibid.-style narrative chains."""
    text = text.replace(" Ibid.", " ; Ibid.")
    text = text.replace(" Ibid,", " ; Ibid,")
    parts = re.split(r"[.;]\s+", text)
    return [_normalize_space(p) for p in parts if _normalize_space(p)]


def _extract_vouchers_from_clause(clause: str) -> List[str]:
    """Extract voucher-like mentions, including compact series expansions."""
    out: List[str] = []

    for match in COMPACT_SERIES_RE.finditer(clause):
        surname = match.group(1)
        if _is_non_collector_label(surname):
            continue
        first_num = match.group(2)
        out.append(f"{surname} {first_num}")

        suffix = match.group(3)
        extras = re.findall(r"\d{2,7}[A-Za-z]?", suffix)
        for num in extras:
            out.append(f"{surname} {num}")

    for pattern in VOUCHER_PATTERNS:
        for m in pattern.finditer(clause):
            if _is_non_collector_label(m.group(1)):
                continue
            voucher = f"{m.group(1)} {m.group(2)}"
            out.append(voucher)

    # Keep order but remove duplicates.
    dedup: List[str] = []
    seen = set()
    for voucher in out:
        nv = normalize_voucher(voucher)
        if nv and nv not in seen:
            seen.add(nv)
            dedup.append(_normalize_space(voucher))

    return dedup


def extract_pairs_regex(pdf_name: str, section: SectionBlock) -> List[ExtractedPair]:
    """Regex extractor with context propagation for Ibid.-style specimen prose."""
    clauses = _split_into_clauses(section.text)
    current_country: Optional[str] = None
    pairs: List[ExtractedPair] = []

    for clause in clauses:
        clause_countries = _extract_country_candidates(clause)
        if clause_countries:
            current_country = clause_countries[0]

        clause_vouchers = _extract_vouchers_from_clause(clause)
        if not clause_vouchers:
            continue

        country_here = clause_countries[0] if clause_countries else current_country
        if not country_here:
            continue

        for voucher in clause_vouchers:
            pairs.append(
                ExtractedPair(
                    pdf=pdf_name,
                    method="regex",
                    section_start=section.start_line,
                    voucher=voucher,
                    country=country_here,
                    confidence="medium",
                    context=clause[:300],
                )
            )

    return _dedup_pairs(pairs)


def _extract_context_windows(section_text: str, vouchers: Sequence[str], window_words: int = 35) -> Dict[str, str]:
    """Build per-voucher context windows with N words around each match."""
    words = re.findall(r"\S+", section_text)
    if not words:
        return {}

    windows: Dict[str, str] = {}
    text = " ".join(words)

    for voucher in vouchers:
        pat = re.compile(re.escape(voucher), re.IGNORECASE)
        match = pat.search(text)
        if not match:
            continue

        start_char = match.start()
        end_char = match.end()

        # Map char index to word index.
        running = 0
        start_w = 0
        end_w = len(words) - 1
        for idx, w in enumerate(words):
            next_running = running + len(w) + 1
            if running <= start_char < next_running:
                start_w = idx
            if running <= end_char <= next_running:
                end_w = idx
                break
            running = next_running

        left = max(0, start_w - window_words)
        right = min(len(words), end_w + window_words + 1)
        windows[voucher] = " ".join(words[left:right])

    return windows


def _call_openrouter(
    prompt: str,
    model: str,
    api_key: Optional[str],
    max_tokens: int = 500,
    timeout: int = 45,
    retries: int = 2,
    retry_backoff_s: float = 1.5,
) -> str:
    """Call OpenRouter chat completions and return raw content."""
    if not api_key:
        return ""

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": max_tokens,
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/DatasetMaker",
        "X-Title": "DatasetMaker",
    }

    attempts = max(1, retries + 1)
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(
                "https://openrouter.ai/api/v1/chat/completions",
                data=json.dumps(payload).encode("utf-8"),
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout) as response:
                result = json.loads(response.read().decode("utf-8"))
            return result.get("choices", [{}])[0].get("message", {}).get("content", "")
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError, KeyError) as exc:
            if attempt >= attempts:
                logger.warning(
                    "OpenRouter call failed after %d attempts (timeout=%ss): %s",
                    attempts,
                    timeout,
                    exc,
                )
                return ""
            sleep_s = retry_backoff_s * attempt
            logger.warning(
                "OpenRouter transient failure (attempt %d/%d): %s. Retrying in %.1fs...",
                attempt,
                attempts,
                exc,
                sleep_s,
            )
            time.sleep(sleep_s)
        except Exception as exc:
            logger.warning("OpenRouter unexpected error: %s", exc)
            return ""
    return ""


def extract_pairs_llm(
    pdf_name: str,
    section: SectionBlock,
    model: str,
    api_key: Optional[str],
    candidate_vouchers: Sequence[str],
    window_words: int = 35,
    max_tokens: int = 500,
    timeout: int = 45,
    retries: int = 2,
    retry_backoff_s: float = 1.5,
    candidate_batch_size: int = 4,
) -> List[ExtractedPair]:
    """LLM extractor (OpenRouter) using per-voucher context windows."""
    if not api_key:
        return []

    def _candidate_batches(items: Sequence[str], size: int) -> List[List[str]]:
        if not items:
            return [[]]
        n = max(1, size)
        return [list(items[i : i + n]) for i in range(0, len(items), n)]

    def _parse_pairs_from_llm(raw_content: str) -> List[dict]:
        """Parse JSON output robustly, with regex fallback for partial responses."""
        if not raw_content:
            return []

        content = raw_content.strip()
        content = re.sub(r"^```json\s*", "", content, flags=re.IGNORECASE)
        content = re.sub(r"^```\s*", "", content)
        content = re.sub(r"\s*```$", "", content)

        try:
            parsed = json.loads(content)
            pairs = parsed.get("pairs", []) if isinstance(parsed, dict) else []
            return pairs if isinstance(pairs, list) else []
        except json.JSONDecodeError:
            pass

        # Salvage 1: extract first complete JSON object if possible
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidate = content[start : end + 1]
            try:
                parsed = json.loads(candidate)
                pairs = parsed.get("pairs", []) if isinstance(parsed, dict) else []
                if isinstance(pairs, list):
                    return pairs
            except json.JSONDecodeError:
                pass

        # Salvage 2: regex extract repeated voucher-country objects from malformed JSON
        recovered = []
        for m in re.finditer(
            r'"voucher"\s*:\s*"([^"]+)"\s*,\s*"country"\s*:\s*"([^"]+)"',
            content,
            flags=re.IGNORECASE,
        ):
            recovered.append({"voucher": m.group(1), "country": m.group(2)})
        return recovered

    collected_items: List[dict] = []
    for batch in _candidate_batches(list(candidate_vouchers), candidate_batch_size):
        batch_contexts = _extract_context_windows(section.text, batch, window_words=window_words)
        if not batch_contexts:
            # Fallback: ask model over full section with no candidate restriction.
            batch_contexts = {"*": section.text[:3000]}

        prompt = (
            "Extract voucher-country pairs from taxonomic specimen prose.\n"
            "Return ONLY JSON with this schema:\n"
            "{\"pairs\": [{\"voucher\": \"...\", \"country\": \"...\"}]}\n"
            "Rules:\n"
            "- Prefer canonical country names (e.g., Argentina, Brazil, USA).\n"
            "- Do not return province/state as country.\n"
            "- If voucher appears as compact series (e.g., 'Robledo 1891, 1876'), expand it.\n"
            "- Ignore values that are not specimen vouchers.\n"
            "- If no pair is extractable, return {\"pairs\": []}.\n"
            f"Candidate vouchers: {json.dumps(batch, ensure_ascii=True)}\n"
            f"Context windows: {json.dumps(batch_contexts, ensure_ascii=True)}"
        )

        raw = _call_openrouter(
            prompt=prompt,
            model=model,
            api_key=api_key,
            max_tokens=max_tokens,
            timeout=timeout,
            retries=retries,
            retry_backoff_s=retry_backoff_s,
        )
        if not raw:
            continue

        collected_items.extend(_parse_pairs_from_llm(raw))

    pairs = collected_items
    out: List[ExtractedPair] = []
    for item in pairs:
        if not isinstance(item, dict):
            continue
        voucher = _normalize_space(str(item.get("voucher", "")))
        country = _normalize_space(str(item.get("country", "")))
        if not voucher or not country:
            continue

        resolved = _resolve_country_token(country)
        if not resolved:
            continue

        out.append(
            ExtractedPair(
                pdf=pdf_name,
                method="llm",
                section_start=section.start_line,
                voucher=voucher,
                country=resolved,
                confidence="low",
                context="llm_context_window",
            )
        )

    return _dedup_pairs(out)


def _dedup_pairs(pairs: Iterable[ExtractedPair]) -> List[ExtractedPair]:
    deduped: List[ExtractedPair] = []
    seen = set()
    for p in pairs:
        key = (p.pdf, p.method, normalize_voucher(p.voucher), p.country)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)
    return deduped


_USE_EXTENDED_COUNTRY_RESOLVER = False


def _resolve_country_token(value: str) -> Optional[str]:
    """Resolve country using configured mode (strict by default)."""
    if _USE_EXTENDED_COUNTRY_RESOLVER:
        return resolve_country_extended(value)
    return resolve_country(value)


def evaluate_against_gold(extracted: pd.DataFrame, gold: pd.DataFrame) -> pd.DataFrame:
    """Evaluate extracted pairs against gold CSV with columns pdf,voucher,country."""
    required = {"pdf", "voucher", "country"}
    if not required.issubset(gold.columns):
        raise ValueError("gold CSV must include columns: pdf,voucher,country")

    e = extracted.copy()
    g = gold.copy()

    e["voucher_norm"] = e["voucher"].map(normalize_voucher)
    g["voucher_norm"] = g["voucher"].map(normalize_voucher)

    merged = e.merge(
        g[["pdf", "voucher_norm", "country"]].rename(columns={"country": "gold_country"}),
        on=["pdf", "voucher_norm"],
        how="left",
    )

    def classify(row: pd.Series) -> str:
        gold_country = row.get("gold_country")
        if pd.isna(gold_country):
            return "false_positive"
        if str(row["country"]).strip().lower() == str(gold_country).strip().lower():
            return "match"
        return "country_mismatch"

    merged["status"] = merged.apply(classify, axis=1)
    return merged


def _to_dataframe(records: Sequence[ExtractedPair]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=["pdf", "method", "section_start", "voucher", "country", "confidence", "context"])
    return pd.DataFrame([asdict(r) for r in records])


def build_confidence_merge(regex_df: pd.DataFrame, llm_df: pd.DataFrame) -> pd.DataFrame:
    """Build merged output with regex as primary and LLM as fallback.

    Rules:
    - Keep regex rows as authoritative when voucher exists in regex.
    - Add llm rows only for vouchers absent in regex.
    - If same voucher exists in both with different country, keep regex and flag conflict.
    """
    base_cols = ["pdf", "voucher", "country", "source", "source_confidence", "llm_country", "conflict_flag", "conflict_note"]

    if regex_df.empty and llm_df.empty:
        return pd.DataFrame(columns=base_cols)

    r = regex_df.copy()
    l = llm_df.copy()

    if not r.empty:
        r["voucher_norm"] = r["voucher"].map(normalize_voucher)
    else:
        r = pd.DataFrame(columns=["pdf", "voucher", "country", "voucher_norm"])

    if not l.empty:
        l["voucher_norm"] = l["voucher"].map(normalize_voucher)
    else:
        l = pd.DataFrame(columns=["pdf", "voucher", "country", "voucher_norm"])

    out_rows: List[dict] = []

    # Index LLM by (pdf, voucher_norm) for conflict checks and fallback enrichment.
    llm_index: Dict[tuple, dict] = {}
    if not l.empty:
        for _, row in l.iterrows():
            key = (row.get("pdf", ""), row.get("voucher_norm", ""))
            llm_index[key] = {
                "voucher": row.get("voucher", ""),
                "country": row.get("country", ""),
            }

    # 1) Regex rows are primary.
    if not r.empty:
        for _, row in r.iterrows():
            pdf = row.get("pdf", "")
            voucher = row.get("voucher", "")
            country = row.get("country", "")
            vnorm = row.get("voucher_norm", "")

            llm_match = llm_index.get((pdf, vnorm))
            llm_country = llm_match.get("country") if llm_match else ""
            conflict = bool(llm_country and str(llm_country).strip().lower() != str(country).strip().lower())

            out_rows.append(
                {
                    "pdf": pdf,
                    "voucher": voucher,
                    "country": country,
                    "source": "regex",
                    "source_confidence": "primary",
                    "llm_country": llm_country,
                    "conflict_flag": conflict,
                    "conflict_note": "regex_kept_country_conflict" if conflict else "",
                    "voucher_norm": vnorm,
                }
            )

    # 2) Add only llm rows that are missing in regex.
    regex_keys = set()
    if not r.empty:
        regex_keys = {(row.get("pdf", ""), row.get("voucher_norm", "")) for _, row in r.iterrows()}

    if not l.empty:
        for _, row in l.iterrows():
            key = (row.get("pdf", ""), row.get("voucher_norm", ""))
            if key in regex_keys:
                continue
            out_rows.append(
                {
                    "pdf": row.get("pdf", ""),
                    "voucher": row.get("voucher", ""),
                    "country": row.get("country", ""),
                    "source": "llm",
                    "source_confidence": "fallback",
                    "llm_country": row.get("country", ""),
                    "conflict_flag": False,
                    "conflict_note": "llm_only",
                    "voucher_norm": row.get("voucher_norm", ""),
                }
            )

    merged = pd.DataFrame(out_rows)
    if merged.empty:
        return pd.DataFrame(columns=base_cols)

    merged = merged.sort_values(["pdf", "voucher_norm", "source"]).reset_index(drop=True)
    return merged[base_cols]


def build_confidence_summary(merged_confidence_df: pd.DataFrame) -> pd.DataFrame:
    """Build compact summary metrics for confidence merge output."""
    rows: List[dict] = []

    if merged_confidence_df.empty:
        rows.append({"metric": "total_rows", "value": 0})
        rows.append({"metric": "unique_vouchers", "value": 0})
        rows.append({"metric": "source_regex", "value": 0})
        rows.append({"metric": "source_llm", "value": 0})
        rows.append({"metric": "conflict_true", "value": 0})
        rows.append({"metric": "conflict_false", "value": 0})
        return pd.DataFrame(rows)

    total_rows = int(len(merged_confidence_df))
    unique_vouchers = int(
        merged_confidence_df.assign(voucher_norm=merged_confidence_df["voucher"].map(normalize_voucher))["voucher_norm"].nunique()
    )
    source_counts = merged_confidence_df["source"].value_counts(dropna=False).to_dict()
    conflict_counts = merged_confidence_df["conflict_flag"].astype(bool).value_counts(dropna=False).to_dict()

    rows.append({"metric": "total_rows", "value": total_rows})
    rows.append({"metric": "unique_vouchers", "value": unique_vouchers})
    rows.append({"metric": "source_regex", "value": int(source_counts.get("regex", 0))})
    rows.append({"metric": "source_llm", "value": int(source_counts.get("llm", 0))})
    rows.append({"metric": "conflict_true", "value": int(conflict_counts.get(True, 0))})
    rows.append({"metric": "conflict_false", "value": int(conflict_counts.get(False, 0))})

    # Optional per-pdf detail lines
    per_pdf = (
        merged_confidence_df.groupby("pdf", dropna=False)
        .agg(
            rows=("voucher", "size"),
            regex_rows=("source", lambda s: int((s == "regex").sum())),
            llm_rows=("source", lambda s: int((s == "llm").sum())),
            conflicts=("conflict_flag", lambda s: int(pd.Series(s).astype(bool).sum())),
        )
        .reset_index()
    )

    for _, r in per_pdf.iterrows():
        rows.append({"metric": f"pdf_rows::{r['pdf']}", "value": int(r["rows"])})
        rows.append({"metric": f"pdf_regex_rows::{r['pdf']}", "value": int(r["regex_rows"])})
        rows.append({"metric": f"pdf_llm_rows::{r['pdf']}", "value": int(r["llm_rows"])})
        rows.append({"metric": f"pdf_conflicts::{r['pdf']}", "value": int(r["conflicts"])})

    return pd.DataFrame(rows)


def run_experiment(
    pdf_paths: Sequence[Path],
    out_dir: Path,
    run_llm: bool,
    llm_model: str,
    llm_api_key: Optional[str],
    llm_timeout: int,
    llm_retries: int,
    llm_retry_backoff_s: float,
    llm_max_tokens: int,
    llm_candidate_batch_size: int,
    gold_csv: Optional[Path],
    window_words: int,
    verbose: bool,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    all_regex: List[ExtractedPair] = []
    all_llm: List[ExtractedPair] = []
    section_rows: List[dict] = []

    for pdf_path in pdf_paths:
        if not pdf_path.exists():
            logger.warning("Skipping missing PDF: %s", pdf_path)
            continue

        lines = load_pdf_lines(pdf_path)
        if not lines:
            logger.warning("No text extracted from PDF: %s", pdf_path)
            continue

        sections = find_specimen_sections(lines, pdf_path.name)
        if not sections:
            # Fallback: collapse PDF line breaks to recover broken headings
            # such as "Specimen" + "studied." split across lines.
            continuous_text = _collapse_lines_to_text(lines)
            sections = find_specimen_sections_continuous(continuous_text, pdf_path.name)

        if verbose:
            logger.info("%s -> %d sections", pdf_path.name, len(sections))

        section_rows.extend(
            {
                "pdf": s.pdf,
                "title_line": s.title_line,
                "start_line": s.start_line,
                "end_line": s.end_line,
                "title": s.title,
                "text_preview": s.text[:400],
            }
            for s in sections
        )

        for section in sections:
            regex_pairs = extract_pairs_regex(pdf_path.name, section)
            all_regex.extend(regex_pairs)

            if run_llm:
                candidates = [p.voucher for p in regex_pairs]
                llm_pairs = extract_pairs_llm(
                    pdf_name=pdf_path.name,
                    section=section,
                    model=llm_model,
                    api_key=llm_api_key,
                    candidate_vouchers=candidates,
                    window_words=window_words,
                    max_tokens=llm_max_tokens,
                    timeout=llm_timeout,
                    retries=llm_retries,
                    retry_backoff_s=llm_retry_backoff_s,
                    candidate_batch_size=llm_candidate_batch_size,
                )
                all_llm.extend(llm_pairs)

    regex_df = _to_dataframe(_dedup_pairs(all_regex))
    llm_df = _to_dataframe(_dedup_pairs(all_llm))

    sections_df = pd.DataFrame(section_rows)
    sections_df.to_csv(out_dir / "sections_detected.csv", index=False)
    regex_df.to_csv(out_dir / "regex_pairs.csv", index=False)
    llm_df.to_csv(out_dir / "llm_pairs.csv", index=False)

    combined = pd.concat([regex_df, llm_df], ignore_index=True)
    combined.to_csv(out_dir / "combined_pairs.csv", index=False)

    # Confidence merge: regex primary, llm as fallback for missing vouchers,
    # conflicts flagged but regex country kept.
    merged_confidence_df = build_confidence_merge(regex_df, llm_df)
    merged_confidence_df.to_csv(out_dir / "merged_confidence_pairs.csv", index=False)
    summary_confidence_df = build_confidence_summary(merged_confidence_df)
    summary_confidence_df.to_csv(out_dir / "summary_confidence.csv", index=False)

    if gold_csv and gold_csv.exists():
        gold_df = pd.read_csv(gold_csv)
        eval_rows = []
        for method in ["regex", "llm"]:
            sub = combined[combined["method"] == method]
            if sub.empty:
                continue
            scored = evaluate_against_gold(sub, gold_df)
            scored["method"] = method
            eval_rows.append(scored)

        if eval_rows:
            eval_df = pd.concat(eval_rows, ignore_index=True)
            eval_df.to_csv(out_dir / "evaluation.csv", index=False)

            summary = (
                eval_df.groupby(["method", "status"], dropna=False)
                .size()
                .reset_index(name="count")
                .sort_values(["method", "status"])
            )
            summary.to_csv(out_dir / "evaluation_summary.csv", index=False)

    logger.info("Experiment outputs written to: %s", out_dir)


def _load_pdf_list(args: argparse.Namespace) -> List[Path]:
    paths: List[Path] = [Path(p).expanduser().resolve() for p in args.pdf]

    if args.pdf_list:
        with open(args.pdf_list, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    paths.append(Path(line).expanduser().resolve())

    # Keep order, remove duplicates.
    seen = set()
    unique: List[Path] = []
    for p in paths:
        key = str(p)
        if key not in seen:
            seen.add(key)
            unique.append(p)

    return unique


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark country extraction from specimen prose (regex + OpenRouter)."
    )
    parser.add_argument("--pdf", action="append", default=[], help="PDF path (repeatable)")
    parser.add_argument("--pdf-list", help="Text file with one PDF path per line")
    parser.add_argument("--run-llm", action="store_true", help="Run OpenRouter extraction")
    parser.add_argument(
        "--llm-model",
        default="liquid/lfm-2.5-1.2b-instruct:free",
        help="OpenRouter model name",
    )
    parser.add_argument(
        "--llm-api-key",
        default=LLM_API_KEY,
        help="OpenRouter API key (default: OPENROUTER_API_KEY env)",
    )
    parser.add_argument(
        "--gold-csv",
        help="Optional gold CSV with columns pdf,voucher,country",
    )
    parser.add_argument(
        "--window-words",
        type=int,
        default=35,
        help="LLM context window words before/after each voucher",
    )
    parser.add_argument(
        "--use-extended-country-resolver",
        action="store_true",
        help="Use resolve_country_extended (may call external geocoding and be slower)",
    )
    parser.add_argument(
        "--llm-timeout",
        type=int,
        default=45,
        help="OpenRouter request timeout in seconds",
    )
    parser.add_argument(
        "--llm-retries",
        type=int,
        default=2,
        help="OpenRouter retries on transient failures",
    )
    parser.add_argument(
        "--llm-retry-backoff-s",
        type=float,
        default=1.5,
        help="Backoff base seconds between OpenRouter retries",
    )
    parser.add_argument(
        "--llm-max-tokens",
        type=int,
        default=500,
        help="OpenRouter max_tokens for extraction response",
    )
    parser.add_argument(
        "--llm-candidate-batch-size",
        type=int,
        default=4,
        help="Number of candidate vouchers per LLM call",
    )
    parser.add_argument(
        "--out-dir",
        default="get_taxon_ref_/logs/prose_country_experiment",
        help="Output directory for CSV reports",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    global _USE_EXTENDED_COUNTRY_RESOLVER
    _USE_EXTENDED_COUNTRY_RESOLVER = bool(args.use_extended_country_resolver)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    pdf_paths = _load_pdf_list(args)
    if not pdf_paths:
        parser.error("No PDFs provided. Use --pdf and/or --pdf-list.")

    out_dir = Path(args.out_dir).expanduser().resolve()
    gold_csv = Path(args.gold_csv).expanduser().resolve() if args.gold_csv else None

    run_experiment(
        pdf_paths=pdf_paths,
        out_dir=out_dir,
        run_llm=args.run_llm,
        llm_model=args.llm_model,
        llm_api_key=args.llm_api_key,
        llm_timeout=args.llm_timeout,
        llm_retries=args.llm_retries,
        llm_retry_backoff_s=args.llm_retry_backoff_s,
        llm_max_tokens=args.llm_max_tokens,
        llm_candidate_batch_size=args.llm_candidate_batch_size,
        gold_csv=gold_csv,
        window_words=args.window_words,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()

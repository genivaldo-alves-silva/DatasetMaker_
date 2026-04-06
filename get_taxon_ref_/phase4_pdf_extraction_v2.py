#!/usr/bin/env python3
"""
Phase 4 v2: PDF Table Extraction — PyMuPDF-first approach

Extrai species, voucher e country para um dado código GenBank de um PDF científico.

Workflow (cascata):
  1. [RÁPIDO] PyMuPDF get_text() + parsing inteligente de linhas (~0.1s)
  2. [FALLBACK] pdfplumber extract_text() com layout-aware parsing (~1s)
  3. [CACHE] Se .md existir, reutilizar parse_md_tables() do phase4_pdf_extraction.py

Principais lições do artigo Medium (Mark Kramer, 2025):
  - Nenhuma ferramenta out-of-the-box (incluindo LLMs, Docling, Reducto) extrai 
    tabelas com 100% de precisão
  - pdfplumber é a melhor fundação, mas requer código custom significativo
  - PyMuPDF é extremamente rápido e lida com rotação automaticamente
  - O segredo é combinar ferramentas com lógica custom específica para o domínio

Para tabelas GenBank em artigos de taxonomia, PyMuPDF get_text() é suficiente porque:
  - A estrutura é repetitiva (species, voucher, locality, accessions)
  - Accession codes são facilmente identificáveis por regex
  - O texto extraído preserva a ordem de leitura corretamente
"""

import re
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ============================================================================
# Patterns
# ============================================================================

# GenBank accession formats (NCBI standard):
#   1 letter + 5 digits  (U12345)              — 6 chars
#   2 letters + 6-8 digits (AF123456)           — 8-10 chars
# Codes with 2 letters + 5 digits (e.g. MA67874, MA54629) are typically
# herbarium/specimen codes, NOT GenBank accessions. Codes with 1 letter
# + 6-7 digits (e.g. F1020647, H6059300) are also specimen codes.
ACCESSION_STRICT_RE = re.compile(
    r'^(?:[A-Z]\d{5}|[A-Z]{2}\d{6,8})(?:\.\d+)?$'
)

ACCESSION_RELAXED_RE = re.compile(
    r'^(?:[A-Z]\d{5}|[A-Z]{2}\d{6,8})(?:\.\d+)?$'
)

# Optional trailing footnote marker often attached by PDF superscripts
# e.g. MF977778a, MF972231b, JQ087932*
_ACC_FOOTNOTE_RE = re.compile(r'[*†‡§a-z]+$')


def _normalize_accession_token(token: str, strip_footnote: bool = True) -> str:
    """Normalize accession-like token for robust matching."""
    t = token.strip().strip('()[]{}<>,;:')
    t = t.strip('.,;:')
    if strip_footnote:
        t = _ACC_FOOTNOTE_RE.sub('', t)
    t = re.sub(r'\.\d+$', '', t)
    return t.upper()


def _extract_accession_tokens(line: str, strip_footnote: bool = True) -> list[str]:
    """Extract normalized accession tokens from an arbitrary text line."""
    out = []
    for tok in line.split():
        norm = _normalize_accession_token(tok, strip_footnote=strip_footnote)
        if ACCESSION_STRICT_RE.match(norm) or ACCESSION_RELAXED_RE.match(norm):
            out.append(norm)
    return out

# Dash/missing data indicators
DASH_RE = re.compile(r'^[–\-—]+$')

# Species pattern: "Genus species" ou "G. species" ou "G. cf. species"
# Key: requires explicit separator (period or space) between genus and epithet
# to avoid matching single words like "Mexico", "Argentina"
SPECIES_RE = re.compile(
    r'^(?:'
    r'[A-Z][a-z]{2,}\.?\s+(?:(?:cf|aff)\.\s+)?[a-z]'  # Full genus: Fomitiporia aethiopica
    r'|[A-Z][a-z]{2,}\.?\s+sp\.?(?:\s|$)'               # Full genus sp.: Pluteus sp., Pluteus sp. I
    r'|[A-Z]\.\s*(?:(?:cf|aff)\.\s+)?[a-z]{3,}'         # Abbrev+period: F. aethiopica (period mandatory)
    r'|[A-Z]\.\s*sp\.?(?:\s|$)'                          # Abbrev sp.: P. sp., P. sp. I
    r'|[A-Z]\s+(?:(?:cf|aff)\.\s+)?[a-z]{3,}'            # Abbrev+space: F aethiopica (space mandatory)
    r')'
)

# Genus-only pattern (ex: "Aurificaria", "Coltricia", "Fomitiporella")
GENUS_ONLY_RE = re.compile(r'^[A-Z][a-z]{3,}$')

# Voucher/specimen patterns
VOUCHER_RE = re.compile(
    r'^(?:'
    r'[A-Z]{2,7}[-:\s]\s*[A-Za-z]*\d+'  # MUCL 44806, CBS 428.86, LAH:SH148, CBS: 233.56
    r'|[A-Z][a-z]+\.?\s+\d+'             # He 592, Dai 10756, Cui 10321
    r'|FP-\d+'                     # FP-134784
    r'|[A-Z]{1,3}\s+\d+'          # LF 39116
    r'|[A-Z]+\s*,\s*[A-Z]'        # CORD, Robledo 219
    r'|[A-Z]{2,}\s+[A-Z]'         # LWZ 20150802-9
    r'|[A-Z]\.[A-Z]\.\s+[A-Z]'    # E.C. Vellinga 3131 (UC)
    r'|[A-Z]\.\s*[A-Z]\s+[A-Z]'   # E. Vellinga (UC)
    r'|#\s*\d+'                    # # 115977 (Mushroom Observer)
    r'|[A-Z]\d{1,}'               # A18, L1 (single letter + digits)
    r'|[A-Z]{3,}\d{2,}'           # ANGE305 (3+ uppercase + digits, avoid GenBank 2-letter prefixes)
    r'|[A-Z][a-z]+-[A-Z]+\d+'     # Asif-SP14AS11 (Name-PREFIXdigits)
    r'|[A-Z]\.\s*&\s*[A-Z]'       # J. & A. Guinberteau (initials with &)
    r'|Mushroom\s+Observer'        # Mushroom Observer (prefix for MO vouchers)
    r'|[A-Z]{2,}[a-z]{2,}\s+[A-Z]?\d+'  # CLZhao F11059, CLZhao 4069 (CamelCase collector codes)
    r'|[A-Z]\.\s+[A-Z][a-z]+\s+\d+'  # M. Korhonen 10305, P. Joijer 4118 (Initial. Surname Number)
    r'|\d+[A-Z]{2,}'              # 83SAMHYP, 14AS11 (digit-prefixed specimen codes)
    r'|IO\.\d+\.\d+'              # IO.14.164 (dot-separated specimen codes)
    r')'
)

# Reference pattern: "Author et al. (year)" or "Author and Author (year)"
# Also matches without parentheses: "Author et al. 2021", "Author 2004"
REFERENCE_RE = re.compile(
    r'(?:'
    r'(?:et\s+al|[A-Z][a-z]+\s+and\s+[A-Z]).*\(?(?:19|20)\d{2}\)?'  # et al. (2021) or et al. 2021
    r'|[A-Z][a-z]+\s+\(?(?:19|20)\d{2}[a-z]?\)?'               # Larsson (2011), Vellinga 2004, Du (2020b)
    r'|[A-Z][a-z]+\s+et\s+al\.?'                               # Vellinga et al. (no year yet)
    r'|al\.\s*\(?(?:19|20)\d{2}'                                 # al. (2020) — split reference continuation
    r'|GenBank\s+direct'                                        # GenBank direct submission
    r'|direct\s+submission'                                     # direct submission
    r'|[Tt]his\s+study'                                         # This study (common reference in tables)
    r'|[Uu]npub'                                                # Unpublished / (unpub.)
    r'|[Ss]aragiotto\s+\(unpub'                                 # Saragiotto (unpub.) — specific author pattern
    r')'
)

# Gene/accession column header keywords
GENE_KEYWORDS = {
    'its', 'its1', 'its2', 'its1-5.8s-its2',
    'lsu', 'nlsu', 'nrlsu', '28s', 'nuc 28s',
    'ssu', 'nrssu', '18s',
    'tef1', 'tef', 'tef1-α', 'tef-1', 'ef-1α', 'ef1', 'ef1-α',
    'rpb1', 'rpb2', 'rpb-1', 'rpb-2', 'rbp1', 'rbp2',  # rbp1/rbp2 = common typos
    'β-tubulin', 'beta-tubulin', 'btub', 'tub2',
    'accession', 'accession #', 'accession no',
}

# Super-header patterns to skip (merged headers spanning multiple columns)
SUPER_HEADER_RE = re.compile(
    r'genbank\s+accession|accession\s+number|accession\s+no\w*$',
    re.IGNORECASE
)

# Table title pattern (supports Arabic and Roman numerals)
TABLE_TITLE_RE = re.compile(
    r'table\s+(?:\d+|[IVXivx]+)[\.\s]',
    re.IGNORECASE
)

# "Table continued" marker
TABLE_CONTINUED_RE = re.compile(
    r'table\s+(?:\d+|[IVXivx]+)\s*\(continued\)',
    re.IGNORECASE
)

# Page header/footer patterns (journal name, page numbers, etc.)
PAGE_NOISE_RE = re.compile(
    r'^(?:'
    r'\d{1,3}$'                       # Page numbers (1-999; 4+ digit numbers may be voucher codes)
    r'|Mycol\w*\s+Progress'          # Journal headers
    r'|MYCOLOGIA'
    r'|MycoKeys'
    r'|DOI\s+\d'
    r'|[A-Z][A-Z\s]+:(?!.*\d)'             # AUTHOR NAME et al.: (but not TASM:YG G39, LAH:SH148, JBSD:130924 voucher codes)
    r'|\d+:\s*\d+–\d+'              # 114: 133–175
    r'|T, IT, PT\s*='               # Footnotes about types
    r'|shown in bold'               # Table footnotes
    r'|[a-z]\s*='                   # Footnote definitions
    r')',
    re.IGNORECASE
)

# Countries list for matching
COUNTRIES = {
    'Afghanistan', 'Albania', 'Algeria', 'Argentina', 'Armenia', 'Australia',
    'Austria', 'Azerbaijan', 'Bangladesh', 'Belarus', 'Belgium', 'Benin', 'Bolivia',
    'Bosnia and Herzegovina', 'Botswana', 'Brazil', 'Brunei', 'Bulgaria',
    'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon', 'Canada', 'Chad',
    'Chile', 'China', 'Colombia', 'Congo', 'Costa Rica', 'Croatia', 'Cuba',
    'Czech Republic', 'Czech', 'Denmark', 'Dominican Republic', 'Ecuador', 'Egypt',
    'El Salvador', 'England', 'Equatorial Guinea', 'Estonia', 'Ethiopia',
    'Fiji', 'Finland', 'France', 'French Guiana', 'Gabon', 'Georgia',
    'Germany', 'Ghana', 'Greece', 'Guadeloupe', 'Guatemala', 'Guinea',
    'Guyana', 'Haiti', 'Honduras', 'Hungary', 'Iceland', 'India',
    'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy', 'Jamaica',
    'Japan', 'Jordan', 'Kazakhstan', 'Kenya', 'Korea', 'Kuwait',
    'Kyrgyzstan', 'Laos', 'Latvia', 'Lebanon', 'Liberia', 'Libya',
    'Lithuania', 'Luxembourg', 'Madagascar', 'Malawi', 'Malaysia', 'Mali',
    'Martinique', 'Mauritania', 'Mauritius', 'Mexico', 'Moldova', 'Mongolia',
    'Montenegro', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nepal',
    'Netherlands', 'New Caledonia', 'New Zealand', 'Nicaragua', 'Niger',
    'Nigeria', 'North Macedonia', 'Norway', 'Oman', 'Pakistan', 'Palestine',
    'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Poland',
    'Portugal', 'Puerto Rico', 'Qatar', 'Reunion', 'Romania', 'Russia',
    'Rwanda', 'Saudi Arabia', 'Scotland', 'Senegal', 'Serbia', 'Sierra Leone',
    'Singapore', 'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia',
    'South Africa', 'South Korea', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname',
    'Sweden', 'Switzerland', 'Syria', 'Taiwan', 'Tajikistan', 'Tanzania',
    'Thailand', 'Togo', 'Trinidad', 'Trinidad and Tobago', 'Tunisia',
    'Turkey', 'Turkmenistan', 'Uganda', 'UK', 'Ukraine', 
    'United Arab Emirates', 'United Kingdom', 'United States', 'Uruguay',
    'USA', 'Uzbekistan', 'Vanuatu', 'Venezuela', 'Vietnam', 'Wales',
    'Yemen', 'Zambia', 'Zimbabwe',
}
COUNTRIES_LOWER = {c.lower() for c in COUNTRIES}
_COUNTRIES_CANONICAL = {c.lower(): c for c in COUNTRIES}

# Country name aliases: native/historical names → canonical English name
# Includes diacritical variants, endonyms, and common alternative spellings
_COUNTRY_ALIASES = {
    'türkiye': 'Turkey',
    'turkiye': 'Turkey',
    'turkei': 'Turkey',
    'chine': 'China',
    'guadelupe': 'Guadeloupe',
    'guadalupe': 'Guadeloupe',
    'guadeloupe': 'Guadeloupe',
    'república dominicana': 'Dominican Republic',
    'republica dominicana': 'Dominican Republic',
    'côte d\'ivoire': 'Ivory Coast',
    'cote d\'ivoire': 'Ivory Coast',
    'ivory coast': 'Ivory Coast',
    'deutschland': 'Germany',
    'brasil': 'Brazil',
    'españa': 'Spain',
    'espana': 'Spain',
    'nippon': 'Japan',
    'zhongguo': 'China',
    'rossiya': 'Russia',
    'ísland': 'Iceland',
    'island': 'Iceland',
    'hanguk': 'South Korea',
    'polska': 'Poland',
    'österreich': 'Austria',
    'osterreich': 'Austria',
    'schweiz': 'Switzerland',
    'suisse': 'Switzerland',
    'svizzera': 'Switzerland',
    'nederland': 'Netherlands',
    'belgique': 'Belgium',
    'belgië': 'Belgium',
    'belgie': 'Belgium',
    'sverige': 'Sweden',
    'norge': 'Norway',
    'suomi': 'Finland',
    'danmark': 'Denmark',
    'magyarország': 'Hungary',
    'magyarorszag': 'Hungary',
    'česko': 'Czech Republic',
    'cesko': 'Czech Republic',
    'czech. rep.': 'Czech Republic',
    'czechia': 'Czech Republic',
    'slovensko': 'Slovakia',
    'hrvatska': 'Croatia',
    'srbija': 'Serbia',
    'crna gora': 'Montenegro',
    'shqipëria': 'Albania',
    'shqiperia': 'Albania',
    'sakartvelo': 'Georgia',
    'zhōnghuá': 'China',
    'bharat': 'India',
    'misr': 'Egypt',
    'al-maghrib': 'Morocco',
    'muang thai': 'Thailand',
    'prathet thai': 'Thailand',
    'việt nam': 'Vietnam',
    'viet nam': 'Vietnam',
    # French abbreviations for African countries
    'rdc': 'Congo',
    'drc': 'Congo',
    'r.d.c.': 'Congo',
    'république démocratique du congo': 'Congo',
    'republique democratique du congo': 'Congo',
    # Common abbreviations with "Rep." / "Republic"
    'democratic rep. of congo': 'Congo',
    'democratic republic of congo': 'Congo',
    'democratic republic of the congo': 'Congo',
    'dem. rep. congo': 'Congo',
    'dem. rep. of congo': 'Congo',
    'rep. of congo': 'Congo',
    'rep. of the congo': 'Congo',
    'dr congo': 'Congo',
    'rep. of korea': 'South Korea',
    'republic of korea': 'South Korea',
    'rep. of china': 'Taiwan',
    'republic of china': 'Taiwan',
    'czech rep.': 'Czech Republic',
    'czech rep': 'Czech Republic',
    'p.r. china': 'China',
    'p.r.china': 'China',
    "people's republic of china": 'China',
    'peoples republic of china': 'China',
    'great britain': 'United Kingdom',
    'u.s.a.': 'USA',
    'u.s.a': 'USA',
    'russian federation': 'Russia',
    'ivory coast': 'Ivory Coast',
    'côte d\'ivoire': 'Ivory Coast',
    "cote d'ivoire": 'Ivory Coast',
    'reunion': 'Reunion',
    'réunion': 'Reunion',
}

# ISO 3166-1 alpha-2 codes → full country name (case-sensitive: UPPERCASE only)
ISO2_TO_COUNTRY = {
    'AF': 'Afghanistan', 'AL': 'Albania', 'DZ': 'Algeria', 'AR': 'Argentina',
    'AM': 'Armenia', 'AU': 'Australia', 'AT': 'Austria', 'AZ': 'Azerbaijan',
    'BD': 'Bangladesh', 'BE': 'Belgium', 'BJ': 'Benin', 'BO': 'Bolivia',
    'BA': 'Bosnia and Herzegovina', 'BW': 'Botswana', 'BR': 'Brazil',
    'BN': 'Brunei', 'BZ':'Belize', 'BG': 'Bulgaria', 'BF': 'Burkina Faso', 'BI': 'Burundi',
    'KH': 'Cambodia', 'CM': 'Cameroon', 'CA': 'Canada', 'TD': 'Chad',
    'CL': 'Chile', 'CN': 'China', 'CO': 'Colombia', 'CG': 'Congo',
    'CD': 'Congo', 'CR': 'Costa Rica', 'HR': 'Croatia', 'CU': 'Cuba',
    'CZ': 'Czech Republic', 'CZ': 'Czech', 'DK': 'Denmark', 'DO': 'Dominican Republic',
    'EC': 'Ecuador', 'EG': 'Egypt', 'SV': 'El Salvador',
    'GQ': 'Equatorial Guinea', 'EE': 'Estonia', 'ET': 'Ethiopia',
    'FJ': 'Fiji', 'FI': 'Finland', 'FR': 'France', 'GF': 'French Guiana',
    'GA': 'Gabon', 'GE': 'Georgia', 'DE': 'Germany', 'GH': 'Ghana',
    'GR': 'Greece', 'GP': 'Guadeloupe', 'GT': 'Guatemala', 'GN': 'Guinea',
    'GY': 'Guyana', 'HT': 'Haiti', 'HN': 'Honduras', 'HU': 'Hungary',
    'IS': 'Iceland', 'IN': 'India', 'ID': 'Indonesia', 'IR': 'Iran',
    'IQ': 'Iraq', 'IE': 'Ireland', 'IL': 'Israel', 'IT': 'Italy',
    'JM': 'Jamaica', 'JP': 'Japan', 'JO': 'Jordan', 'KZ': 'Kazakhstan',
    'KE': 'Kenya', 'KR': 'South Korea', 'KW': 'Kuwait', 'KG': 'Kyrgyzstan',
    'LA': 'Laos', 'LV': 'Latvia', 'LB': 'Lebanon', 'LR': 'Liberia',
    'LY': 'Libya', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'MG': 'Madagascar',
    'MW': 'Malawi', 'MY': 'Malaysia', 'ML': 'Mali', 'MQ': 'Martinique',
    'MR': 'Mauritania', 'MU': 'Mauritius', 'MX': 'Mexico', 'MD': 'Moldova',
    'MN': 'Mongolia', 'ME': 'Montenegro', 'MA': 'Morocco', 'MZ': 'Mozambique',
    'MM': 'Myanmar', 'NA': 'Namibia', 'NP': 'Nepal', 'NL': 'Netherlands',
    'NC': 'New Caledonia', 'NZ': 'New Zealand', 'NI': 'Nicaragua',
    'NE': 'Niger', 'NG': 'Nigeria', 'MK': 'North Macedonia', 'NO': 'Norway',
    'OM': 'Oman', 'PK': 'Pakistan', 'PS': 'Palestine', 'PA': 'Panama',
    'PG': 'Papua New Guinea', 'PY': 'Paraguay', 'PE': 'Peru',
    'PH': 'Philippines', 'PL': 'Poland', 'PT': 'Portugal', 'PR': 'Puerto Rico',
    'QA': 'Qatar', 'RE': 'Reunion', 'RO': 'Romania', 'RU': 'Russia',
    'RW': 'Rwanda', 'SA': 'Saudi Arabia', 'SN': 'Senegal', 'RS': 'Serbia',
    'SL': 'Sierra Leone', 'SG': 'Singapore', 'SK': 'Slovakia',
    'SI': 'Slovenia', 'SB': 'Solomon Islands', 'SO': 'Somalia',
    'ZA': 'South Africa', 'ES': 'Spain', 'LK': 'Sri Lanka', 'SD': 'Sudan',
    'SR': 'Suriname', 'SE': 'Sweden', 'CH': 'Switzerland', 'SY': 'Syria',
    'TW': 'Taiwan', 'TJ': 'Tajikistan', 'TZ': 'Tanzania', 'TH': 'Thailand',
    'TG': 'Togo', 'TT': 'Trinidad and Tobago', 'TN': 'Tunisia',
    'TR': 'Turkey', 'TM': 'Turkmenistan', 'UG': 'Uganda', 'UA': 'Ukraine',
    'AE': 'United Arab Emirates', 'GB': 'United Kingdom',
    'US': 'United States', 'UY': 'Uruguay', 'UZ': 'Uzbekistan',
    'VU': 'Vanuatu', 'VE': 'Venezuela', 'VN': 'Vietnam', 'YE': 'Yemen',
    'ZM': 'Zambia', 'ZW': 'Zimbabwe',
}

# ISO 3166-1 alpha-3 codes → full country name (case-sensitive: UPPERCASE only)
ISO3_TO_COUNTRY = {
    'AFG': 'Afghanistan', 'ALB': 'Albania', 'DZA': 'Algeria', 'ARG': 'Argentina',
    'ARM': 'Armenia', 'AUS': 'Australia', 'AUT': 'Austria', 'AZE': 'Azerbaijan',
    'BGD': 'Bangladesh', 'BLZ': 'Belize', 'BEL': 'Belgium', 'BEN': 'Benin', 'BOL': 'Bolivia',
    'BIH': 'Bosnia and Herzegovina', 'BWA': 'Botswana', 'BRA': 'Brazil',
    'BRN': 'Brunei', 'BGR': 'Bulgaria', 'BFA': 'Burkina Faso', 'BDI': 'Burundi',
    'KHM': 'Cambodia', 'CMR': 'Cameroon', 'CAN': 'Canada', 'TCD': 'Chad',
    'CHL': 'Chile', 'CHN': 'China', 'COL': 'Colombia', 'COG': 'Congo',
    'COD': 'Congo', 'CRI': 'Costa Rica', 'HRV': 'Croatia', 'CUB': 'Cuba',
    'CZE': 'Czech Republic', 'CZE': 'Czech','DNK': 'Denmark', 'DOM': 'Dominican Republic',
    'ECU': 'Ecuador', 'EGY': 'Egypt', 'SLV': 'El Salvador',
    'GNQ': 'Equatorial Guinea', 'EST': 'Estonia', 'ETH': 'Ethiopia',
    'FJI': 'Fiji', 'FIN': 'Finland', 'FRA': 'France', 'GUF': 'French Guiana',
    'GAB': 'Gabon', 'GEO': 'Georgia', 'DEU': 'Germany', 'GHA': 'Ghana',
    'GRC': 'Greece', 'GLP': 'Guadeloupe', 'GTM': 'Guatemala', 'GIN': 'Guinea',
    'GUY': 'Guyana', 'HTI': 'Haiti', 'HND': 'Honduras', 'HUN': 'Hungary',
    'ISL': 'Iceland', 'IND': 'India', 'IDN': 'Indonesia', 'IRN': 'Iran',
    'IRQ': 'Iraq', 'IRL': 'Ireland', 'ISR': 'Israel', 'ITA': 'Italy',
    'JAM': 'Jamaica', 'JPN': 'Japan', 'JOR': 'Jordan', 'KAZ': 'Kazakhstan',
    'KEN': 'Kenya', 'KOR': 'South Korea', 'KWT': 'Kuwait', 'KGZ': 'Kyrgyzstan',
    'LAO': 'Laos', 'LVA': 'Latvia', 'LBN': 'Lebanon', 'LBR': 'Liberia',
    'LBY': 'Libya', 'LTU': 'Lithuania', 'LUX': 'Luxembourg', 'MDG': 'Madagascar',
    'MWI': 'Malawi', 'MYS': 'Malaysia', 'MLI': 'Mali', 'MTQ': 'Martinique',
    'MRT': 'Mauritania', 'MUS': 'Mauritius', 'MEX': 'Mexico', 'MDA': 'Moldova',
    'MNG': 'Mongolia', 'MNE': 'Montenegro', 'MAR': 'Morocco', 'MOZ': 'Mozambique',
    'MMR': 'Myanmar', 'NAM': 'Namibia', 'NPL': 'Nepal', 'NLD': 'Netherlands',
    'NCL': 'New Caledonia', 'NZL': 'New Zealand', 'NIC': 'Nicaragua',
    'NER': 'Niger', 'NGA': 'Nigeria', 'MKD': 'North Macedonia', 'NOR': 'Norway',
    'OMN': 'Oman', 'PAK': 'Pakistan', 'PSE': 'Palestine', 'PAN': 'Panama',
    'PNG': 'Papua New Guinea', 'PRY': 'Paraguay', 'PER': 'Peru',
    'PHL': 'Philippines', 'POL': 'Poland', 'PRT': 'Portugal', 'PRI': 'Puerto Rico',
    'QAT': 'Qatar', 'REU': 'Reunion', 'ROU': 'Romania', 'RUS': 'Russia',
    'RWA': 'Rwanda', 'SAU': 'Saudi Arabia', 'SEN': 'Senegal', 'SRB': 'Serbia',
    'SLE': 'Sierra Leone', 'SGP': 'Singapore', 'SVK': 'Slovakia',
    'SVN': 'Slovenia', 'SLB': 'Solomon Islands', 'SOM': 'Somalia',
    'ZAF': 'South Africa', 'ESP': 'Spain', 'LKA': 'Sri Lanka', 'SDN': 'Sudan',
    'SUR': 'Suriname', 'SWE': 'Sweden', 'CHE': 'Switzerland', 'SYR': 'Syria',
    'TWN': 'Taiwan', 'TJK': 'Tajikistan', 'TZA': 'Tanzania', 'THA': 'Thailand',
    'TGO': 'Togo', 'TTO': 'Trinidad and Tobago', 'TUN': 'Tunisia',
    'TUR': 'Turkey', 'TKM': 'Turkmenistan', 'UGA': 'Uganda', 'UKR': 'Ukraine',
    'ARE': 'United Arab Emirates', 'GBR': 'United Kingdom',
    'USA': 'United States', 'URY': 'Uruguay', 'UZB': 'Uzbekistan',
    'VUT': 'Vanuatu', 'VEN': 'Venezuela', 'VNM': 'Vietnam', 'YEM': 'Yemen',
    'ZMB': 'Zambia', 'ZWE': 'Zimbabwe',
}

# Words that are parts of multi-word country names — used for multi-line merging
_COUNTRY_WORD_PARTS = {
    'dominican', 'republic', 'united', 'states', 'kingdom', 'arab', 'emirates',
    'new', 'zealand', 'caledonia', 'guinea', 'equatorial', 'sierra', 'leone',
    'sri', 'lanka', 'south', 'africa', 'korea', 'north', 'macedonia',
    'papua', 'trinidad', 'tobago', 'burkina', 'faso', 'solomon', 'islands',
    'costa', 'rica', 'el', 'salvador', 'saudi', 'arabia', 'bosnia',
    'herzegovina', 'french', 'guiana',
}


# ============================================================================
# Result dataclass
# ============================================================================

@dataclass
class AccessionLookupResult:
    """Resultado de busca de um accession code em um PDF."""
    accession: str = ""
    species: str = ""
    voucher: str = ""
    country: str = ""
    gene_region: str = ""        # ITS, nLSU, etc.
    other_accessions: dict = field(default_factory=dict)  # gene_region -> accession
    raw_row_lines: list = field(default_factory=list)
    confidence: str = "high"     # high, medium, low
    method: str = ""             # pymupdf, pdfplumber, markdown


# ============================================================================
# Helper functions
# ============================================================================

def is_accession_like(s: str) -> bool:
    """Check if a string looks like a GenBank accession code."""
    s = _normalize_accession_token(s, strip_footnote=True)
    return bool(ACCESSION_STRICT_RE.match(s) or ACCESSION_RELAXED_RE.match(s))


def is_accession_or_dash(s: str) -> bool:
    """Check if a line is an accession code or a dash (missing data)."""
    s = s.strip()
    if bool(DASH_RE.match(s)):
        return True
    if is_accession_like(s):
        return True
    tokens = _extract_accession_tokens(s, strip_footnote=True)
    return len(tokens) == 1


def _is_citation_marker(s: str) -> bool:
    """Return True for citation markers like [40], [2, 5], [1-3]."""
    return bool(re.fullmatch(r'\[\s*\d+(?:\s*[-,]\s*\d+)*\s*\]', s.strip()))


def is_species_name(s: str) -> bool:
    """Check if a line looks like a species name."""
    # Normalize quoted "sp." variants (smart/straight quotes from PDFs)
    # e.g. Fomitiporia "sp." PS1 → Fomitiporia sp. PS1
    normalized = re.sub(r'["\'\u2018\u2019\u201b\u201c\u201d\u201e\u201f\u2032\u2033`]+', '', s.strip())
    return bool(SPECIES_RE.match(normalized))


def is_genus_only(s: str) -> bool:
    """Check if a line is just a genus name (e.g. 'Aurificaria')."""
    return bool(GENUS_ONLY_RE.match(s.strip()))


def resolve_country(s: str) -> Optional[str]:
    """Resolve a string to a canonical country name, or None if not a country.
    
    Handles:
    - Full country names (case-insensitive): 'Brazil', 'china' → 'Brazil', 'China'
    - Country aliases/endonyms: 'Türkiye' → 'Turkey', 'Deutschland' → 'Germany'
    - ISO 3166-1 alpha-2 codes (uppercase only): 'BR' → 'Brazil', 'CN' → 'China'
    - ISO 3166-1 alpha-3 codes (uppercase only): 'BRA' → 'Brazil', 'CHN' → 'China'
    - Country with state/region in parentheses: 'USA (MI)', 'Portugal (Madeira)'
    - Common abbreviations: 'Czech Rep.', 'Democratic Rep. of Congo'
    """
    stripped = s.strip()
    lower = stripped.lower()
    # 1. Full country name (case-insensitive)
    if lower in COUNTRIES_LOWER:
        return _COUNTRIES_CANONICAL.get(lower, stripped)
    # 2. Country aliases/endonyms (Türkiye → Turkey, etc.)
    if lower in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[lower]
    # 3. Normalized alias (strip diacritics then check)
    import unicodedata
    normalized = unicodedata.normalize('NFKD', lower).encode('ascii', 'ignore').decode('ascii')
    if normalized != lower and normalized in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[normalized]
    # 4. ISO 3166-1 alpha-2 (exactly 2 uppercase letters)
    if len(stripped) == 2 and stripped.isalpha() and stripped.isupper():
        return ISO2_TO_COUNTRY.get(stripped)
    # 5. ISO 3166-1 alpha-3 (exactly 3 uppercase letters)
    if len(stripped) == 3 and stripped.isalpha() and stripped.isupper():
        return ISO3_TO_COUNTRY.get(stripped)
    # 6. Country with parenthetical state/region: 'USA (MI)' → match 'USA'
    #    Also handles 'Portugal (Madeira)', 'China (Hainan)', etc.
    paren_match = re.match(r'^([A-Za-z .]+?)\s*\(', stripped)
    if paren_match:
        base = paren_match.group(1).strip()
        base_lower = base.lower()
        if base_lower in COUNTRIES_LOWER:
            return _COUNTRIES_CANONICAL.get(base_lower, base)
        if base_lower in _COUNTRY_ALIASES:
            return _COUNTRY_ALIASES[base_lower]
        # Also check ISO codes: 'US (MI)', though rare
        if len(base) == 2 and base.isalpha() and base.isupper() and base in ISO2_TO_COUNTRY:
            return ISO2_TO_COUNTRY[base]
    return None


def resolve_country_extended(s: str) -> Optional[str]:
    """Resolve country with detectar_pais fallback for difficult cases.
    
    Tries resolve_country() first (fast, no false positives). If that fails,
    uses TaxonQualifier.country_detector.detectar_pais() as fallback, but ONLY
    when the input doesn't look like a voucher, accession, or species name
    (to avoid false positives like 'AJ78 (LOU)' → 'United States').
    
    The detectar_pais result is normalized back to our canonical country names.
    """
    # Fast path: our strict resolver
    result = resolve_country(s)
    if result:
        return result
    
    stripped = s.strip()
    if not stripped or len(stripped) < 2:
        return None
    
    # Guard: skip if it looks like accession, voucher, species, or pure numbers
    if is_accession_like(stripped):
        return None
    if VOUCHER_RE.match(stripped):
        return None
    if SPECIES_RE.match(stripped):
        return None
    if stripped.isdigit():
        return None
    # Guard: skip if it has digits mixed with letters typical of voucher codes
    # (e.g., 'AJ78 (LOU)', 'TNSF12351', 'CBS 609.82')
    if re.search(r'[A-Z]{2,}\s*\d', stripped) and not re.search(r'rep\.|republic', stripped, re.I):
        return None
    
    # Try detectar_pais
    try:
        from TaxonQualifier.country_detector import detectar_pais
        raw = detectar_pais(stripped)
        if not raw or isinstance(raw, list):
            return None
        # Normalize the result back through our resolver
        # (detectar_pais may return non-English names like 'Česko', '臺灣')
        normalized = resolve_country(raw)
        if normalized:
            return normalized
        # Check if raw itself is already a canonical English name in our set
        if raw.strip().lower() in COUNTRIES_LOWER:
            return _COUNTRIES_CANONICAL.get(raw.strip().lower(), raw.strip())
        return None
    except ImportError:
        return None
    except Exception:
        return None


def is_country(s: str) -> bool:
    """Check if a string matches a known country name or ISO code."""
    return resolve_country(s) is not None


def _split_voucher_country(voucher: str) -> tuple[str, str]:
    """
    Split trailing country name from a voucher string.
    
    E.g., 'MFLU:19-1331, holotype Thailand' → ('MFLU:19-1331, holotype', 'Thailand')
          'HMAS:290752, holotype China'      → ('HMAS:290752, holotype', 'China')
          'MCVE:736'                          → ('MCVE:736', '')
    
    Returns (cleaned_voucher, country_or_empty).
    """
    # Try last 1-2 words as country
    parts = voucher.rsplit(None, 2)
    if len(parts) >= 2:
        # Try last word
        last = parts[-1].strip(' ,;')
        resolved = resolve_country(last)
        if resolved:
            # Remove trailing country from voucher
            idx = voucher.rfind(last)
            cleaned = voucher[:idx].rstrip(' ,;')
            return cleaned, resolved
        # Try last two words
        if len(parts) >= 3:
            last2 = parts[-2] + ' ' + parts[-1]
            resolved = resolve_country(last2.strip(' ,;'))
            if resolved:
                idx = voucher.rfind(parts[-2])
                cleaned = voucher[:idx].rstrip(' ,;')
                return cleaned, resolved
    return voucher, ''


def is_voucher(s: str) -> bool:
    """Check if a line looks like a voucher/specimen code.
    
    Returns False for reference-like strings (Author YYYY) even if they match
    the voucher pattern, because references take priority.
    """
    stripped = s.strip()
    if not VOUCHER_RE.match(stripped):
        return False
    # Reject if it looks like a reference (Author YYYY, Author et al. YYYY)
    if is_reference(stripped):
        return False
    return True


def is_reference(s: str) -> bool:
    """Check if a line looks like a literature reference."""
    return bool(REFERENCE_RE.search(s.strip()))


def is_page_noise(s: str) -> bool:
    """Check if a line is page header/footer noise."""
    return bool(PAGE_NOISE_RE.match(s.strip()))


def is_gene_header(s: str) -> bool:
    """Check if a string is a gene column header keyword."""
    return s.strip().lower() in GENE_KEYWORDS


def is_super_header(s: str) -> bool:
    """Check if a line is a super-header (spanning multiple columns)."""
    return bool(SUPER_HEADER_RE.match(s.strip()))


# ============================================================================
# PDF Text Extraction
# ============================================================================

def extract_text_lines(pdf_path: str | Path) -> list[str]:
    """
    Extract all text lines from PDF using PyMuPDF.
    
    PyMuPDF advantages:
    - Handles rotated pages automatically
    - Very fast (<0.1s)
    - Preserves reading order
    - Each table cell value appears on its own line
    """
    import pymupdf
    
    # Common PDF ligatures to normalize
    _LIGATURES = str.maketrans({
        '\ufb00': 'ff', '\ufb01': 'fi', '\ufb02': 'fl',
        '\ufb03': 'ffi', '\ufb04': 'ffl',
    })
    doc = pymupdf.open(str(pdf_path))
    all_lines = []
    for page in doc:
        text = page.get_text().translate(_LIGATURES)
        for line in text.split('\n'):
            s = line.strip()
            if s:
                all_lines.append(s)
    doc.close()
    return all_lines


# ============================================================================
# Table Header Detection
# ============================================================================

@dataclass
class TableHeader:
    """Parsed table header information."""
    title_line: int = -1          # Line index of "Table N. ..."
    header_start: int = -1        # First header line index
    header_end: int = -1          # Last header line index (exclusive = first data line)
    col_names: list = field(default_factory=list)   # Actual column names in order
    gene_cols: list = field(default_factory=list)    # Gene column names (ITS, nLSU, etc.)
    meta_cols: list = field(default_factory=list)    # Metadata column names (species, voucher, etc.)
    n_gene_cols: int = 0          # Number of gene/accession columns
    n_meta_cols: int = 0          # Number of metadata columns
    n_total_cols: int = 0         # Total columns
    has_country_after_acc: bool = False  # Country column appears after accessions
    has_reference_col: bool = False      # References column present
    pre_gene_meta_cols: list = field(default_factory=list)   # Meta columns BEFORE gene columns
    post_gene_meta_cols: list = field(default_factory=list)  # Meta columns AFTER gene columns


def find_table_headers(lines: list[str]) -> list[TableHeader]:
    """
    Find all GenBank table headers in the text lines.
    
    A GenBank table header contains:
    - A table title mentioning species/specimens/sequences/accession
    - Column headers including gene names (ITS, nLSU, etc.)
    
    Returns list of TableHeader objects.
    """
    headers = []
    i = 0
    
    while i < len(lines):
        # Look for table title
        if TABLE_TITLE_RE.search(lines[i]):
            title_line = i
            title_text = lines[i].lower()
            
            # Check if this is a GenBank-related table
            gb_keywords = ['Genera / Species name', 'genera', 'species name', 'species', 'specimen', 'sequence', 'accession', 
                          'genbank', 'collection', 'voucher', 'phylogen']
            is_gb_table = any(kw in title_text for kw in gb_keywords)
            
            if is_gb_table:
                header = _parse_header_region(lines, title_line)
                if header and header.n_gene_cols > 0:
                    headers.append(header)
        
        # Also detect "Table (continued)" headers
        if TABLE_CONTINUED_RE.search(lines[i]):
            # This is a continuation - reuse the previous header structure
            # but update the position
            if headers:
                cont_header = _parse_header_region(lines, i)
                if cont_header:
                    # Inherit gene/meta structure from previous header
                    cont_header.gene_cols = headers[0].gene_cols
                    cont_header.meta_cols = headers[0].meta_cols
                    cont_header.n_gene_cols = headers[0].n_gene_cols
                    cont_header.n_meta_cols = headers[0].n_meta_cols
                    cont_header.n_total_cols = headers[0].n_total_cols
                    cont_header.has_country_after_acc = headers[0].has_country_after_acc
                    cont_header.has_reference_col = headers[0].has_reference_col
                    cont_header.pre_gene_meta_cols = headers[0].pre_gene_meta_cols
                    cont_header.post_gene_meta_cols = headers[0].post_gene_meta_cols
                    headers.append(cont_header)
        
        i += 1
    
    return headers


def _parse_header_region(lines: list[str], title_idx: int) -> Optional[TableHeader]:
    """
    Parse column headers starting from a table title line.
    
    Scans forward from the title to find column header lines:
    - Skip super-headers like "GenBank accession numbers"
    - Collect gene column names (ITS, nLSU, etc.)
    - Collect metadata column names (Species, Voucher, Locality, etc.)
    """
    header = TableHeader(title_line=title_idx)
    header.header_start = title_idx + 1
    
    col_names = []
    gene_cols = []
    meta_cols = []
    
    # Scan forward from title, collecting header lines
    # Header region ends when we hit a data line (species name, accession, voucher, etc.)
    max_header_lines = 15  # Headers shouldn't be longer than this
    
    for j in range(title_idx + 1, min(title_idx + max_header_lines + 1, len(lines))):
        line = lines[j].strip()
        
        # Skip super-headers
        if is_super_header(line):
            continue
        
        # Skip the "Table N (continued)" tag itself
        if TABLE_CONTINUED_RE.search(line):
            continue
        
        # Skip table notes/footnotes that appear right after title
        if line.lower().startswith('shown in bold') or line.lower().startswith('[new'):
            continue
        
        # Check if this is a gene column header
        line_lower = line.lower().strip()
        
        # Normalize common patterns
        normalized = line_lower.replace('-', '').replace('α', 'a').replace(' ', '')
        
        if is_gene_header(line_lower) or any(g in line_lower for g in ['its', 'lsu', 'ssu', 'tef', 'rpb', 'btub']):
            gene_cols.append(line)
            col_names.append(line)
            continue
        
        # Check if it looks like a metadata header
        meta_keywords = ['species', 'genus', 'taxon', 'voucher', 'specimen', 'sample',
                        'locality', 'origin', 'country', 'collection', 'substrate', 'host',
                        'order', 'family', 'reference', 'name']
        
        is_meta = any(kw in line_lower for kw in meta_keywords)
        
        if is_meta:
            meta_cols.append(line)
            col_names.append(line)
            continue
        
        # If it's an accession column header (like "Accession #")
        if 'accession' in line_lower and not is_super_header(line):
            gene_cols.append(line)
            col_names.append(line)
            continue
        
        # If we've found at least some columns and this line doesn't look like a header,
        # it's probably the start of data
        if len(col_names) >= 2:
            # Check if this line looks like data (species, accession, voucher, etc.)
            if is_species_name(line) or is_accession_like(line) or is_voucher(line) or is_genus_only(line):
                header.header_end = j
                break
            # Could also be a multi-word header we didn't recognize
            if len(line.split()) <= 4 and not any(c.isdigit() for c in line):
                # Likely still a header (e.g., "Order/Family")
                meta_cols.append(line)
                col_names.append(line)
                continue
        
        # Default: if no columns found yet, might be a header line we don't recognize
        if len(col_names) == 0 and len(line.split()) <= 5:
            # Skip potential noise
            continue
        
        # If we've been going for too long without finding gene cols, abort
        if j - title_idx > 10 and not gene_cols:
            return None
    
    if not gene_cols:
        return None
    
    # Set header_end if not set
    if header.header_end == -1:
        header.header_end = title_idx + len(col_names) + 2  # Rough estimate
    
    header.col_names = col_names
    header.gene_cols = gene_cols
    header.meta_cols = meta_cols
    header.n_gene_cols = len(gene_cols)
    header.n_meta_cols = len(meta_cols)
    header.n_total_cols = len(col_names)
    
    # Detect if country/references come after accession columns
    # by checking column order
    _detect_post_accession_cols(header)
    
    # Split meta_cols into pre-gene and post-gene based on position in col_names
    _split_meta_by_gene_position(header)
    
    # Also flag has_reference_col if any meta_col resolves to 'reference'
    # (the column-order check may miss cases where References is listed before
    # gene columns in the header text but actually appears after them in data)
    if not header.has_reference_col:
        for mc in meta_cols:
            if _resolve_meta_col_field(mc) == 'reference':
                header.has_reference_col = True
                break
    
    return header


def _detect_post_accession_cols(header: TableHeader):
    """
    Detect if metadata columns (country, references) appear AFTER accession columns.
    This happens in some tables (e.g., Case 4: Order/Family, Species, Sample, ITS, nLSU, Country, References)
    """
    if not header.col_names:
        return
    
    # Find position of first gene column in col_names
    first_gene_idx = None
    for i, col in enumerate(header.col_names):
        col_lower = col.lower()
        if any(g in col_lower for g in ['its', 'lsu', 'ssu', 'tef', 'rpb', 'btub', 'accession']):
            first_gene_idx = i
            break
    
    if first_gene_idx is None:
        return
    
    # Check if any metadata columns come after the first gene column
    for i in range(first_gene_idx + 1, len(header.col_names)):
        col_lower = header.col_names[i].lower()
        if 'reference' in col_lower:
            header.has_reference_col = True
        if 'country' in col_lower or 'locality' in col_lower or 'origin' in col_lower:
            header.has_country_after_acc = True


def _split_meta_by_gene_position(header: TableHeader):
    """
    Split header.meta_cols into pre-gene and post-gene lists based on their
    position relative to gene columns in header.col_names.
    
    For tables like:
        Taxon | nSSU ITS nLSU | Collection (Herbarium) | Geographic origin
    
    pre_gene_meta_cols  = ['Taxon']  (before first gene col)
    post_gene_meta_cols = ['Collection', '(Herbarium)', 'Geographic origin']  (after last gene col)
    
    For tables where all meta is before genes (the common case),
    post_gene_meta_cols will be empty.
    """
    if not header.col_names or not header.gene_cols:
        header.pre_gene_meta_cols = list(header.meta_cols)
        header.post_gene_meta_cols = []
        return
    
    # Build a set for fast gene-col lookup (case-insensitive)
    gene_set = {g.strip().lower() for g in header.gene_cols}
    
    # Find first and last gene column positions in col_names
    first_gene_pos = None
    last_gene_pos = None
    for i, cn in enumerate(header.col_names):
        if cn.strip().lower() in gene_set:
            if first_gene_pos is None:
                first_gene_pos = i
            last_gene_pos = i
    
    if first_gene_pos is None:
        # No gene columns found in col_names (shouldn't happen)
        header.pre_gene_meta_cols = list(header.meta_cols)
        header.post_gene_meta_cols = []
        return
    
    # Build position map for each meta_col in col_names
    pre = []
    post = []
    for mc in header.meta_cols:
        # Find this meta_col's position in col_names
        try:
            pos = header.col_names.index(mc)
        except ValueError:
            # Not found — default to pre (safe fallback)
            pre.append(mc)
            continue
        
        if pos < first_gene_pos:
            pre.append(mc)
        elif pos > last_gene_pos:
            post.append(mc)
        else:
            # Between gene cols (unusual) — treat as pre
            pre.append(mc)
    
    header.pre_gene_meta_cols = pre
    header.post_gene_meta_cols = post


# ============================================================================
# Multi-line Country Merging
# ============================================================================

def _is_country_word_part(s: str) -> bool:
    """Check if a word is a known part of a multi-word country name."""
    return s.strip().lower() in _COUNTRY_WORD_PARTS


def _merge_multiline_countries(meta_lines: list[tuple[int, str]]) -> list[tuple[int, str]]:
    """
    Merge consecutive lines that together form a known country name.
    
    E.g., [("Dominican"), ("Republic")] → ["Dominican Republic"]
    
    This handles PDFs where multi-word country names are split across lines:
      Line 408: Dominican
      Line 409: Republic
    → merged to: "Dominican Republic"
    """
    if len(meta_lines) < 2:
        return meta_lines
    
    result = []
    i = 0
    while i < len(meta_lines):
        merged = False
        if i + 1 < len(meta_lines):
            # Only merge when the first line ALONE is NOT a country
            # (i.e. multi-word country split across lines: "Dominican" + "Republic").
            # If the first line already resolves (e.g. "USA (OR)"), skip merging.
            if not resolve_country(meta_lines[i][1].strip()):
                combined = meta_lines[i][1].strip() + " " + meta_lines[i + 1][1].strip()
                if resolve_country(combined):
                    result.append((meta_lines[i][0], combined))
                    i += 2
                    merged = True
        if not merged:
            result.append(meta_lines[i])
            i += 1
    return result


def _merge_multiline_countries_flat(meta_lines: list[str]) -> list[str]:
    """Same as _merge_multiline_countries but for flat string lists (extract_all_rows_from_pdf)."""
    if len(meta_lines) < 2:
        return meta_lines
    
    result = []
    i = 0
    while i < len(meta_lines):
        merged = False
        if i + 1 < len(meta_lines):
            if not resolve_country(meta_lines[i].strip()):
                combined = meta_lines[i].strip() + " " + meta_lines[i + 1].strip()
                if resolve_country(combined):
                    result.append(combined)
                    i += 2
                    merged = True
        if not merged:
            result.append(meta_lines[i])
            i += 1
    return result


# Pattern for a "voucher continuation" line: pure digits, or starts with #
_VOUCHER_CONTINUATION_RE = re.compile(
    r'^(?:\d{3,}|#\s*\d+)$'  # pure digits (3+), or # followed by digits
)


def _merge_multiline_vouchers(meta_lines: list[tuple[int, str]]) -> list[tuple[int, str]]:
    """
    Merge consecutive lines that together form a voucher.
    
    Handles cases where PDF line-wraps split a voucher across two lines:
      - "Mushroom Observer" + "# 115977" → "Mushroom Observer # 115977"
      - "J. & A. Guinberteau" + "99101101" → "J. & A. Guinberteau 99101101"
    
    Rules: merge line[i] + line[i+1] when:
      - line[i] is_voucher but line[i+1] is a pure-number or #-number continuation
      - OR: combined(line[i] + line[i+1]) is_voucher and neither individually is country/species/reference
    """
    if len(meta_lines) < 2:
        return meta_lines
    
    result = []
    i = 0
    while i < len(meta_lines):
        merged = False
        if i + 1 < len(meta_lines):
            cur = meta_lines[i][1].strip()
            nxt = meta_lines[i + 1][1].strip()
            
            # Case 1: current is voucher-like, next is continuation (digits or #digits)
            if is_voucher(cur) and _VOUCHER_CONTINUATION_RE.match(nxt):
                combined = cur + " " + nxt
                result.append((meta_lines[i][0], combined))
                i += 2
                merged = True
            
            # Case 1b: current ends with comma — multi-line voucher continuation
            # e.g., 'Chalange 97082101,' + 'isotype' → 'Chalange 97082101, isotype'
            elif cur.endswith(',') and len(nxt) < 30 and not resolve_country(nxt) and not is_reference(nxt) and not is_species_name(nxt) and not is_accession_like(nxt):
                combined = cur + " " + nxt
                result.append((meta_lines[i][0], combined))
                i += 2
                merged = True
            
            # Case 2: current is voucher-like (e.g., "Mushroom Observer", "J. & A. Guinberteau"),
            # next is continuation, and neither is a country or reference
            elif (not resolve_country(cur) and not is_reference(cur) and
                  not resolve_country(nxt) and not is_reference(nxt) and
                  not is_species_name(nxt)):
                combined = cur + " " + nxt
                if is_voucher(combined) and not is_voucher(cur) and not is_voucher(nxt):
                    result.append((meta_lines[i][0], combined))
                    i += 2
                    merged = True
        
        if not merged:
            result.append(meta_lines[i])
            i += 1
    return result


def _merge_multiline_vouchers_flat(meta_lines: list[str]) -> list[str]:
    """Same as _merge_multiline_vouchers but for flat string lists."""
    if len(meta_lines) < 2:
        return meta_lines
    
    result = []
    i = 0
    while i < len(meta_lines):
        merged = False
        if i + 1 < len(meta_lines):
            cur = meta_lines[i].strip()
            nxt = meta_lines[i + 1].strip()
            
            if is_voucher(cur) and _VOUCHER_CONTINUATION_RE.match(nxt):
                result.append(cur + " " + nxt)
                i += 2
                merged = True
            elif cur.endswith(',') and len(nxt) < 30 and not resolve_country(nxt) and not is_reference(nxt) and not is_species_name(nxt) and not is_accession_like(nxt):
                result.append(cur + " " + nxt)
                i += 2
                merged = True
            elif (not resolve_country(cur) and not is_reference(cur) and
                  not resolve_country(nxt) and not is_reference(nxt) and
                  not is_species_name(nxt)):
                combined = cur + " " + nxt
                if is_voucher(combined) and not is_voucher(cur) and not is_voucher(nxt):
                    result.append(combined)
                    i += 2
                    merged = True
        
        if not merged:
            result.append(meta_lines[i])
            i += 1
    return result


def _merge_multiline_species(meta_lines: list[tuple[int, str]]) -> list[tuple[int, str]]:
    """
    Merge consecutive lines where a species+authority string wraps across two lines.
    
    Handles cases where PDF line-wraps split a species authority across two lines:
      - "Macropsalliota meleagris (Gray) Kun L. Yang, Jia Y. Lin &" + "Zhu L. Yang"
      - "Leucoagaricus lahorensiformis S. Hussain, H. Ahmad," + "Afshan & Khalid"
      - "Leucocoprinus biornatus var. virens T. Lebel," + "Saba & Vellinga"
    
    Rules: merge line[i] + line[i+1] when:
      - line[i] is a species name (is_species_name + _is_valid_species_candidate)
      - line[i] ends with '&' or ',' (incomplete authority)
      - line[i+1] is NOT species, voucher, country, reference, or accession
      - line[i+1] is reasonably short (< 40 chars)
    """
    if len(meta_lines) < 2:
        return meta_lines
    
    result = []
    i = 0
    while i < len(meta_lines):
        merged = False
        if i + 1 < len(meta_lines):
            cur = meta_lines[i][1].strip()
            nxt = meta_lines[i + 1][1].strip()
            
            # Rule 1: Authority wrapping — line ends with '&' or ','
            if (is_species_name(cur) and _is_valid_species_candidate(cur)
                    and cur.rstrip().endswith(('&', ','))
                    and not is_species_name(nxt)
                    and not VOUCHER_RE.match(nxt)
                    and not resolve_country(nxt)
                    and not is_reference(nxt)
                    and not is_accession_or_dash(nxt)
                    and len(nxt) < 40):
                combined = cur + " " + nxt
                result.append((meta_lines[i][0], combined))
                i += 2
                merged = True
            
            # Rule 2: Genus on one line, epithet on the next
            # e.g. "Hymenochaete" + "huangshanensis" → "Hymenochaete huangshanensis"
            elif (is_genus_only(cur) and not resolve_country(cur)
                    and re.match(r'^[a-z]{3,}$', nxt)):
                combined = cur + " " + nxt
                if is_species_name(combined) and _is_valid_species_candidate(combined):
                    result.append((meta_lines[i][0], combined))
                    i += 2
                    merged = True
        
        if not merged:
            result.append(meta_lines[i])
            i += 1
    return result


# ============================================================================
# Core: Accession Lookup via Reverse Context Search
# ============================================================================

def _split_multi_accession_lines(
    lines: list[str],
    headers: list['TableHeader'],
) -> tuple[list[str], list['TableHeader']]:
    """
    Split lines that contain multiple accession codes into individual lines.
    
    Some PDFs pack multiple gene accessions (and voucher info) on one line:
        'HM562046 HM562247 AJ216 (LOU)'
    This splits into:
        'HM562046'
        'HM562247'
        'AJ216 (LOU)'
    
    Adjusts header indices to account for the additional lines.
    Only splits lines that are AFTER a table header (inside table data).
    
    Args:
        lines: original text lines from PDF
        headers: detected table headers (will have indices updated)
        
    Returns:
        (new_lines, updated_headers) with multi-acc lines split
    """
    if not headers:
        return lines, headers
    
    # Determine regions that are inside table data (after header_end)
    table_starts = set()
    for h in headers:
        table_starts.add(h.header_end)
    
    new_lines = []
    # Mapping from old index to new index for adjusting headers
    index_map = {}  # old_idx -> new_idx
    
    for old_idx, line in enumerate(lines):
        index_map[old_idx] = len(new_lines)
        stripped = line.strip()
        
        # Only split lines inside table data regions
        # (after first header_end and not header lines themselves)
        in_table = any(old_idx >= hs for hs in table_starts)
        
        if in_table and not is_accession_or_dash(stripped):
            tokens = stripped.split()
            acc_tokens = []
            for tok in tokens:
                norm = _normalize_accession_token(tok, strip_footnote=True)
                if is_accession_like(norm):
                    acc_tokens.append(tok)
            
            if len(acc_tokens) >= 2:
                # Multiple accessions on one line — split them
                non_acc_parts = []
                for tok in tokens:
                    if is_accession_like(_normalize_accession_token(tok, strip_footnote=True)):
                        # Flush accumulated non-acc tokens before this accession
                        if non_acc_parts:
                            new_lines.append(' '.join(non_acc_parts))
                            non_acc_parts = []
                        new_lines.append(_normalize_accession_token(tok, strip_footnote=False))
                    else:
                        non_acc_parts.append(tok)
                # Flush remaining non-acc tokens (voucher info at end)
                if non_acc_parts:
                    new_lines.append(' '.join(non_acc_parts))
                continue
        
        new_lines.append(line)
    
    # If no lines were split, return originals unchanged
    if len(new_lines) == len(lines):
        return lines, headers
    
    # Update header indices
    for h in headers:
        if h.title_line in index_map:
            h.title_line = index_map[h.title_line]
        if h.header_start in index_map:
            h.header_start = index_map[h.header_start]
        if h.header_end in index_map:
            h.header_end = index_map[h.header_end]
    
    return new_lines, headers


def lookup_accession_in_pdf(
    pdf_path: str | Path,
    target_accession: str,
) -> Optional[AccessionLookupResult]:
    """
    Find species, voucher, and country for a given GenBank accession code in a PDF.
    
    Strategy (accession-anchor + reverse context):
    1. Extract all text lines from PDF using PyMuPDF
    2. Find the target accession line
    3. Find nearest table header to understand column structure
    4. Expand the accession block (consecutive accession/dash lines around target)
    5. Extract metadata from lines before/after the block
    6. Handle forward-fill for species by looking further back
    
    Args:
        pdf_path: Path to the PDF file
        target_accession: GenBank accession code to look up (e.g., 'JQ087932')
    
    Returns:
        AccessionLookupResult or None if not found
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        logger.error(f"PDF not found: {pdf_path}")
        return None
    
    target = _normalize_accession_token(target_accession.strip(), strip_footnote=True)
    
    # Step 1: Extract text
    lines = extract_text_lines(pdf_path)
    
    # Step 1b: Find table headers BEFORE any line splitting
    headers = find_table_headers(lines)
    
    # Step 1c: Split multi-accession lines into individual lines.
    # Some PDFs pack multiple accession codes (and voucher info) on one line,
    # e.g., 'HM562046 HM562247 AJ216 (LOU)'. Split these so the rest of the
    # pipeline (block expansion, row extraction) works with one accession per line.
    lines, headers = _split_multi_accession_lines(lines, headers)
    
    # Step 2: Find target accession
    target_idx = _find_accession_line(lines, target)
    if target_idx is None:
        logger.warning(f"Accession {target} not found in {pdf_path.name}")
        return None
    
    # Step 3: Find nearest table header (already detected, use updated indices)
    header = _find_nearest_header(headers, target_idx)
    
    if header is None:
        logger.warning(f"No table header found near accession {target}")
        # Try heuristic approach without header
        return _heuristic_lookup(lines, target_idx, target)
    
    # Step 4: Expand accession block around target
    acc_block_start, acc_block_end = _expand_accession_block(lines, target_idx)
    n_acc_in_block = acc_block_end - acc_block_start
    
    # Step 5: Determine row boundaries and extract row
    result = _extract_row_from_context(
        lines, target_idx, acc_block_start, acc_block_end, header, target
    )
    
    if result:
        result.method = "pymupdf"
    
    return result


def _find_accession_line(lines: list[str], target: str) -> Optional[int]:
    """Find the line index containing the target accession code.
    
    Also handles GenBank version suffixes: if the target is 'KY948823'
    and the line contains 'KY948823.1', we still find it.
    
    Handles multi-token lines where multiple accessions and/or voucher info
    appear on the same line (e.g., 'HM562046 HM562247 AJ216 (LOU)').
    """
    for i, line in enumerate(lines):
        stripped = line.strip()
        if _normalize_accession_token(stripped, strip_footnote=True) == target:
            return i
        toks = _extract_accession_tokens(stripped, strip_footnote=True)
        if target in toks:
            return i
    return None


def _find_nearest_header(headers: list[TableHeader], target_idx: int) -> Optional[TableHeader]:
    """Find the table header closest to (and before) the target line."""
    best = None
    for h in headers:
        if h.header_end <= target_idx:
            if best is None or h.header_end > best.header_end:
                best = h
    return best


def _expand_accession_block(lines: list[str], target_idx: int) -> tuple[int, int]:
    """
    Expand outward from target to find the full block of consecutive accession/dash lines.
    
    In GenBank tables, accession codes for different genes appear on consecutive lines.
    E.g., for a row with nLSU, ITS, tef1-α, RPB2:
        JX093815   <- nLSU
        JX093771   <- ITS (target)
        JX093728   <- tef1-α
        JX093859   <- RPB2
    
    Returns (start_idx, end_idx) where end_idx is exclusive.
    """
    start = target_idx
    end = target_idx + 1
    anchor = lines[target_idx].strip()
    
    # Expand backwards — also accept lines identical to the anchor
    # (handles non-standard accessions like AY45710 repeated for multiple genes)
    while start > 0:
        prev = lines[start - 1].strip()
        if is_accession_or_dash(prev) or prev == anchor:
            start -= 1
        else:
            break
    
    # Expand forwards
    while end < len(lines):
        nxt = lines[end].strip()
        if is_accession_or_dash(nxt) or nxt == anchor:
            end += 1
        else:
            break
    
    return start, end


def _extract_row_from_context(
    lines: list[str],
    target_idx: int,
    acc_start: int,
    acc_end: int,
    header: TableHeader,
    target: str
) -> Optional[AccessionLookupResult]:
    """
    Extract a complete table row given the accession block and header info.
    
    The row structure is:
    [metadata lines] [accession block] [optional post-acc metadata]
    
    For forward-fill tables, the species might not be in the immediate metadata lines,
    so we need to look further back.
    """
    result = AccessionLookupResult(accession=target)
    n_acc = acc_end - acc_start
    
    # Determine expected number of accession columns from header
    expected_n_acc = header.n_gene_cols
    
    # Trim oversized accession blocks: voucher codes like UC2023059 or
    # H6059300 match the accession regex and get absorbed. Trim from the
    # side furthest from the target to restore the expected block size.
    # Save original start so trimmed lines can be seeded into pre_meta.
    _orig_acc_start = acc_start
    if n_acc > expected_n_acc > 0:
        target_pos = target_idx - acc_start
        trim_n = n_acc - expected_n_acc
        if target_pos >= trim_n:
            acc_start += trim_n
        else:
            acc_end -= trim_n
        n_acc = acc_end - acc_start
    
    # Rescue voucher codes absorbed into a short accession block.
    # When the block has fewer lines than expected gene columns AND
    # the line right after the block resolves as a country AND
    # the header defines post-gene voucher columns, the last non-target
    # line in the block is likely a voucher code (e.g. SP394387 from
    # herbarium SP) rather than an accession.  Move it out of the block
    # and seed it into post_meta so it becomes a voucher value.
    _orig_acc_end = acc_end          # save for seeding rescued lines
    if (1 < n_acc <= expected_n_acc
            and header and header.post_gene_meta_cols
            and acc_end < len(lines)
            and resolve_country(lines[acc_end].strip())):
        _post_fields = _build_post_col_fields(header)
        if any(f == 'voucher' for f in _post_fields):
            # Find the non-target line furthest from the target
            if target_idx == acc_start:
                # Target is at start — trim from end
                acc_end -= 1
            else:
                # Target is at end — trim from start
                acc_start += 1
            n_acc = acc_end - acc_start
    
    # Rescue truncated accessions adjacent to an undersized block.
    # PDF extraction sometimes strips a leading letter from 2-letter accessions
    # (e.g., JX093814 → X093814), producing [A-Z]\d{6,8} which fails
    # ACCESSION_STRICT_RE.  When the block is still too small, absorb
    # adjacent near-accession lines so the row alignment stays correct.
    _NEAR_ACC_RE = re.compile(r'^[A-Z]\d{6,8}$')
    _near_acc_rescued = False
    if 0 < n_acc < expected_n_acc:
        if acc_start > 0:
            _prev = lines[acc_start - 1].strip()
            if _NEAR_ACC_RE.match(_prev):
                acc_start -= 1
                n_acc += 1
                _near_acc_rescued = True
        if n_acc < expected_n_acc and acc_end < len(lines):
            _nxt = lines[acc_end].strip()
            if _NEAR_ACC_RE.match(_nxt):
                acc_end += 1
                n_acc += 1
                _near_acc_rescued = True
        # Keep saved boundaries in sync so pre/post_meta seeding works
        _orig_acc_start = min(_orig_acc_start, acc_start)
        _orig_acc_end = max(_orig_acc_end, acc_end)

    # Check if header has a substrate column (affects SP-ACC-CO heuristic)
    _header_has_substrate = False
    if header.meta_cols:
        _pcf = _build_pre_col_fields(header)
        _header_has_substrate = 'substrate' in _pcf
    
    # Count metadata lines before accession block
    # Walk backwards from acc_start, collecting non-noise lines.
    # Seed with any accession-like lines trimmed from the block top —
    # these are likely voucher codes (e.g., UC2023059, H6059300).
    pre_meta = [(idx, lines[idx].strip()) for idx in range(_orig_acc_start, acc_start)]
    j = _orig_acc_start - 1
    # Expected number of pre-accession metadata: 
    # In most cases: species, voucher, locality = 2-4 fields
    max_pre_meta = header.n_meta_cols + 2  # Allow some slack
    # Build set of known header column names to detect repeated table headers
    # on new pages (e.g., "TABLE 1. (Continued)" followed by column names)
    _header_col_names = set()
    for _gc in header.gene_cols:
        _header_col_names.add(_gc.strip().lower())
    for _mc in header.meta_cols:
        _mc_stripped = _mc.strip()
        if len(_mc_stripped) <= 40:  # skip long descriptive text
            _header_col_names.add(_mc_stripped.lower())
    # Add common column keywords that appear in continued table headers
    _header_col_names.update([
        'genbank accession no.', 'genbank accession no', 'genbank no.',
    ])
    _stopped_at_accession = False
    while j >= 0 and len(pre_meta) < max_pre_meta:
        # Stop if we've walked back into the table header area
        if j < header.header_end:
            break
        
        line = lines[j].strip()
        
        # Stop if this line is a repeated table column name from a continued
        # table header on a new page (e.g., 'ITS', 'nLSU', 'Species Name')
        if line.lower() in _header_col_names:
            break
        
        # Stop if we hit another accession block (previous row's tail)
        # Also check for accession codes with trailing footnote markers
        # (e.g. "MF972231a" where 'a' is a PDF superscript footnote)
        _line_no_fn = re.sub(r'[a-e*†‡§]+$', '', line)
        if is_accession_or_dash(line) or (_line_no_fn != line and is_accession_or_dash(_line_no_fn)):
            # Check for SP-ACC-CO pattern: an accession-like token sandwiched
            # between a species line and a country line is likely a voucher
            # (e.g. LE231603 is herbarium code LE + specimen 231603)
            # EXCEPT: when the header has a Substrate column, the "species" line
            # before the accession is likely a substrate (host plant), and the
            # "country" after is from the next row. Skip this heuristic in that case.
            if j > 0 and not _header_has_substrate:
                prev = lines[j - 1].strip()
                nxt = lines[j + 1].strip() if j + 1 < len(lines) else ''
                _prev_taxonish = (
                    (is_species_name(prev) and _is_valid_species_candidate(prev))
                    or bool(re.match(r'^[A-Z][A-Za-z\-]+\s+sp\.?$', prev))
                )
                if (_prev_taxonish and resolve_country(nxt)
                        and not is_reference(prev) and not resolve_country(prev)):
                    pre_meta.insert(0, (j, line))
                    j -= 1
                    continue
            _stopped_at_accession = True
            break
        if TABLE_TITLE_RE.search(line) or TABLE_CONTINUED_RE.search(line):
            break
        
        # Skip page noise (page numbers, journal headers)
        if is_page_noise(line):
            # Don't skip bare numbers (1-3 digits, or digit/digit) that sit right
            # before a country code — they're voucher/specimen numbers, not page numbers.
            # E.g. '21' between 'Flabellophora parva' and 'BR' is a voucher code.
            if (re.fullmatch(r'\d{1,3}(?:/\d+)?', line)
                    and pre_meta and resolve_country(pre_meta[-1][1])):
                pass  # Include — likely a voucher number
            else:
                j -= 1
                continue
        
        # Skip footnote lines
        if line.startswith('T, IT, PT') or line.startswith('a ') or line.startswith('b '):
            j -= 1
            continue
        
        pre_meta.insert(0, (j, line))
        j -= 1
    
    # Trim previous row's post-accession data from pre_meta.
    # In tables with post-gene columns (Collection, Geographic origin, etc.),
    # the PDF layout is: ...acc_prev | post_meta_prev | pre_meta_curr | acc_curr...
    # The backward scan collects post_meta_prev into pre_meta by mistake.
    # Remove leading non-species lines when the scan stopped at an accession
    # boundary and the header has post-gene DATA columns (voucher, country).
    # Don't trim when post-gene columns are only Reference — that doesn't
    # leak data rows that look like species/country/voucher.
    _post_has_data_cols = bool(
        header and header.post_gene_meta_cols
        and any(
            f in ('voucher', 'country')
            for f in _build_post_col_fields(header)
        )
    )
    if (_stopped_at_accession
            and _post_has_data_cols
            and len(pre_meta) > 1):
        # Trim leading lines until we hit a species-like line
        while len(pre_meta) > 1:
            _, _lead = pre_meta[0]
            if is_species_name(_lead) and _is_valid_species_candidate(_lead):
                break  # This is the current row's species — keep it
            pre_meta.pop(0)
    
    # Collect post-accession metadata (country, references in some tables)
    # Always collect a few lines — even when the header doesn't explicitly
    # indicate post-accession columns, country/reference may appear there
    # (e.g., tables with Order/Family, Species, Sample No., ITS, nLSU, Country, Ref).
    post_col_fields = _build_post_col_fields(header) if header else []
    # Seed with any accession-like lines rescued from the block tail —
    # these are likely voucher codes (e.g. SP394387, herbarium SP).
    post_meta = [(idx, lines[idx].strip()) for idx in range(acc_end, _orig_acc_end)]
    if True:  # Always collect post-meta
        k = _orig_acc_end   # start scanning after rescued lines
        # Allow more post lines when header has post-gene meta columns
        max_post = max(3, len(post_col_fields) + 1)
        while k < len(lines) and len(post_meta) < max_post:
            line = lines[k].strip()
            
            # Stop if we hit an accession or another species/voucher (next row)
            # Don't stop at species-like strings that resolve as countries
            # (e.g. "Rep. of Congo" matches SPECIES_RE but is a country name)
            if is_accession_or_dash(line):
                break
            if is_species_name(line) and not resolve_country(line):
                # Don't stop at collector names (e.g. "Pearson s.n.",
                # "Josserand s.n") or environmental samples — these are
                # post-accession metadata, not a new species row.
                _is_collector = bool(re.search(r'\bs\.?n\.?\b', line, re.IGNORECASE))
                _is_env_sample = line.lower().startswith(('environmental', 'uncultured'))
                if not _is_collector and not _is_env_sample:
                    break
            
            # Stop at page noise
            if is_page_noise(line) or TABLE_TITLE_RE.search(line):
                break
            
            post_meta.append((k, line))
            k += 1
    
    # === Map values to fields ===
    
    # 0. Merge multi-line countries in pre_meta and post_meta
    pre_meta = _merge_multiline_countries(pre_meta)
    post_meta = _merge_multiline_countries(post_meta)
    
    # 0b. Merge multi-line vouchers in pre_meta
    pre_meta = _merge_multiline_vouchers(pre_meta)
    
    # 0c. Trim leading reference/noise lines from pre_meta.
    # When the header indicates a References column, text lines belonging to
    # the previous row's Reference field (and its country) leak into the
    # current row's pre_meta. Remove these leading references, countries,
    # taxonomic rank headers, and GenBank noise as long as we don't reduce
    # below the expected number of actual data fields (species, voucher, etc.).
    if header.has_reference_col and pre_meta:
        _pre_col_fields_trim = _build_pre_col_fields(header) if header.meta_cols else []
        # Count actual pre-accession data fields for minimum threshold.
        # Exclude None, substrate, and reference — reference data appears AFTER
        # accessions (in post_meta), not before them.
        _n_expected = max(1, sum(
            1 for f in _pre_col_fields_trim
            if f is not None and f not in ('substrate', 'reference')
        )) if _pre_col_fields_trim else 1
        # Phase 1: ALWAYS trim leading references from pre_meta.
        # A reference at position 0 is almost certainly leaked from the
        # previous row (real data starts with species/voucher, not references).
        # This must run unconditionally — even when len(pre_meta) == _n_expected
        # (which happens when species+voucher are merged into one line).
        while pre_meta:
            _trim_text = pre_meta[0][1]
            _trim_stripped = _trim_text.strip()
            if is_reference(_trim_text):
                # Keep if it's a genuine species with authority
                # (e.g., "F. aethiopica Decock et al.").
                # Reject reference-like strings that superficially match species
                # (e.g., "Wang er al. (2021b)" — 'er' is only 2 chars, not a real epithet).
                _guard_parts = _trim_stripped.split()
                _has_binomial = (len(_guard_parts) >= 2
                                 and re.match(r'^[A-Z]', _guard_parts[0])
                                 and re.match(r'^[a-z]{3,}', _guard_parts[1]))
                if (_has_binomial
                        and is_species_name(_trim_text)
                        and _is_valid_species_candidate(_trim_text)):
                    break
                pre_meta.pop(0)
                continue
            # "Author and Author" / "Author & Author" without year — split reference
            # e.g., 'Wang and Zhou' where '(2024)' is on the next line.
            # NOT in global REFERENCE_RE to avoid blocking authority merges.
            if re.match(r'^[A-Z][a-z]+\s+(?:and|&)\s+[A-Z][a-z]+$', _trim_stripped):
                pre_meta.pop(0)
                continue
            # Incomplete reference: first author + trailing conjunction
            # e.g., 'Miettinen and', 'Fernandez-Lopez et', 'Wang &'
            if re.match(r'^[A-Z][a-z]+(?:-[A-Z][a-z]+)?\s+(?:and|et|&)\s*$', _trim_stripped):
                pre_meta.pop(0)
                continue
            # Standalone year — continuation of multi-line reference
            # e.g., "(2006)", "(2024)", "(2021a)", "2006"
            if re.fullmatch(r'\(?(?:19|20)\d{2}[a-z]?\)?', _trim_stripped):
                pre_meta.pop(0)
                continue
            # GenBank submission date noise (e.g., "25.IX.2013 GenBank")
            if re.search(r'\bGenBank\b', _trim_text, re.IGNORECASE):
                pre_meta.pop(0)
                continue
            break
        # Phase 2: trim ALL noise types (references, country, taxonomy, etc.)
        # subject to _n_expected minimum.
        while pre_meta and len(pre_meta) > _n_expected:
            _trim_text = pre_meta[0][1]
            _trim_stripped = _trim_text.strip()
            # Trim leaked reference from previous row
            if is_reference(_trim_text):
                _guard_parts = _trim_stripped.split()
                _has_binomial = (len(_guard_parts) >= 2
                                 and re.match(r'^[A-Z]', _guard_parts[0])
                                 and re.match(r'^[a-z]{3,}', _guard_parts[1]))
                if (_has_binomial
                        and is_species_name(_trim_text)
                        and _is_valid_species_candidate(_trim_text)):
                    break
                pre_meta.pop(0)
                continue
            # "Author and Author" without year (split reference)
            if re.match(r'^[A-Z][a-z]+\s+(?:and|&)\s+[A-Z][a-z]+$', _trim_stripped):
                pre_meta.pop(0)
                continue
            # Incomplete reference: first author + trailing conjunction
            if re.match(r'^[A-Z][a-z]+(?:-[A-Z][a-z]+)?\s+(?:and|et|&)\s*$', _trim_stripped):
                pre_meta.pop(0)
                continue
            # Standalone year (continuation of multi-line reference)
            if re.fullmatch(r'\(?(?:19|20)\d{2}[a-z]?\)?', _trim_stripped):
                pre_meta.pop(0)
                continue
            # Trim leaked country from previous row
            if resolve_country(_trim_text):
                pre_meta.pop(0)
                continue
            # Also trim GenBank submission date noise (e.g., "24.II.2010 GenBank")
            if re.search(r'\bGenBank\b', _trim_text, re.IGNORECASE):
                pre_meta.pop(0)
                continue
            # Trim taxonomic rank headers: "-/Schizoporaceae", "Hymenochaetales/",
            # "Hymenochaetales/Chaetoporellaceae", "-/Incertae sedis", etc.
            if re.match(r'^-/', _trim_text) or re.search(r'/\s*$', _trim_text):
                pre_meta.pop(0)
                continue
            # Trim standalone family (-aceae) or order (-ales) names
            if re.match(r'^[A-Z][a-z]*(aceae|ales)$', _trim_stripped):
                pre_meta.pop(0)
                continue
            # Trim lines containing (Year) that aren't valid species —
            # handles reference typos like 'Wang er al. (2021b)'
            if (re.search(r'\((?:19|20)\d{2}[a-z]?\)', _trim_stripped)
                    and not (is_species_name(_trim_text)
                             and _is_valid_species_candidate(_trim_text))):
                pre_meta.pop(0)
                continue
            break
    
    # 0d. Merge multi-line species in pre_meta (authority continuations)
    pre_meta = _merge_multiline_species(pre_meta)
    
    # 1. Map accession block to gene columns
    acc_lines = [(i, lines[i].strip()) for i in range(acc_start, acc_end)]
    # Extract embedded voucher when accession and voucher share one PDF line
    # (e.g. "HM562258 AEF735 (MICH)" → accession HM562258, voucher AEF735 (MICH))
    _embedded_voucher = None
    for _ai, (_alidx, _alval) in enumerate(acc_lines):
        _m_emb = re.match(r'(' + re.escape(target) + r')\s+(.+)', _alval)
        if _m_emb:
            acc_lines[_ai] = (_alidx, _m_emb.group(1))
            _embedded_voucher = _m_emb.group(2)
            break
    _map_accessions_to_genes(result, acc_lines, header, target)
    
    # 2. Extract species, voucher, country from pre_meta
    #    Strategy: use POSITIONAL mapping when header has named columns and
    #    alignment is good. This correctly handles 'Substrate' columns that
    #    contain host plant names (which look like species to heuristic matching).
    #    Fall back to heuristic classification only when positional fails.
    species_found = False
    positional_done = False
    
    if header.meta_cols:
        pre_col_fields = _build_pre_col_fields(header)
        n_pre = len(pre_meta)
        n_cols = len(pre_col_fields)
        
        if n_pre == n_cols and n_cols > 0:
            # Perfect alignment — use positional mapping directly
            positional_done = True
            for i, field in enumerate(pre_col_fields):
                _, line_text = pre_meta[i]
                if field == 'species':
                    if is_species_name(line_text) and _is_valid_species_candidate(line_text):
                        result.species = line_text
                        species_found = True
                    elif is_genus_only(line_text) and _is_valid_species_candidate(line_text):
                        # Genus-only (e.g., "Aurificaria") — check if next pos is the real species
                        if i + 1 < n_pre and pre_col_fields[i + 1] == 'species':
                            pass  # Will be set by next position
                        else:
                            result.species = line_text
                            species_found = True
                elif field == 'country':
                    resolved = resolve_country(line_text)
                    if resolved:
                        result.country = resolved
                elif field == 'voucher':
                    if (line_text.strip()
                            and not _is_citation_marker(line_text)
                            and not (is_species_name(line_text)
                                     and _is_valid_species_candidate(line_text))):
                        result.voucher = line_text.strip()
                elif field == 'substrate':
                    pass  # Explicitly skip substrate (host plant)
                # None / unknown fields: skip
        
        elif n_pre == n_cols + 1 and n_cols > 0:
            # One extra line — could be genus group header at start
            # OR a parenthetical qualifier at the end.
            # If first field is 'species' and pre_meta[0] IS a species,
            # the extra line is at the END → use offset=0.
            positional_done = True
            offset = 1  # Default: skip the extra line at start
            if (pre_col_fields and pre_col_fields[0] == 'species'
                    and pre_meta
                    and is_species_name(pre_meta[0][1])
                    and _is_valid_species_candidate(pre_meta[0][1])):
                offset = 0  # Species IS at position 0; extra line at end
            for i, field in enumerate(pre_col_fields):
                meta_idx = i + offset
                if meta_idx >= n_pre:
                    continue
                _, line_text = pre_meta[meta_idx]
                if field == 'species':
                    if is_species_name(line_text) and _is_valid_species_candidate(line_text):
                        result.species = line_text
                        species_found = True
                    elif is_genus_only(line_text) and _is_valid_species_candidate(line_text):
                        result.species = line_text
                        species_found = True
                elif field == 'country':
                    resolved = resolve_country(line_text)
                    if resolved:
                        result.country = resolved
                elif field == 'voucher':
                    if (line_text.strip()
                            and not _is_citation_marker(line_text)
                            and not (is_species_name(line_text)
                                     and _is_valid_species_candidate(line_text))):
                        result.voucher = line_text.strip()
                elif field == 'substrate':
                    pass  # Skip
        
        elif n_pre == n_cols - 1 and n_cols > 0:
            # One line missing (typically species for forward-fill) 
            positional_done = True
            # Determine which field is missing by checking if first col is species
            if pre_col_fields and pre_col_fields[0] == 'species':
                # Check if first pre_meta item IS actually a species
                # (meaning it's NOT species that's missing — voucher or another
                # field was merged into the species line). Fall to heuristic
                # which handles merged species+voucher via step 7b.
                first_text = pre_meta[0][1] if pre_meta else ''
                if is_species_name(first_text) and _is_valid_species_candidate(first_text):
                    # Species IS present → merged field case → use heuristic
                    positional_done = False
                else:
                    # Species is truly missing → map remaining from position 1
                    # EXCEPT: when the backward scan stopped at another
                    # accession block AND the lines right after the current
                    # block contain both a voucher candidate and a country,
                    # these pre_meta lines are the PREVIOUS row's
                    # post-accession data.  Skip filling here so the
                    # post_meta heuristic (step 3) provides correct values.
                    _pre_is_prev_row = False
                    if _stopped_at_accession and _near_acc_rescued:
                        # Near-acc rescue expanded the block past the gap
                        # between previous-row data and original block start.
                        # The pre_meta lines are the previous row's tail.
                        # Peek at post-block lines to confirm current row's
                        # data is available there instead.
                        _pk_co = _pk_vo = False
                        for _pk in range(_orig_acc_end,
                                         min(len(lines), _orig_acc_end + 3)):
                            _pkl = lines[_pk].strip()
                            if is_accession_or_dash(_pkl):
                                break
                            if resolve_country(_pkl):
                                _pk_co = True
                            elif (_pkl
                                  and not is_reference(_pkl)
                                  and not is_page_noise(_pkl)):
                                _pk_vo = True
                        _pre_is_prev_row = _pk_co and _pk_vo
                    if not _pre_is_prev_row:
                        for i in range(1, n_cols):
                            meta_idx = i - 1  # pre_meta is offset by -1
                            if meta_idx >= n_pre:
                                continue
                            field = pre_col_fields[i]
                            _, line_text = pre_meta[meta_idx]
                            if field == 'country':
                                resolved = resolve_country(line_text)
                                if resolved:
                                    result.country = resolved
                            elif field == 'voucher':
                                if (line_text.strip()
                                        and not _is_citation_marker(line_text)
                                        and not (is_species_name(line_text)
                                                 and _is_valid_species_candidate(line_text))):
                                    result.voucher = line_text.strip()
                            elif field == 'substrate':
                                pass  # Skip
            else:
                # Some other field missing — can't reliably determine which,
                # fall through to heuristic
                positional_done = False
        
        if not positional_done and n_pre > 0 and n_pre < n_cols:
            # Alignment gap > 1: typically None-mapped columns (Order/Family etc.)
            # are empty. Try aligning pre_meta against only the non-None fields.
            _active_fields = [(j, f) for j, f in enumerate(pre_col_fields) if f is not None]
            if len(_active_fields) == n_pre:
                positional_done = True
                for meta_idx, (_, field) in enumerate(_active_fields):
                    _, line_text = pre_meta[meta_idx]
                    if field == 'species':
                        if is_species_name(line_text) and _is_valid_species_candidate(line_text):
                            result.species = line_text
                            species_found = True
                        elif is_genus_only(line_text) and _is_valid_species_candidate(line_text):
                            result.species = line_text
                            species_found = True
                    elif field == 'country':
                        resolved = resolve_country(line_text)
                        if resolved:
                            result.country = resolved
                    elif field == 'voucher':
                        if (line_text.strip()
                                and not _is_citation_marker(line_text)
                                and not (is_species_name(line_text)
                                         and _is_valid_species_candidate(line_text))):
                            result.voucher = line_text.strip()
                    elif field == 'substrate':
                        pass  # Skip
    
    # Heuristic fallback (when positional mapping not possible or not aligned)
    # Uses "last species wins" since positional mapping already handles substrate
    # columns; heuristic path only runs for tables WITHOUT known substrate columns.
    if not positional_done:
        for idx, (line_idx, line) in enumerate(pre_meta):
            resolved = resolve_country(line)
            # Check species BEFORE reference — species with authorship
            # (e.g. "F. aethiopica Decock et al.") match is_reference() due
            # to "et al." but must still be recognized as species.
            if is_species_name(line) and _is_valid_species_candidate(line):
                result.species = line
                species_found = True
            elif is_reference(line):
                pass  # Skip reference lines (e.g. "Justo et al. 2021")
            elif _is_citation_marker(line):
                pass
            elif resolved:
                result.country = resolved
            elif is_voucher(line):
                result.voucher = line
            elif not species_found and is_genus_only(line) and _is_valid_species_candidate(line):
                if idx + 1 < len(pre_meta) and is_species_name(pre_meta[idx + 1][1]):
                    pass  # Next line is the species
                else:
                    result.species = line
                    species_found = True
    
    # 3. Extract from post_meta (country, voucher, references)
    # Use positional mapping when header defines post-gene columns
    post_positional_done = False
    if post_col_fields and post_meta:
        n_post = len(post_meta)
        n_post_cols = len(post_col_fields)
        # When more post lines than expected, trim trailing noise
        # (figure captions, next-row species that slipped through).
        # Only trim when all fields are mapped (non-None); otherwise the
        # heuristic fallback is needed for unmapped column data.
        if (n_post > n_post_cols > 0
                and all(f is not None for f in post_col_fields)):
            post_meta = post_meta[:n_post_cols]
            n_post = n_post_cols
        if n_post == n_post_cols:
            post_positional_done = True
            for pi, pfield in enumerate(post_col_fields):
                _, pline = post_meta[pi]
                if pfield == 'voucher':
                    # Don't assign a country value to voucher field
                    if resolve_country(pline):
                        if not result.country:
                            result.country = resolve_country(pline)
                    elif _is_citation_marker(pline):
                        pass
                    elif (pline.strip()
                            and not (is_species_name(pline)
                                     and _is_valid_species_candidate(pline)
                                     # Collector notation ("s.n.") and
                                     # environmental/uncultured samples look
                                     # like species to regex — keep as voucher
                                     and not re.search(r'\bs\.?n\.?\b', pline, re.IGNORECASE)
                                     and not pline.strip().lower().startswith(
                                         ('environmental', 'uncultured')))):
                        if not result.voucher:
                            result.voucher = pline.strip()
                        else:
                            result.voucher = result.voucher + ' ' + pline.strip()
                elif pfield == 'country':
                    resolved = resolve_country(pline)
                    if resolved and not result.country:
                        result.country = resolved
                elif pfield == 'species':
                    if (is_species_name(pline) and _is_valid_species_candidate(pline)
                            and not species_found):
                        result.species = pline
                        species_found = True
                # None / substrate / reference: skip
        elif n_post == n_post_cols - 1 and n_post_cols > 1:
            # One line fewer than expected (e.g., missing voucher on forward-fill)
            post_positional_done = True
            # Try aligning by skipping the first expected column
            for pi in range(1, n_post_cols):
                meta_idx = pi - 1
                if meta_idx >= n_post:
                    continue
                pfield = post_col_fields[pi]
                _, pline = post_meta[meta_idx]
                if pfield == 'voucher' and not result.voucher:
                    # Don't assign a country value to voucher field
                    if resolve_country(pline):
                        if not result.country:
                            result.country = resolve_country(pline)
                    elif _is_citation_marker(pline):
                        pass
                    elif (pline.strip()
                            and not (is_species_name(pline)
                                     and _is_valid_species_candidate(pline)
                                     and not re.search(r'\bs\.?n\.?\b', pline, re.IGNORECASE)
                                     and not pline.strip().lower().startswith(
                                         ('environmental', 'uncultured')))):
                        result.voucher = pline.strip()
                elif pfield == 'country' and not result.country:
                    resolved = resolve_country(pline)
                    if resolved:
                        result.country = resolved
    
    # Heuristic fallback for post_meta (when positional not done)
    if not post_positional_done:
        for line_idx, line in post_meta:
            resolved = resolve_country(line)
            if resolved and not result.country:
                result.country = resolved
            elif is_reference(line):
                pass  # We don't store references for now
            elif _is_citation_marker(line):
                pass
            elif not result.voucher:
                # Try splitting combined voucher+country (e.g.
                # "Pearson s.n. (MICH, as P. plautus) UK (England)")
                v_part, c_part = _split_voucher_country(line)
                if c_part:
                    result.voucher = v_part
                    if not result.country:
                        result.country = c_part
                elif line.strip() and not (is_species_name(line)
                        and _is_valid_species_candidate(line)):
                    result.voucher = line.strip()
    
    # 3b. Positional fallback for unrecognized voucher codes
    # Only needed when heuristic path was used (positional already handled this)
    if not positional_done and header.meta_cols and pre_meta:
        _positional_fill(result, pre_meta, header, species_found)
    
    # 4. Handle forward-fill: if no species found, look further back
    if not species_found:
        # Start searching from BEFORE the current row's pre_meta,
        # not from before the accession block. This avoids re-encountering
        # substrate lines from the current row.
        ff_start = pre_meta[0][0] if pre_meta else acc_start
        species = _find_forward_fill_species(lines, ff_start, header)
        if species:
            result.species = species
            result.confidence = "medium"  # Lower confidence due to forward-fill
    
    # 5. If country was in pre_meta but before a voucher that we didn't catch
    # (sometimes country is the line right before the accession block when there's
    #  no separate locality column, or it's in a combined position)
    if not result.country:
        # Check pre_meta for anything that looks like a country
        for line_idx, line in pre_meta:
            resolved = resolve_country(line)
            if resolved:
                result.country = resolved
                break
    
    # 5b. Apply embedded voucher extracted from accession line
    # (e.g., "HM562258 AEF735 (MICH)" → voucher "AEF735 (MICH)")
    if _embedded_voucher and not result.voucher:
        result.voucher = _embedded_voucher

    # 6. Split trailing country from voucher (e.g., "MFLU:19-1331, holotype Thailand")
    if result.voucher and not result.country:
        result.voucher, split_country = _split_voucher_country(result.voucher)
        if split_country:
            result.country = split_country

    # 6b. Voucher rescue fallback for rows where voucher wasn't assigned.
    # Recover voucher-like IDs that may look accession-like but are not part of
    # the current row gene accessions (e.g., GB0090937 in a Voucher column).
    if not result.voucher:
        known_row_acc = {
            _normalize_accession_token(v, strip_footnote=True)
            for v in result.other_accessions.values()
            if v
        }
        known_row_acc.add(_normalize_accession_token(result.accession, strip_footnote=True))
        for _, cand in pre_meta + post_meta:
            c = cand.strip()
            if (not c or bool(DASH_RE.match(c)) or _is_citation_marker(c)
                    or resolve_country(c) or is_reference(c)):
                continue
            if is_species_name(c) and _is_valid_species_candidate(c):
                continue
            # Prefer explicit voucher patterns first.
            if is_voucher(c):
                result.voucher = c
                break
            # Fallback: accession-like token not used as current row gene accession.
            if is_accession_like(c):
                c_norm = _normalize_accession_token(c, strip_footnote=True)
                if c_norm not in known_row_acc:
                    result.voucher = c
                    break
    
    # 7. If species was found but looks like it has author/voucher info concatenated,
    # clean it up. Also extract embedded voucher from species+author lines.
    if result.species:
        raw_species_line = result.species
        result.species = _clean_species_name(result.species)
        
        # 7b. Extract embedded voucher from species+author line
        # E.g., "Leucoagaricus subpurpureolilacinus Z.W. Ge & Zhu L. Yang HKAS:48285, holotype"
        # After cleaning species, search remainder for institutional voucher code
        if not result.voucher and raw_species_line != result.species:
            remainder = raw_species_line[len(result.species):].strip()
            # Use unanchored pattern for institutional codes (PREFIX:code or PREFIX code)
            m = re.search(r'[A-Z]{2,7}[-:]\s*[A-Za-z]*\d+', remainder)
            if m:
                voucher_text = remainder[m.start():].strip()
                if not is_reference(voucher_text):
                    result.voucher = voucher_text
    
    # 8. Strip PDF superscript footnote markers from voucher codes.
    # E.g., 'MUCL 51555a,b' → 'MUCL 51555', 'MUCL 46017a,b (=CRGF 182)' → 'MUCL 46017 (=CRGF 182)'
    if result.voucher:
        result.voucher = re.sub(
            r'(?<=[\d)])\s*[a-e](?:\s*,\s*[a-e])*(?=\s+\(|\s*$)',
            '', result.voucher,
        )

    # Store raw row for debugging
    result.raw_row_lines = [line for _, line in pre_meta] + \
                           [lines[i] for i in range(acc_start, acc_end)] + \
                           [line for _, line in post_meta]
    
    return result


# Normalized meta column names → field mapping (exact matches)
_META_COL_FIELD_MAP = {
    'species': 'species', 'taxon': 'species', 'name': 'species',
    'voucher': 'voucher', 'specimen': 'voucher', 'herbarium': 'voucher',
    'collection': 'voucher', 'sample': 'voucher', 'sample no': 'voucher',
    'sample no.': 'voucher',
    'country': 'country', 'origin': 'country', 'locality': 'country',
    'location': 'country', 'loc.': 'country', 'loc': 'country',
    'substrate': 'substrate', 'host': 'substrate', 'host plant': 'substrate',
}


def _resolve_meta_col_field(col_name: str) -> str | None:
    """
    Map a column header name to a field using keyword matching.
    
    Returns one of: 'species', 'voucher', 'country', 'substrate', 'reference', None
    
    Handles complex column names like:
    - 'Genera / Species name' → 'species'
    - 'Collection reference' → 'voucher'  (not a bibliographic reference!)
    - 'Substrate' → 'substrate' (host plant — must NOT be confused with species)
    """
    col_lower = col_name.strip().lower().rstrip('.')
    
    # Direct lookup first (fast path)
    field = _META_COL_FIELD_MAP.get(col_lower)
    if field:
        return field
    
    # Keyword-based matching for complex / multi-word column names
    # Order matters: more specific checks first
    
    # Species-related ("Genera / Species name", "Species/Taxa")
    if any(kw in col_lower for kw in ('species', 'taxon', 'organism', 'genera')):
        return 'species'
    
    # Substrate/host ("Substrate", "Host plant") — BEFORE voucher check
    # to avoid 'host collection' being misidentified
    if any(kw in col_lower for kw in ('substrate', 'host')):
        return 'substrate'
    
    # Voucher/collection — "Collection reference" is a voucher column, not
    # a bibliographic reference. Check 'collection' keyword presence.
    # "Sample No." is a common synonym for voucher.
    if any(kw in col_lower for kw in ('voucher', 'specimen', 'herbarium', 'collection', 'sample')):
        return 'voucher'
    
    # Country/locality
    if any(kw in col_lower for kw in ('country', 'origin', 'locality', 'location')):
        return 'country'
    
    # Pure reference column ("Reference", "References", "Ref.")
    # Must NOT match "Collection reference" — that's handled above as voucher
    if col_lower in ('reference', 'references', 'ref', 'ref.'):
        return 'reference'
    
    return None


def _build_col_fields_from_meta_list(meta_col_list: list[str], skip_references: bool = True) -> list[str | None]:
    """
    Build positional field mapping from a list of meta column names.
    
    Shared logic for both pre-accession and post-accession column mapping.
    
    Args:
        meta_col_list: list of column header strings to map
        skip_references: if True, skip 'reference' columns (pre-acc behavior)
    """
    _GROUP_HEADERS = {'genbank', 'genbank no', 'accession no', 'accession number'}
    
    col_fields = []
    raw_mappings = []
    
    for col_name in meta_col_list:
        col_lower = col_name.strip().lower().rstrip('.')
        
        if len(col_name.strip()) > 40:
            continue
        
        if col_lower in _GROUP_HEADERS:
            continue
        
        field = _resolve_meta_col_field(col_name)
        
        if skip_references and field == 'reference':
            continue
        
        raw_mappings.append((col_lower, field))
        col_fields.append(field)
    
    # Disambiguation: if 'specimen' mapped to 'voucher' but there's also an
    # explicit 'voucher' column, re-map 'specimen' to 'species'.
    has_explicit_voucher = any(
        col_lower != 'specimen' and field == 'voucher'
        for col_lower, field in raw_mappings
    )
    if has_explicit_voucher:
        for i, (col_lower, field) in enumerate(raw_mappings):
            if col_lower == 'specimen' and field == 'voucher':
                col_fields[i] = 'species'
    
    return col_fields


def _build_pre_col_fields(header: 'TableHeader') -> list[str | None]:
    """
    Build the positional field mapping for pre-accession metadata columns.
    
    When the header has post-gene meta columns (e.g., Collection, Geographic origin
    appearing AFTER gene cols), only the pre-gene meta columns are used here.
    Otherwise falls back to all meta_cols (backward-compatible).
    """
    # Use pre_gene_meta_cols when available and non-empty;
    # otherwise fall back to full meta_cols for backward compatibility
    if header.post_gene_meta_cols:
        source = header.pre_gene_meta_cols
    else:
        source = header.meta_cols
    return _build_col_fields_from_meta_list(source, skip_references=True)


def _build_post_col_fields(header: 'TableHeader') -> list[str | None]:
    """
    Build the positional field mapping for post-accession metadata columns.
    
    Only returns fields when the header has columns after gene columns
    (e.g., Collection, Geographic origin in Pluteaceae tables).
    Returns empty list for tables with no post-gene meta columns.
    """
    if not header.post_gene_meta_cols:
        return []
    return _build_col_fields_from_meta_list(header.post_gene_meta_cols, skip_references=True)


def _positional_fill(
    result: AccessionLookupResult,
    pre_meta: list[tuple[int, str]],
    header: 'TableHeader',
    species_found: bool,
) -> None:
    """
    Positional fallback: use header meta_cols order to fill missing fields.
    
    When the header defines named columns like [Species, Voucher, Origin, References],
    the pre_meta lines should appear in the same order. If a field is still empty
    after heuristic classification, use the positional mapping to fill it.
    
    This handles unusual voucher codes (e.g. '420526MF0068') that don't match
    VOUCHER_RE but sit in the correct 'Voucher' column position.
    """
    pre_col_fields = _build_pre_col_fields(header)
    
    if not pre_col_fields:
        return
    
    # Check alignment: the number of pre_meta items should match the pre-accession
    # meta columns. Allow off-by-one for leaked reference lines.
    n_pre = len(pre_meta)
    n_cols = len(pre_col_fields)
    
    if n_pre == n_cols:
        offset = 0
    elif n_pre == n_cols + 1:
        # An extra line leaked in (likely a reference from previous row)
        offset = 1
    elif n_pre == n_cols - 1:
        # Missing one line (e.g., no species when forward-filled)
        offset = 0
    else:
        # Large gap: try matching pre_meta against only non-None fields
        _active = [(j, f) for j, f in enumerate(pre_col_fields) if f is not None]
        if len(_active) == n_pre:
            for meta_idx, (_, field) in enumerate(_active):
                _, line_text = pre_meta[meta_idx]
                if field == 'voucher' and not result.voucher:
                    if (line_text.strip()
                            and not (is_species_name(line_text) and _is_valid_species_candidate(line_text))
                            and not resolve_country(line_text)):
                        result.voucher = line_text.strip()
                elif field == 'country' and not result.country:
                    resolved = resolve_country(line_text)
                    if resolved:
                        result.country = resolved
        return
    
    for i, field in enumerate(pre_col_fields):
        if field is None:
            continue
        meta_idx = i + offset
        if meta_idx >= n_pre:
            continue
        
        _, line_text = pre_meta[meta_idx]
        
        if field == 'voucher' and not result.voucher:
            # Accept any non-empty, non-noise text as voucher at this position.
            # After leading-reference trimming (step 0c), positional alignment
            # is reliable — trust the column position, only reject species-like strings.
            # Also reject country strings to prevent misalignment when a field is merged.
            if (line_text.strip()
                    and not (is_species_name(line_text) and _is_valid_species_candidate(line_text))
                    and not resolve_country(line_text)):
                result.voucher = line_text.strip()
        elif field == 'country' and not result.country:
            resolved = resolve_country(line_text)
            if resolved:
                result.country = resolved
        # species is handled by the main loop + forward-fill, don't override


def _positional_fill_flat(
    row: dict,
    pre_meta: list[str],
    header: 'TableHeader',
) -> None:
    """Same as _positional_fill but for extract_all_rows_from_pdf (flat string lists)."""
    pre_col_fields = _build_pre_col_fields(header)
    
    if not pre_col_fields:
        return
    
    n_pre = len(pre_meta)
    n_cols = len(pre_col_fields)
    
    if n_pre == n_cols:
        offset = 0
    elif n_pre == n_cols + 1:
        offset = 1
    elif n_pre == n_cols - 1:
        offset = 0
    else:
        # Large gap: try matching pre_meta against only non-None fields
        _active = [(j, f) for j, f in enumerate(pre_col_fields) if f is not None]
        if len(_active) == n_pre:
            for meta_idx, (_, field) in enumerate(_active):
                line_text = pre_meta[meta_idx]
                if field == 'voucher' and 'voucher' not in row:
                    if (line_text.strip()
                            and not (is_species_name(line_text) and _is_valid_species_candidate(line_text))
                            and not resolve_country(line_text)):
                        row['voucher'] = line_text.strip()
                elif field == 'country' and 'country' not in row:
                    resolved = resolve_country(line_text)
                    if resolved:
                        row['country'] = resolved
        return
    
    for i, field in enumerate(pre_col_fields):
        if field is None:
            continue
        meta_idx = i + offset
        if meta_idx >= n_pre:
            continue
        
        line_text = pre_meta[meta_idx]
        
        if field == 'voucher' and 'voucher' not in row:
            if (line_text.strip()
                    and not (is_species_name(line_text) and _is_valid_species_candidate(line_text))
                    and not resolve_country(line_text)):
                row['voucher'] = line_text.strip()
        elif field == 'country' and 'country' not in row:
            resolved = resolve_country(line_text)
            if resolved:
                row['country'] = resolved


def _map_accessions_to_genes(
    result: AccessionLookupResult, 
    acc_lines: list[tuple[int, str]], 
    header: TableHeader,
    target: str
):
    """
    Map accession block lines to gene column names.
    
    Uses the header's gene column order to assign each accession to the right gene.
    Also determines which gene the target accession belongs to.
    """
    gene_cols = header.gene_cols
    
    # Basic mapping: position in block → gene column
    for i, (line_idx, acc_val) in enumerate(acc_lines):
        if i < len(gene_cols):
            gene_name = gene_cols[i].strip()
            
            # Is this the target?
            if acc_val.strip() == target:
                result.gene_region = gene_name
            
            # Store in other_accessions
            if not DASH_RE.match(acc_val.strip()):
                result.other_accessions[gene_name] = acc_val.strip()


def _find_forward_fill_species(
    lines: list[str], 
    current_row_start: int,
    header: TableHeader
) -> Optional[str]:
    """
    For forward-fill tables, find the species name by looking at previous rows.
    
    Uses row-level backward traversal:
    1. Walk backwards from current position
    2. When we encounter an accession block (previous row's tail), we've found a row boundary
    3. Look at the metadata lines before that accession block for a species
    4. If no species found, keep going to the row before that
    
    Substrate-aware: when the header defines a 'Substrate' column, uses positional 
    mapping to correctly identify species vs substrate/host-plant names.
    """
    n_acc = header.n_gene_cols
    j = current_row_start - 1
    max_lookback = 300
    min_line = max(0, current_row_start - max_lookback)
    
    # Pre-compute substrate awareness from header
    _has_substrate = False
    _pre_col_fields = []
    if header.meta_cols:
        _pre_col_fields = _build_pre_col_fields(header)
        _has_substrate = 'substrate' in _pre_col_fields
    
    # Track which lines are metadata vs accession as we walk back
    # When we find an accession block of size n_acc, the lines before it
    # (up to the next accession block) belong to one row's metadata
    
    while j >= min_line:
        line = lines[j].strip()
        
        # Skip noise
        if is_page_noise(line) or TABLE_CONTINUED_RE.search(line) or TABLE_TITLE_RE.search(line):
            j -= 1
            continue
        
        # Skip super-headers from continued tables
        if is_super_header(line):
            j -= 1
            continue
        
        # Skip gene column headers (from continuation headers)
        if is_gene_header(line.lower()):
            j -= 1
            continue
        
        # Skip metadata column headers  
        meta_kw = ['species', 'genus', 'voucher', 'specimen', 'locality', 'origin',
                   'collection', 'substrate', 'sample', 'order', 'family', 'reference',
                   'name', 'herbarium']
        if any(kw in line.lower() for kw in meta_kw) and len(line.split()) <= 6 and not is_species_name(line):
            j -= 1
            continue
        
        # If we're at an accession/dash line, this is part of a previous row's accession block
        if is_accession_or_dash(line):
            # Skip the entire accession block
            while j >= min_line and is_accession_or_dash(lines[j].strip()):
                j -= 1
            
            # Now we're at the metadata lines before this accession block
            # Collect them, then use positional mapping if substrate-aware
            meta_scan_limit = header.n_meta_cols + 2
            collected_meta = []  # (line_idx, text)
            scan_j = j
            
            while scan_j >= min_line and len(collected_meta) < meta_scan_limit:
                m_line = lines[scan_j].strip()
                
                if is_accession_or_dash(m_line):
                    break
                if is_page_noise(m_line) or TABLE_CONTINUED_RE.search(m_line) or TABLE_TITLE_RE.search(m_line):
                    scan_j -= 1
                    continue
                if is_super_header(m_line) or is_gene_header(m_line.lower()):
                    scan_j -= 1
                    continue
                if any(kw in m_line.lower() for kw in meta_kw) and len(m_line.split()) <= 6 and not is_species_name(m_line):
                    scan_j -= 1
                    continue
                if is_reference(m_line) and not (is_species_name(m_line) and _is_valid_species_candidate(m_line)):
                    scan_j -= 1
                    continue
                
                collected_meta.insert(0, (scan_j, m_line))
                scan_j -= 1
            
            # Try positional mapping to find species (substrate-aware)
            if _has_substrate and _pre_col_fields and collected_meta:
                n_meta = len(collected_meta)
                n_cols = len(_pre_col_fields)
                
                # Determine offset
                offset = 0
                if n_meta == n_cols:
                    offset = 0
                elif n_meta == n_cols + 1:
                    offset = 1
                elif n_meta == n_cols - 1:
                    offset = -1 if _pre_col_fields and _pre_col_fields[0] == 'species' else 0
                
                # Find species by position
                for fi, field in enumerate(_pre_col_fields):
                    if field != 'species':
                        continue
                    mi = fi + offset
                    if 0 <= mi < n_meta:
                        _, candidate = collected_meta[mi]
                        if is_species_name(candidate) and _is_valid_species_candidate(candidate):
                            j = scan_j
                            return candidate
                
                # Positional didn't find species in this row, continue to previous
                j = scan_j
                continue
            
            # Non-substrate-aware fallback: scan collected metadata for species
            for _, m_line in collected_meta:
                if is_species_name(m_line) and _is_valid_species_candidate(m_line):
                    return m_line
                
                if is_genus_only(m_line) and _is_valid_species_candidate(m_line):
                    # Check next lines for species
                    for cm_idx, cm_line in collected_meta:
                        if cm_idx > _ and is_species_name(cm_line) and _is_valid_species_candidate(cm_line):
                            return cm_line
            
            j = scan_j
            continue
        
        # Non-accession, non-noise line
        # Check species BEFORE reference: species with authorship (e.g. "F. apiahyna
        # (Speg.) Robledo et al.") must not be mistakenly skipped as a reference.
        if is_species_name(line) and _is_valid_species_candidate(line):
            # Don't match header column names as species
            # (e.g. "Geographic origin" matches SPECIES_RE)
            if header and header.meta_cols:
                _hdr_names = {mc.strip().lower() for mc in header.meta_cols if len(mc.strip()) <= 40}
                if line.lower() in _hdr_names:
                    j -= 1
                    continue
            return line
        
        # Skip references (e.g., "Justo et al. 2021")
        if is_reference(line):
            j -= 1
            continue
        
        # Check for genus-only (skip known countries like "Italy")
        if is_genus_only(line) and _is_valid_species_candidate(line) and not resolve_country(line):
            for k in range(j + 1, min(j + 3, len(lines))):
                nxt = lines[k].strip()
                if is_species_name(nxt) and _is_valid_species_candidate(nxt):
                    return nxt
        
        j -= 1
    
    return None


# Common English words/phrases that match SPECIES_RE but aren't species names
_NON_SPECIES_WORDS = {
    'herbarium', 'voucher', 'specimen', 'collection', 'reference', 'substrate',
    'locality', 'origin', 'country', 'accession', 'genbank', 'table', 'figure',
    'species', 'genus', 'family', 'order', 'class', 'kingdom', 'phylum',
    'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten',
    'hundred', 'thousand', 'million',
    'the', 'this', 'that', 'these', 'those', 'some', 'all', 'any', 'each', 'every',
    'new', 'novel', 'first', 'second', 'third', 'last', 'next', 'previous',
    'also', 'been', 'have', 'more', 'most', 'only', 'other', 'same', 'such',
    'based', 'using', 'after', 'before', 'between', 'during', 'since', 'until',
    'however', 'nevertheless', 'furthermore', 'moreover', 'therefore', 'although',
    'from', 'into', 'with', 'about', 'against', 'along', 'among', 'around',
    'molecular', 'phylogenetic', 'morphological', 'preliminary', 'previously',
    'analyses', 'analysis', 'results', 'discussion', 'materials', 'methods',
    'several', 'various', 'different', 'distinct', 'similar', 'related',
    'direct', 'indirect', 'submitted', 'submission', 'unpublished',
    'sample', 'samples', 'including', 'respectively', 'approximately',
    'additional', 'altogether', 'comparison', 'following', 'subsequent',
    # Country-related words that can be misidentified as genus names
    'republic', 'democratic', 'kingdom', 'united', 'states', 'emirates',
    'dominican', 'trinidad', 'tobago', 'sierra', 'leone', 'sri', 'lanka',
    'papua', 'equatorial', 'solomon', 'islands', 'burkina', 'faso',
    'saudi', 'arabia', 'costa', 'rica', 'salvador', 'herzegovina', 'macedonia',
    # Common substrate/host prefixes (these start lines like "Living twig of ...",
    # "Unidentified liana", "Terricolous")
    'living', 'unidentified', 'terricolous', 'terrestrial', 'dead',
    'decayed', 'decaying', 'rotting', 'fallen', 'standing',
    # Methodology words that can leak from text near tables
    'sequencing', 'amplification', 'extraction', 'reactions', 'alignment',
    'protocol', 'primers', 'conducted', 'performed', 'obtained',
    # Reference/citation words
    'present',  # "Present study" is a reference, not a species
}


def _is_valid_species_candidate(s: str) -> bool:
    """
    Validate that a string that matches SPECIES_RE is actually a plausible species name,
    not a common English phrase.
    """
    s_stripped = s.strip()
    words = s_stripped.lower().split()
    
    if not words:
        return False
    
    # Check if the first word (potential genus) is a common English word
    first_word = words[0].rstrip('.')
    if first_word in _NON_SPECIES_WORDS:
        return False
    
    # Check if second word is a common non-epithet word
    # Reject country names as genus ("Italy", "China" match GENUS_ONLY_RE)
    if resolve_country(s_stripped):
        return False
    
    if len(words) >= 2:
        second_word = words[1].rstrip('.,;:')
        # Species epithets are typically Latin, so common English prepositions/articles are suspicious
        english_non_epithets = {
            'the', 'and', 'for', 'are', 'was', 'were', 'has', 'had', 'have',
            'been', 'not', 'but', 'with', 'from', 'that', 'this', 'will',
            'can', 'may', 'should', 'would', 'could', 'its', 'our', 'your',
            'hundred', 'thousand', 'used', 'shown', 'data', 'voucher',
            'specimens', 'collections', 'references', 'numbers',
            'et',  # Latin "and" in author citations: "Justo et al."
        }
        if second_word in english_non_epithets:
            return False
    
    # Reject "Name Year" pattern (e.g., "Vellinga 2004", "Liang 2010")
    # These are bibliographic references, not species names.
    if len(words) == 2 and re.fullmatch(r'(?:19|20)\d{2}', words[1].rstrip('.,;:')):
        return False
    
    # Reject taxonomic rank names (family: -aceae, order: -ales), except
    # explicit placeholder rows like "Amylocorticiales sp.".
    if first_word.endswith('aceae') or first_word.endswith('ales'):
        second = words[1].rstrip('.,;:') if len(words) >= 2 else ''
        if not second.startswith('sp'):
            return False
    
    return True


def _clean_species_name(species: str) -> str:
    """
    Clean up species name by removing author citations, type markers, etc.
    
    "F. aethiopica Decock et al." -> "F. aethiopica"
    "F. apiahyna (Speg.) Robledo et al." -> "F. apiahyna"
    "C. cf. stuckertiana (Speg.) Rajchenb." -> "C. cf. stuckertiana"
    "P. sp. I (sect. Hispidoderma)" -> "P. sp. I (sect. Hispidoderma)"
    """
    s = species.strip()
    # Remove paired quote marks frequently emitted by PDF extraction.
    s = re.sub(r'^["\'\u2018\u2019\u201b\u201c\u201d`]+', '', s)
    s = re.sub(r'["\'\u2018\u2019\u201b\u201c\u201d`]+$', '', s)

    # Special case: "G. sp." entries — keep Roman numerals and section qualifiers
    # e.g. "P. sp. I (sect. Hispidoderma)", "Pluteus sp. II (sect. Celluloderma)"
    m_sp = re.match(
        r'^([A-Z][a-z]*\.?\s+sp\.(?:\s|$)\s*(?:[IVXLC]+\s*)?(?:\(sect\.\s*[A-Za-z]+\))?)',
        s
    )
    if m_sp:
        return m_sp.group(1).strip()

    # Keep the binomial part, optionally with cf./aff. and infraspecific epithet
    # Remove everything from the author part onwards
    m = re.match(
        r'^([A-Z][a-z]*\.?\s+(?:(?:cf|aff)\.\s+)?[a-z]+(?:\s+(?:subsp|var|f)\.\s+[a-z]+)?)',
        s
    )
    if m:
        return m.group(1).strip()
    
    return s


def _heuristic_lookup(
    lines: list[str], 
    target_idx: int, 
    target: str
) -> Optional[AccessionLookupResult]:
    """
    Fallback heuristic when no table header is found.
    
    Simply look at nearby lines and classify them.
    """
    result = AccessionLookupResult(accession=target, confidence="low", method="pymupdf_heuristic")
    
    # Expand accession block
    acc_start, acc_end = _expand_accession_block(lines, target_idx)
    
    # Look backwards for species, voucher, country
    for j in range(acc_start - 1, max(0, acc_start - 10), -1):
        line = lines[j].strip()
        
        if is_species_name(line) and _is_valid_species_candidate(line) and not result.species:
            result.species = _clean_species_name(line)
        else:
            resolved = resolve_country(line)
            if resolved and not result.country:
                result.country = resolved
            elif is_voucher(line) and not result.voucher:
                result.voucher = line
    
    # Also try merging consecutive backward lines for multi-word countries
    if not result.country:
        for j in range(acc_start - 1, max(0, acc_start - 9), -1):
            combined = lines[j].strip() + " " + lines[j + 1].strip() if j + 1 < acc_start else ""
            if combined:
                resolved = resolve_country(combined)
                if resolved:
                    result.country = resolved
                    break
    
    # Look forward for country
    for j in range(acc_end, min(len(lines), acc_end + 5)):
        line = lines[j].strip()
        resolved = resolve_country(line)
        if resolved and not result.country:
            result.country = resolved
    
    return result


# ============================================================================
# Full table reconstruction (optional, for batch extraction)
# ============================================================================

def _select_best_header(headers: list) -> 'TableHeader | None':
    """
    Select the best (most reliable) GenBank table header from candidates.
    
    Filters out false positive headers where gene_cols contain long text
    (body text incorrectly parsed as headers) and prefers headers with
    the most gene columns.
    """
    valid = []
    for h in headers:
        # Filter: real gene column names are short (≤ 30 chars)
        max_gene_len = max((len(g.strip()) for g in h.gene_cols), default=0)
        if max_gene_len > 30:
            continue
        
        # Filter: must have at least 1 gene col that matches known gene keywords
        has_known_gene = any(
            any(kw in g.lower() for kw in ['its', 'lsu', 'ssu', 'tef', 'rpb', 'btub', '28s', '18s', 'accession'])
            for g in h.gene_cols
        )
        if not has_known_gene:
            continue
        
        valid.append(h)
    
    if not valid:
        return headers[0] if headers else None
    
    # Prefer header with most gene columns
    return max(valid, key=lambda h: h.n_gene_cols)


def extract_all_rows_from_pdf(
    pdf_path: str | Path,
) -> list[dict]:
    """
    Extract all data rows from GenBank tables in a PDF.
    
    Returns list of dicts with keys: species, voucher, country, and gene accessions.
    
    This does a full table reconstruction using the accession-anchor approach:
    1. Find all accession blocks (consecutive accession/dash lines)
    2. Group them into rows with metadata
    3. Apply forward-fill for species
    """
    pdf_path = Path(pdf_path)
    lines = extract_text_lines(pdf_path)
    headers = find_table_headers(lines)
    
    if not headers:
        logger.warning(f"No GenBank table headers found in {pdf_path.name}")
        return []
    
    # Split multi-accession lines (same preprocessing as lookup_accession_in_pdf)
    lines, headers = _split_multi_accession_lines(lines, headers)
    
    header = _select_best_header(headers)
    if not header:
        logger.warning(f"No valid GenBank table header in {pdf_path.name}")
        return []
    
    n_acc = header.n_gene_cols
    
    # Pre-compute substrate awareness for batch processing
    _batch_has_substrate = False
    if header.meta_cols:
        _batch_pcf = _build_pre_col_fields(header)
        _batch_has_substrate = 'substrate' in _batch_pcf
    
    rows = []
    current_species = ""
    current_genus = ""
    
    # Find all accession blocks in the document
    i = header.header_end
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Skip noise, headers
        if is_page_noise(line) or TABLE_TITLE_RE.search(line) or \
           TABLE_CONTINUED_RE.search(line) or is_super_header(line) or \
           is_gene_header(line.lower()):
            i += 1
            continue
        
        # Skip metadata header names (in continued tables)
        meta_kw = ['species', 'genus', 'voucher', 'specimen', 'locality', 'origin',
                   'collection', 'substrate', 'name', 'sample', 'order', 'family', 'reference']
        if any(kw in line.lower() for kw in meta_kw) and len(line.split()) <= 6 and not is_species_name(line):
            i += 1
            continue
        
        # Check if this is the start of an accession block
        if is_accession_or_dash(line):
            # Expand the block
            acc_start = i
            acc_end = i + 1
            while acc_end < len(lines) and is_accession_or_dash(lines[acc_end]):
                acc_end += 1
            
            n_found_acc = acc_end - acc_start
            
            # Only process if block size matches expected gene columns
            if n_found_acc == n_acc:
                # Collect metadata lines before this block
                pre_meta = []
                j = acc_start - 1
                while j >= 0 and len(pre_meta) < header.n_meta_cols + 1:
                    m_line = lines[j].strip()
                    if is_accession_or_dash(m_line):
                        # SP-ACC-CO heuristic: accession-like voucher?
                        # Disabled when header has substrate column to prevent
                        # substrate lines from triggering false positive.
                        if j > 0 and not _batch_has_substrate:
                            prev = lines[j - 1].strip()
                            nxt = lines[j + 1].strip() if j + 1 < len(lines) else ''
                            _prev_taxonish = (
                                (is_species_name(prev) and _is_valid_species_candidate(prev))
                                or bool(re.match(r'^[A-Z][A-Za-z\-]+\s+sp\.?$', prev))
                            )
                            if (_prev_taxonish and resolve_country(nxt)
                                    and not is_reference(prev) and not resolve_country(prev)):
                                pre_meta.insert(0, m_line)
                                j -= 1
                                continue
                        break
                    if is_page_noise(m_line):
                        # Don't skip bare numbers before country codes (voucher numbers)
                        if (re.fullmatch(r'\d{1,3}(?:/\d+)?', m_line)
                                and pre_meta and resolve_country(pre_meta[-1])):
                            pass  # Include
                        else:
                            break
                    elif TABLE_TITLE_RE.search(m_line) or TABLE_CONTINUED_RE.search(m_line) or \
                       is_super_header(m_line) or is_gene_header(m_line.lower()):
                        break
                    
                    # Skip continued table header cols
                    if any(kw in m_line.lower() for kw in meta_kw) and len(m_line.split()) <= 5:
                        break
                    
                    pre_meta.insert(0, m_line)
                    j -= 1
                
                # Collect post-acc metadata (always, to catch country/references)
                post_meta = []
                k = acc_end
                while k < len(lines) and len(post_meta) < 3:
                    p_line = lines[k].strip()
                    if is_accession_or_dash(p_line) or is_species_name(p_line) or \
                       is_page_noise(p_line) or TABLE_TITLE_RE.search(p_line):
                        break
                    post_meta.append(p_line)
                    k += 1
                
                # Merge multi-line countries in pre_meta and post_meta
                pre_meta = _merge_multiline_countries_flat(pre_meta)
                post_meta = _merge_multiline_countries_flat(post_meta)
                
                # Merge multi-line vouchers in pre_meta
                pre_meta = _merge_multiline_vouchers_flat(pre_meta)
                
                # Build row
                row = {'_raw_pre': pre_meta, '_raw_acc': [lines[x].strip() for x in range(acc_start, acc_end)]}
                
                # Map accessions to genes
                for gi, gene in enumerate(header.gene_cols):
                    if gi < n_found_acc:
                        val = lines[acc_start + gi].strip()
                        if not DASH_RE.match(val):
                            row[gene.strip()] = val
                
                # Extract metadata from pre_meta
                # Strategy: positional mapping first (when header defines columns),
                # heuristic fallback otherwise.
                species_in_row = False
                positional_done = False
                
                if header.meta_cols:
                    pre_col_fields = _build_pre_col_fields(header)
                    n_pre = len(pre_meta)
                    n_cols = len(pre_col_fields)
                    
                    if n_pre == n_cols and n_cols > 0:
                        positional_done = True
                        for fi, field in enumerate(pre_col_fields):
                            m_line = pre_meta[fi]
                            if field == 'species':
                                if is_species_name(m_line) and _is_valid_species_candidate(m_line):
                                    cleaned = _clean_species_name(m_line)
                                    row['species'] = cleaned
                                    current_species = cleaned
                                    species_in_row = True
                                elif is_genus_only(m_line) and _is_valid_species_candidate(m_line):
                                    current_genus = m_line
                            elif field == 'country':
                                resolved = resolve_country(m_line)
                                if resolved:
                                    row['country'] = resolved
                            elif field == 'voucher':
                                if (m_line.strip()
                                        and not is_reference(m_line)
                                        and not _is_citation_marker(m_line)):
                                    row['voucher'] = m_line.strip()
                            elif field == 'substrate':
                                pass  # Skip substrate (host plant)
                    
                    elif n_pre == n_cols + 1 and n_cols > 0:
                        positional_done = True
                        offset = 1
                        for fi, field in enumerate(pre_col_fields):
                            mi = fi + offset
                            if mi >= n_pre:
                                continue
                            m_line = pre_meta[mi]
                            if field == 'species':
                                if is_species_name(m_line) and _is_valid_species_candidate(m_line):
                                    cleaned = _clean_species_name(m_line)
                                    row['species'] = cleaned
                                    current_species = cleaned
                                    species_in_row = True
                                elif is_genus_only(m_line) and _is_valid_species_candidate(m_line):
                                    current_genus = m_line
                            elif field == 'country':
                                resolved = resolve_country(m_line)
                                if resolved:
                                    row['country'] = resolved
                            elif field == 'voucher':
                                if (m_line.strip()
                                        and not is_reference(m_line)
                                        and not _is_citation_marker(m_line)):
                                    row['voucher'] = m_line.strip()
                            elif field == 'substrate':
                                pass
                    
                    elif n_pre == n_cols - 1 and n_cols > 0:
                        positional_done = True
                        if pre_col_fields and pre_col_fields[0] == 'species':
                            for fi in range(1, n_cols):
                                mi = fi - 1
                                if mi >= n_pre:
                                    continue
                                field = pre_col_fields[fi]
                                m_line = pre_meta[mi]
                                if field == 'country':
                                    resolved = resolve_country(m_line)
                                    if resolved:
                                        row['country'] = resolved
                                elif field == 'voucher':
                                    if (m_line.strip()
                                            and not is_reference(m_line)
                                            and not _is_citation_marker(m_line)):
                                        row['voucher'] = m_line.strip()
                                elif field == 'substrate':
                                    pass
                        else:
                            positional_done = False
                
                # Heuristic fallback
                if not positional_done:
                    for m_line in pre_meta:
                        resolved = resolve_country(m_line)
                        if is_reference(m_line):
                            pass
                        elif is_species_name(m_line) and _is_valid_species_candidate(m_line):
                            if not species_in_row:
                                cleaned = _clean_species_name(m_line)
                                row['species'] = cleaned
                                current_species = cleaned
                                species_in_row = True
                        elif is_genus_only(m_line) and _is_valid_species_candidate(m_line):
                            current_genus = m_line
                        elif resolved:
                            if 'country' not in row:
                                row['country'] = resolved
                        elif is_voucher(m_line):
                            if 'voucher' not in row:
                                row['voucher'] = m_line
                        elif _is_citation_marker(m_line):
                            pass
                        else:
                            if 'other_meta' not in row:
                                row['other_meta'] = []
                            row['other_meta'] = row.get('other_meta', []) + [m_line]
                
                # Forward fill species
                if not species_in_row and current_species:
                    row['species'] = current_species
                    row['_forward_filled'] = True
                
                # Extract from post_meta
                for p_line in post_meta:
                    resolved = resolve_country(p_line)
                    if resolved and 'country' not in row:
                        row['country'] = resolved
                    elif is_reference(p_line):
                        row['reference'] = p_line
                
                # Positional fallback for unrecognized voucher codes
                # Only needed when heuristic path was used
                if not positional_done and header.meta_cols:
                    _positional_fill_flat(row, pre_meta, header)
                
                # Strip PDF superscript footnote markers from vouchers
                if row.get('voucher'):
                    row['voucher'] = re.sub(
                        r'(?<=[\d)])\s*[a-e](?:\s*,\s*[a-e])*(?=\s+\(|\s*$)',
                        '', row['voucher'],
                    )
                
                rows.append(row)
            
            i = acc_end
        else:
            i += 1
    
    return rows


# ============================================================================
# Convenience functions
# ============================================================================

def find_accession_info(
    pdf_path: str | Path,
    accession_code: str,
    verbose: bool = False
) -> dict:
    """
    High-level convenience function: find species/voucher/country for an accession code.
    
    Args:
        pdf_path: Path to the PDF
        accession_code: GenBank accession code (e.g., 'KC136220')
        verbose: If True, print detailed info
    
    Returns:
        Dict with keys: accession, species, voucher, country, gene_region, confidence
    """
    result = lookup_accession_in_pdf(pdf_path, accession_code)
    
    if result is None:
        return {
            'accession': accession_code,
            'species': '',
            'voucher': '',
            'country': '',
            'gene_region': '',
            'confidence': 'not_found',
        }
    
    info = {
        'accession': result.accession,
        'species': result.species,
        'voucher': result.voucher,
        'country': result.country,
        'gene_region': result.gene_region,
        'confidence': result.confidence,
        'other_accessions': result.other_accessions,
        'method': result.method,
    }
    
    if verbose:
        print(f"  Accession: {info['accession']}")
        print(f"  Species:   {info['species']}")
        print(f"  Voucher:   {info['voucher']}")
        print(f"  Country:   {info['country']}")
        print(f"  Gene:      {info['gene_region']}")
        print(f"  Confidence: {info['confidence']}")
        if result.other_accessions:
            print(f"  Other acc: {result.other_accessions}")
        print(f"  Raw row:   {result.raw_row_lines}")
    
    return info


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    import sys
    
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    if len(sys.argv) < 3:
        print("Usage: python phase4_pdf_extraction_v2.py <pdf_path> <accession_code>")
        print("Example: python phase4_pdf_extraction_v2.py paper.pdf JQ087932")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    accession = sys.argv[2]
    
    info = find_accession_info(pdf_path, accession, verbose=True)

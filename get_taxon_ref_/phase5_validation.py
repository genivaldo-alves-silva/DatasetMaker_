"""
Fase 5: Validação de Dados Extraídos

Valida dados obtidos das fases 2-4 antes de aplicar no DataFrame final.

Regras desta implementação:
- Country ambíguo: não preencher campo principal, salvar candidatos em auditoria.
- Voucher em conflito com valor existente: não sobrescrever coluna `voucher`.
  Quando possível, adiciona o voucher novo ao cluster correspondente no `voucher_dict`.
- Species: atualizar apenas quando species atual estiver incompleto.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import pycountry

from .phase0_detection import is_species_incomplete, has_voucher_in_species

logger = logging.getLogger(__name__)


VOUCHER_PATTERNS = [
    re.compile(r"^[A-Z]{2,6}[-:\s]?\s*[A-Za-z]*\d+[A-Za-z0-9.,/()\-]*$"),
    re.compile(r"^\d+[A-Za-z0-9.,/()\-]*$"),
    re.compile(r"^[A-Za-z][A-Za-z.\-]+\s+\d+[A-Za-z0-9.,/()\-]*$"),
    re.compile(r"^[A-Z]{1,3}\s*\d{4,}[A-Za-z0-9.,/()\-]*$"),
    re.compile(r"^[A-Z]{2,}\s*,\s*[A-Za-z][A-Za-z.\-\s]*\d*[A-Za-z0-9.,/()\-]*$"),
]

CITATION_MARKER_RE = re.compile(r"^\[\d+(?:[-,]\d+)*\]$")
GENBANK_ACCESSION_RE = re.compile(r"^[A-Z]{1,2}\d{5,8}(?:\.\d+)?$", re.IGNORECASE)


@dataclass
class FieldDecision:
    status: str = "rejected"  # accepted, rejected, deferred
    candidate: Optional[str] = None
    value: Optional[str] = None
    reason: str = ""


@dataclass
class ValidationResult:
    gb_accession: str
    source: Optional[str]
    voucher: FieldDecision = field(default_factory=FieldDecision)
    country: FieldDecision = field(default_factory=FieldDecision)
    species: FieldDecision = field(default_factory=FieldDecision)
    country_candidates: list[str] = field(default_factory=list)
    voucher_conflict: bool = False
    voucher_conflict_existing: Optional[str] = None
    voucher_conflict_new: Optional[str] = None
    voucher_dict_updated: bool = False
    notes: list[str] = field(default_factory=list)

    def has_validated_fields(self) -> bool:
        return any(
            field.status == "accepted" and field.value
            for field in (self.voucher, self.country, self.species)
        )

    def has_audit_data(self) -> bool:
        return bool(
            self.country_candidates
            or self.voucher_conflict
            or self.notes
            or self.voucher_dict_updated
        )


class Phase5Validator:
    """Validador da fase 5 para dados enriquecidos."""

    def __init__(self):
        self._country_ready = False
        self._fungal_check_cache: dict[str, bool] = {}
        self._if_check_available: Optional[bool] = None

    def validate(
        self,
        enrichment_result: Any,
        row: pd.Series,
        voucher_dict: Optional[dict] = None,
        update_voucher_dict: bool = True,
    ) -> ValidationResult:
        """
        Valida um EnrichmentResult contra a linha original do DataFrame.

        Args:
            enrichment_result: Objeto com atributos found_voucher/found_country/found_species.
            row: Linha original do DataFrame.
            voucher_dict: Dicionário de vouchers para vincular conflitos.
            update_voucher_dict: Se True, tenta adicionar voucher conflitante ao cluster.

        Returns:
            ValidationResult com decisões por campo.
        """
        result = ValidationResult(
            gb_accession=getattr(enrichment_result, "gb_accession", ""),
            source=getattr(enrichment_result, "source", None),
        )

        existing_voucher = _clean_text(row.get("voucher"))
        existing_country = _clean_text(row.get("geo_loc_name") or row.get("country"))
        existing_species = _clean_text(row.get("Species") or row.get("species"))

        found_voucher = _clean_text(getattr(enrichment_result, "found_voucher", None))
        found_country = _clean_text(getattr(enrichment_result, "found_country", None))
        found_species = _clean_text(getattr(enrichment_result, "found_species", None))

        result.voucher = self._validate_voucher(
            candidate=found_voucher,
            existing=existing_voucher,
            gb_accession=result.gb_accession,
        )

        if result.voucher.status == "deferred" and result.voucher.reason == "conflict_with_existing":
            result.voucher_conflict = True
            result.voucher_conflict_existing = existing_voucher
            result.voucher_conflict_new = found_voucher
            result.notes.append("voucher_conflict")
            if update_voucher_dict and voucher_dict is not None and existing_voucher and found_voucher:
                result.voucher_dict_updated = _attach_voucher_to_cluster(
                    voucher_dict=voucher_dict,
                    existing_voucher=existing_voucher,
                    new_voucher=found_voucher,
                )

        result.country, result.country_candidates = self._validate_country(
            candidate=found_country,
            existing=existing_country,
        )
        if result.country_candidates:
            result.notes.append("country_ambiguous")

        result.species = self._validate_species(
            candidate=found_species,
            existing=existing_species,
        )

        return result

    def _validate_voucher(self, candidate: Optional[str], existing: Optional[str], gb_accession: str) -> FieldDecision:
        decision = FieldDecision(candidate=candidate)

        if not candidate:
            decision.reason = "empty_candidate"
            return decision

        if CITATION_MARKER_RE.match(candidate):
            decision.reason = "citation_marker"
            return decision

        if candidate.lower() in {"none", "nan", "na", "n/a"}:
            decision.reason = "placeholder"
            return decision

        if gb_accession and _normalize_token(candidate) == _normalize_token(gb_accession):
            decision.reason = "same_as_accession"
            return decision

        if GENBANK_ACCESSION_RE.match(candidate) and not _looks_like_specimen_code(candidate):
            decision.reason = "looks_like_accession"
            return decision

        if not any(p.search(candidate) for p in VOUCHER_PATTERNS):
            decision.reason = "regex_not_matched"
            return decision

        if not existing:
            decision.status = "accepted"
            decision.value = candidate
            decision.reason = "valid_and_missing"
            return decision

        if _normalize_token(existing) == _normalize_token(candidate):
            decision.reason = "already_present"
            return decision

        decision.status = "deferred"
        decision.reason = "conflict_with_existing"
        return decision

    def _validate_country(self, candidate: Optional[str], existing: Optional[str]) -> tuple[FieldDecision, list[str]]:
        decision = FieldDecision(candidate=candidate)
        candidates: list[str] = []

        if not candidate:
            decision.reason = "empty_candidate"
            return decision, candidates

        if existing:
            decision.status = "deferred"
            decision.reason = "already_has_country"
            return decision, candidates

        self._ensure_country_detector_ready()

        from TaxonQualifier.country_detector import detectar_pais, revisar_nome_detectado  # lazy import

        detected = detectar_pais(candidate)

        if isinstance(detected, list):
            candidates = sorted({c.strip() for c in detected if isinstance(c, str) and c.strip()})
            decision.status = "deferred"
            decision.reason = "ambiguous_country"
            return decision, candidates

        if isinstance(detected, str) and detected.strip():
            canonical = _canonical_country_name(detected.strip()) or revisar_nome_detectado(detected.strip())
            if canonical:
                decision.status = "accepted"
                decision.value = canonical
                decision.reason = "validated"
                return decision, candidates

        # fallback: tenta validar o texto cru via pycountry
        canonical_candidate = _canonical_country_name(candidate)
        if canonical_candidate:
            decision.status = "accepted"
            decision.value = canonical_candidate
            decision.reason = "validated_fallback"
            return decision, candidates

        decision.reason = "country_not_validated"
        return decision, candidates

    def _validate_species(self, candidate: Optional[str], existing: Optional[str]) -> FieldDecision:
        decision = FieldDecision(candidate=candidate)

        if not candidate:
            decision.reason = "empty_candidate"
            return decision

        cleaned = _clean_species(candidate)
        if not cleaned:
            decision.reason = "empty_after_clean"
            return decision

        # Regra importante: primeiro remove voucher embutido em "Genus sp. VOUCHER"
        # para evitar validar táxon com ruído da própria etiqueta de voucher.
        cleaned = _strip_embedded_voucher_from_species(cleaned)

        if existing and not is_species_incomplete(existing):
            decision.status = "deferred"
            decision.reason = "existing_species_complete"
            return decision

        if is_species_incomplete(cleaned):
            decision.reason = "candidate_incomplete"
            return decision

        if existing and _normalize_spaces(existing).lower() == _normalize_spaces(cleaned).lower():
            decision.reason = "same_as_existing"
            return decision

        fungal_ok = self._is_fungal_taxon(cleaned)
        if fungal_ok is False:
            decision.reason = "non_fungal_taxon"
            return decision
        if fungal_ok is None:
            # Fail-open: se API indisponível, mantém comportamento anterior
            # para não bloquear o pipeline inteiro.
            decision.reason = "if_check_unavailable"

        decision.status = "accepted"
        decision.value = cleaned
        if decision.reason == "if_check_unavailable":
            decision.reason = "valid_for_update_if_unavailable"
        else:
            decision.reason = "valid_for_update"
        return decision

    def _is_fungal_taxon(self, species_name: str) -> Optional[bool]:
        """
        Verifica se o táxon parece fúngico via IndexFungorum SOAP.

        Returns:
            True: táxon reconhecido como fungo
            False: não reconhecido
            None: checagem indisponível (erro de rede/API)
        """
        query = _species_to_if_query(species_name)
        if not query:
            return None

        key = query.lower()
        if key in self._fungal_check_cache:
            return self._fungal_check_cache[key]

        # Se já detectamos indisponibilidade da API nesta execução, evita retries caros.
        if self._if_check_available is False:
            return None

        try:
            from TaxonQualifier.IFget_types_soap import get_species_list_from_if

            # Consulta por gênero reduz custo e é suficiente para filtrar não-fungos.
            result = get_species_list_from_if(query, max_results=30)
            is_fungal = bool(result)
            self._fungal_check_cache[key] = is_fungal
            self._if_check_available = True
            return is_fungal
        except Exception as exc:
            logger.warning("Filtro fúngico IF indisponível para '%s': %s", species_name, exc)
            self._if_check_available = False
            return None

    def _ensure_country_detector_ready(self) -> None:
        if self._country_ready:
            return

        from TaxonQualifier.country_detector import (
            carregar_json_inteligente,
            gerar_lista_paises_pycountry,
            carregar_cache_local,
        )

        module_root = Path(__file__).resolve().parent.parent
        countries_json = module_root / "TaxonQualifier" / "countries_json.json"

        try:
            carregar_json_inteligente(str(countries_json))
            gerar_lista_paises_pycountry()
            carregar_cache_local()
            self._country_ready = True
        except Exception as exc:
            logger.warning("Falha ao inicializar country_detector para Fase 5: %s", exc)


def _clean_text(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    return text


def _normalize_token(value: str) -> str:
    return re.sub(r"[\s:\-_,.;()\[\]{}]", "", value or "").lower()


def _normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _clean_species(species: str) -> str:
    cleaned = species.strip().strip("\"'`´")
    cleaned = _normalize_spaces(cleaned)
    return cleaned


def _species_to_if_query(species_name: str) -> Optional[str]:
    """Extrai termo de busca para o IF (tipicamente o gênero)."""
    if not species_name:
        return None

    cleaned = _normalize_spaces(species_name)
    if not cleaned:
        return None

    first = cleaned.split()[0].strip(".,;:()[]{}")
    # Ignora gêneros abreviados (ex.: "F.") por baixa precisão de busca.
    if len(first) <= 2 or first.endswith('.'):
        return None

    return first


def _strip_embedded_voucher_from_species(species_name: str) -> str:
    """Remove voucher embutido quando a string estiver no padrão 'Genus sp. VOUCHER'."""
    has_voucher, species_clean, _voucher = has_voucher_in_species(species_name)
    if has_voucher and species_clean:
        return species_clean
    return species_name


def _looks_like_specimen_code(value: str) -> bool:
    # Mantém códigos com letras + muitos dígitos quando parecem código de coleção
    # (ex.: VNM00075562), evitando descartar tudo como accession.
    return bool(re.match(r"^[A-Z]{2,6}\d{4,}$", value))


def _attach_voucher_to_cluster(voucher_dict: dict, existing_voucher: str, new_voucher: str) -> bool:
    """
    Anexa um voucher novo ao cluster associado ao voucher existente.

    Returns:
        True se houve atualização no dict.
    """
    if not isinstance(voucher_dict, dict):
        return False

    existing_norm = _normalize_token(existing_voucher)
    new_norm = _normalize_token(new_voucher)

    if not existing_norm or not new_norm or existing_norm == new_norm:
        return False

    # Busca cluster que já contém o voucher existente
    for key, values in voucher_dict.items():
        variants = []
        variants.append(str(key))
        if isinstance(values, list):
            variants.extend(str(v) for v in values)

        normalized = {_normalize_token(v) for v in variants if v}
        if existing_norm in normalized:
            if new_norm in normalized:
                return False
            if not isinstance(values, list):
                voucher_dict[key] = []
                values = voucher_dict[key]
            values.append(new_voucher)
            return True

    # Se não encontrou cluster e existe voucher na linha, cria cluster mínimo.
    voucher_dict.setdefault(existing_voucher, [])
    if not isinstance(voucher_dict[existing_voucher], list):
        voucher_dict[existing_voucher] = []

    existing_variants_norm = {_normalize_token(existing_voucher)} | {
        _normalize_token(str(v)) for v in voucher_dict[existing_voucher]
    }
    if new_norm not in existing_variants_norm:
        voucher_dict[existing_voucher].append(new_voucher)
        return True

    return False


def _canonical_country_name(value: str) -> Optional[str]:
    if not value:
        return None
    raw = value.strip()

    try:
        return pycountry.countries.lookup(raw).name
    except Exception:
        pass

    # tenta versão sem pontuação extra
    cleaned = re.sub(r"\s+", " ", raw.replace(";", " ").replace("/", " ")).strip()
    try:
        return pycountry.countries.lookup(cleaned).name
    except Exception:
        return None


__all__ = [
    "Phase5Validator",
    "ValidationResult",
    "FieldDecision",
    "VOUCHER_PATTERNS",
]

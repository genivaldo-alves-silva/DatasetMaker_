"""
get_taxon_ref_ - Módulo para enriquecimento de dados taxonômicos

Este módulo implementa a busca e extração de dados faltantes (voucher, country, species)
a partir de artigos científicos, complementando os dados obtidos do GenBank.

Estrutura:
- phase0_detection.py: Detecção de lacunas nos dados
- phase1_species_cleanup.py: Limpeza e extração de vouchers da coluna species
- phase2_articles_db.py: Banco de dados local de artigos processados
- phase3_doi_resolver.py: Resolução de DOI via CrossRef, NCBI ELink, Google Scholar
- phase3_pdf_downloader.py: Download de PDFs via Unpaywall, Sci-Hub, etc.
- phase3_supplementary.py: Extração de material suplementar
- phase4_pdf_extraction.py: Extração de tabelas GenBank de PDFs/markdowns (v1 - Docling)
- phase4_pdf_extraction_v2.py: Extração via PyMuPDF-first (v2 - primário)
- phase5_validation.py: Validação de voucher/country/species antes de aplicar no dataset
- phase6_gbif_fallback.py: Fallback GBIF/iDigBio para country ainda vazio
- phase7_consolidation.py: Consolidação de linhas por clusters do voucher_dict
- md_qualifier.py: Orquestrador principal
"""

__version__ = "0.2.0"
__author__ = "DatasetMaker"

from .md_qualifier import MDQualifier

# v1: Docling/Markdown-based extraction (mantido como fallback para .md existentes)
from .phase4_pdf_extraction import (
    process_markdown_file,
    process_pdf_with_docling,
    ExtractedGBRecord,
    TableExtractionResult,
    find_record_by_gb_code,
)

# v2: PyMuPDF-first extraction (primário, ~1000x mais rápido que Docling)
from .phase4_pdf_extraction_v2 import (
    find_accession_info,
    lookup_accession_in_pdf,
    extract_all_rows_from_pdf,
    AccessionLookupResult,
)
from .phase5_validation import Phase5Validator, ValidationResult
from .phase6_gbif_fallback import Phase6Report, fill_missing_countries_with_fallbacks
from .phase7_consolidation import Phase7Report, consolidate_rows_by_voucher_dict

__all__ = [
    "MDQualifier",
    # v1 exports (compatibilidade)
    "process_markdown_file",
    "process_pdf_with_docling",
    "ExtractedGBRecord",
    "TableExtractionResult",
    "find_record_by_gb_code",
    # v2 exports (primário)
    "find_accession_info",
    "lookup_accession_in_pdf",
    "extract_all_rows_from_pdf",
    "AccessionLookupResult",
    "Phase5Validator",
    "ValidationResult",
    "Phase6Report",
    "fill_missing_countries_with_fallbacks",
    "Phase7Report",
    "consolidate_rows_by_voucher_dict",
]

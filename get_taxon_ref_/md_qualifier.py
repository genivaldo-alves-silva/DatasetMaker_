"""
MDQualifier - Orquestrador Principal

Integra todas as fases do pipeline para enriquecer dados taxonômicos:
- Fase 0: Detecção de lacunas
- Fase 1: Limpeza de species
- Fase 2: Consulta banco local de artigos
- Fase 3: Busca DOI, download PDF, material suplementar
- Fase 4: Extração de tabelas GenBank de PDFs (PyMuPDF-first + fallbacks)
"""

import os
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any
from dataclasses import dataclass

import pandas as pd

from .phase0_detection import detect_gaps, LacunasReport
from .phase1_species_cleanup import cleanup_species_column, SpeciesCleanupResult
from .phase2_articles_db import ArticlesDatabase, ArticleRecord
from .phase3_doi_resolver import get_doi_for_record, DOIResult
from .phase3_pdf_downloader import download_article_pdf, DownloadResult
from .phase3_supplementary import (
    process_supplementary_materials, 
    find_record_by_gb_code,
    ExtractedRecord
)
from .phase4_pdf_extraction_v2 import (
    lookup_accession_in_pdf,
    extract_all_rows_from_pdf,
    AccessionLookupResult,
)
from .phase5_validation import Phase5Validator, ValidationResult
from .phase6_gbif_fallback import fill_missing_countries_with_fallbacks, Phase6Report
from .phase6_5_pdf_country import fill_missing_countries_from_pdf_prose, Phase6_5Report
from .phase7_consolidation import consolidate_rows_by_voucher_dict, Phase7Report

# Alias para compatibilidade
ArticlesDB = ArticlesDatabase

logger = logging.getLogger(__name__)

# Diretório base do módulo
MODULE_DIR = Path(__file__).parent


@dataclass
class EnrichmentResult:
    """Resultado do enriquecimento de um registro."""
    gb_accession: str
    original_voucher: Optional[str]
    original_country: Optional[str]
    original_species: Optional[str]
    
    # Dados encontrados
    found_voucher: Optional[str] = None
    found_country: Optional[str] = None
    found_species: Optional[str] = None
    
    # Metadados
    doi: Optional[str] = None
    doi_method: Optional[str] = None
    pdf_downloaded: bool = False
    source: Optional[str] = None  # article_table, supplementary, gbif, etc.
    

@dataclass
class QualificationReport:
    """Relatório final de qualificação."""
    total_processed: int = 0
    vouchers_filled: int = 0
    countries_filled: int = 0
    species_updated: int = 0
    species_cleaned: int = 0
    articles_downloaded: int = 0
    articles_from_cache: int = 0
    failed_downloads: int = 0
    voucher_validated: int = 0
    voucher_rejected: int = 0
    voucher_conflicts: int = 0
    voucher_dict_updates: int = 0
    country_validated: int = 0
    country_rejected: int = 0
    country_ambiguous: int = 0
    species_validated: int = 0
    species_rejected: int = 0
    phase6_countries_filled: int = 0
    phase6_gbif_hits: int = 0
    phase6_idigbio_hits: int = 0
    phase6_5_countries_filled: int = 0
    phase6_5_pdf_rows_with_matches: int = 0
    phase6_5_pdf_rows_ambiguous: int = 0
    phase6_5_no_pdf_in_db: int = 0
    phase6_5_pairs_extracted: int = 0
    phase6_5_llm_fallback_hits: int = 0
    phase7_clusters_merged: int = 0
    phase7_rows_removed: int = 0
    phase7_fields_merged: int = 0


class MDQualifier:
    """
    Classe principal para qualificação de dados taxonômicos.
    
    Orquestra todas as fases do pipeline para preencher lacunas
    de voucher, country e species nos datasets.
    """
    
    def __init__(
        self,
        db_dir: Optional[Path] = None,
        downloads_dir: Optional[Path] = None,
        logs_dir: Optional[Path] = None,
        email: Optional[str] = None,
        ncbi_api_key: Optional[str] = None,
        pop8_path: Optional[Path] = None,
        chrome_binary: Optional[Path] = None,
        chromedriver: Optional[Path] = None,
        allow_scihub: bool = True,
        allow_selenium: bool = True
    ):
        """
        Inicializa o MDQualifier.
        
        Args:
            db_dir: Diretório do banco de artigos
            downloads_dir: Diretório para PDFs
            logs_dir: Diretório para logs
            email: Email para Unpaywall
            ncbi_api_key: API key do NCBI
            pop8_path: Caminho para pop8query
            chrome_binary: Caminho para Chrome
            chromedriver: Caminho para chromedriver
            allow_scihub: Permitir uso do Sci-Hub
            allow_selenium: Permitir uso do Selenium
        """
        # Diretórios
        self.db_dir = db_dir or MODULE_DIR / "articles_db"
        self.downloads_dir = downloads_dir or MODULE_DIR / "downloads"
        self.logs_dir = logs_dir or MODULE_DIR / "logs"
        
        # Criar diretórios se não existirem
        self.db_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Configurações
        self.email = email or os.getenv("UNPAYWALL_EMAIL", "")
        self.ncbi_api_key = ncbi_api_key or os.getenv("NCBI_API_KEY", "")
        self.pop8_path = pop8_path
        self.chrome_binary = chrome_binary
        self.chromedriver = chromedriver
        self.allow_scihub = allow_scihub
        self.allow_selenium = allow_selenium
        
        # Banco de artigos
        self.articles_db = ArticlesDB(self.db_dir)
        self.phase5_validator = Phase5Validator()
        
        # Relatório
        self.report = QualificationReport()

        # Cache temporário da execução atual (evita retrabalho por DOI)
        self._doi_runtime_cache: Dict[str, Dict[str, Any]] = {}
        
    def qualify_dataframe(
        self,
        df: pd.DataFrame,
        voucher_dict: Optional[Dict] = None,
        genus: str = "Unknown"
    ) -> pd.DataFrame:
        """
        Qualifica um DataFrame completo.
        
        Args:
            df: DataFrame com dados a qualificar
            voucher_dict: Dicionário de vouchers existente
            genus: Nome do gênero (para logging)
            
        Returns:
            DataFrame enriquecido
        """
        logger.info(f"Iniciando qualificação de {len(df)} registros para {genus}")

        # Novo ciclo de qualificação: resetar cache temporário por DOI.
        self._doi_runtime_cache = {}
        
        # Fase 0: Detecção de lacunas
        logger.info("Fase 0: Detectando lacunas...")
        gaps_report = detect_gaps(df)
        logger.info(str(gaps_report))
        self._save_phase0_gap_outputs(df, gaps_report, genus)
        
        if not gaps_report.needs_processing():
            logger.info("Nenhuma lacuna detectada - nada a fazer")
            return df
        
        # Fase 1: Limpeza de species
        if gaps_report.needs_cleanup():
            logger.info("Fase 1: Limpando coluna species...")
            df, cleanup_result = cleanup_species_column(df)
            self.report.species_cleaned = cleanup_result.species_cleaned
            logger.info(f"  - Species limpos: {cleanup_result.species_cleaned}")
            logger.info(f"  - Vouchers extraídos: {cleanup_result.vouchers_extracted}")
        
        # Fase 2-5: Processar registros com lacunas
        if gaps_report.has_gaps():
            logger.info("Fases 2-5: Buscando e validando dados em artigos...")
            df = self._process_gaps(df, gaps_report, voucher_dict)

        # Fase 6: fallback de country via GBIF/iDigBio (somente países ainda vazios)
        logger.info("Fase 6: Fallback GBIF/iDigBio para countries vazios...")
        df, phase6_report = fill_missing_countries_with_fallbacks(df, voucher_dict=voucher_dict)
        self._update_phase6_report(phase6_report)

        # Fase 6.5: fallback de country via parser narrativo de PDF (pre-Phase7)
        logger.info("Fase 6.5: Fallback de country em PDF narrativo (pre-Phase7)...")
        df, phase6_5_report, phase6_5_audit = fill_missing_countries_from_pdf_prose(
            df,
            articles_db=self.articles_db,
            voucher_dict=voucher_dict,
        )
        self._update_phase6_5_report(phase6_5_report)
        self._save_phase6_5_audit(phase6_5_audit, genus)

        # Fase 7: consolidação de linhas por voucher_dict
        if voucher_dict:
            logger.info("Fase 7: Consolidação de linhas por voucher_dict...")
            df, phase7_report = consolidate_rows_by_voucher_dict(df, voucher_dict=voucher_dict)
            self._update_phase7_report(phase7_report)
        
        # Gerar relatório final
        self._log_final_report()
        
        return df

    def _save_phase0_gap_outputs(self, df: pd.DataFrame, gaps_report: LacunasReport, genus: str) -> None:
        """
        Salva snapshot das lacunas detectadas na Fase 0.

        Gera:
        - <genus>_gaps.csv: linhas com alguma lacuna + flags por tipo
        - <genus>_gaps_summary.txt: resumo numérico
        """
        try:
            gap_indices = set()
            gap_indices.update(gaps_report.voucher_empty_indices)
            gap_indices.update(gaps_report.country_empty_indices)
            gap_indices.update(gaps_report.species_incomplete_indices)
            gap_indices.update(gaps_report.species_with_voucher_indices)

            safe_genus = re.sub(r"[^A-Za-z0-9._-]+", "_", (genus or "Unknown")).strip("_") or "Unknown"
            out_dir = self.logs_dir / "phase0_gaps"
            out_dir.mkdir(parents=True, exist_ok=True)

            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = out_dir / f"{safe_genus}_gaps_{stamp}.csv"
            summary_path = out_dir / f"{safe_genus}_gaps_summary_{stamp}.txt"

            if gap_indices:
                gaps_df = df.loc[sorted(gap_indices)].copy()
            else:
                gaps_df = df.iloc[0:0].copy()

            # Flags para revisão rápida por linha.
            gaps_df["_gap_voucher_empty"] = gaps_df.index.isin(gaps_report.voucher_empty_indices)
            gaps_df["_gap_country_empty"] = gaps_df.index.isin(gaps_report.country_empty_indices)
            gaps_df["_gap_species_incomplete"] = gaps_df.index.isin(gaps_report.species_incomplete_indices)
            gaps_df["_gap_species_with_voucher"] = gaps_df.index.isin(gaps_report.species_with_voucher_indices)

            # Mantém índice original para rastreamento no parquet de origem.
            gaps_df.insert(0, "_original_index", gaps_df.index)
            gaps_df.to_csv(csv_path, index=False)

            lines = [
                f"genus: {genus}",
                f"generated_at: {datetime.now().isoformat()}",
                f"total_records: {gaps_report.total_records}",
                f"voucher_empty: {gaps_report.voucher_empty}",
                f"country_empty: {gaps_report.country_empty}",
                f"species_incomplete: {gaps_report.species_incomplete}",
                f"species_with_voucher_embedded: {gaps_report.species_with_voucher}",
                f"rows_with_any_gap: {len(gap_indices)}",
                f"gaps_csv: {csv_path}",
            ]
            summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            logger.info(f"Fase 0 salva em: {csv_path}")
            logger.info(f"Resumo Fase 0 salvo em: {summary_path}")
        except Exception as exc:
            logger.warning(f"Falha ao salvar artefatos da Fase 0: {exc}")
    
    def _process_gaps(
        self,
        df: pd.DataFrame,
        gaps_report: LacunasReport,
        voucher_dict: Optional[Dict]
    ) -> pd.DataFrame:
        """
        Processa registros com lacunas buscando em artigos.
        """
        # Identificar registros únicos que precisam de enriquecimento
        indices_to_process = set()
        indices_to_process.update(gaps_report.voucher_empty_indices)
        indices_to_process.update(gaps_report.country_empty_indices)
        indices_to_process.update(gaps_report.species_incomplete_indices)
        
        logger.info(f"Processando {len(indices_to_process)} registros com lacunas")
        
        for idx in indices_to_process:
            row = df.loc[idx]
            self.report.total_processed += 1
            
            # Obter códigos GB do registro
            gb_codes = self._get_gb_codes_from_row(row)
            if not gb_codes:
                continue
            
            # Obter título
            title = row.get('title', '') or row.get('Title', '')
            
            # Processar cada código GB
            for gb_code in gb_codes:
                result = self._enrich_from_article(gb_code, title, row)
                
                if result:
                    validation = self.phase5_validator.validate(
                        enrichment_result=result,
                        row=row,
                        voucher_dict=voucher_dict,
                        update_voucher_dict=True,
                    )
                    self._update_validation_report(validation)
                    # Atualizar DataFrame apenas com campos validados.
                    df = self._apply_enrichment(df, idx, validation)

                    # Se já validou algo útil, não precisa testar outros GB da mesma linha.
                    if validation.has_validated_fields():
                        break
        
        return df
    
    def _get_gb_codes_from_row(self, row: pd.Series) -> List[str]:
        """Extrai códigos GenBank de uma linha."""
        codes = []
        
        # Colunas típicas de genes
        gene_columns = ['ITS', 'nrLSU', 'nrSSU', 'TEF1', 'RPB1', 'RPB2', 
                       '28S', 'its', 'lsu', 'tef1', 'rpb1', 'rpb2']
        
        for col in gene_columns:
            if col in row and pd.notna(row[col]):
                value = str(row[col]).strip()
                # Verificar se parece código GB
                if len(value) >= 6 and value[0].isalpha():
                    codes.append(value)
        
        # Também verificar coluna GBn se existir
        if 'GBn' in row and pd.notna(row['GBn']):
            codes.append(str(row['GBn']).strip())
        
        return codes
    
    def _enrich_from_article(
        self,
        gb_code: str,
        title: str,
        row: pd.Series
    ) -> Optional[EnrichmentResult]:
        """
        Tenta enriquecer dados de um registro buscando no artigo.
        """
        result = EnrichmentResult(
            gb_accession=gb_code,
            original_voucher=row.get('voucher'),
            original_country=row.get('geo_loc_name') or row.get('country'),
            original_species=row.get('Species') or row.get('species')
        )
        
        # Fase 2: Verificar banco local primeiro
        cached_data = self.articles_db.get_data_for_gb(gb_code)
        if cached_data:
            logger.debug(f"Dados encontrados no cache para {gb_code}")
            self.report.articles_from_cache += 1
            result.source = "cache"
            result.found_voucher = cached_data.get('voucher')
            result.found_country = cached_data.get('country')
            result.found_species = cached_data.get('species')
            
            if result.found_voucher or result.found_country or result.found_species:
                return result
        
        # Fase 3.1: Obter DOI
        doi_result = get_doi_for_record(
            gb_accession=gb_code,
            title=title,
            pop8_path=self.pop8_path
        )
        
        if not doi_result.doi:
            logger.debug(f"DOI não encontrado para {gb_code}")
            return None
        
        result.doi = doi_result.doi
        result.doi_method = doi_result.method

        doi = doi_result.doi
        article_url = getattr(doi_result, 'article_url', None)

        # Registro temporário por DOI para evitar download/extração repetidos.
        runtime = self._doi_runtime_cache.setdefault(
            doi,
            {
                'download_attempted': False,
                'download_success': False,
                'pdf_path': None,
                'supplementary_attempted': False,
                'supplementary_records': None,
                'article_url': None,
            }
        )

        if article_url and not runtime.get('article_url'):
            runtime['article_url'] = article_url

        pdf_path = self.downloads_dir / self._build_pdf_filename_from_doi(doi)

        # Fase 3.2: Download do PDF (somente 1x por DOI por execução)
        if not runtime['download_attempted']:
            runtime['download_attempted'] = True

            # Se PDF já existe no disco, reaproveita sem baixar novamente.
            if pdf_path.exists() and pdf_path.stat().st_size > 0:
                runtime['download_success'] = True
                runtime['pdf_path'] = pdf_path
                logger.debug(f"Reutilizando PDF local para DOI {doi}: {pdf_path.name}")
            else:
                download_result = download_article_pdf(
                    doi=doi,
                    output_path=pdf_path,
                    email=self.email,
                    allow_scihub=self.allow_scihub,
                    chrome_binary=self.chrome_binary,
                    chromedriver=self.chromedriver,
                    allow_selenium=self.allow_selenium,
                    article_url=runtime.get('article_url')
                )

                if download_result.success:
                    runtime['download_success'] = True
                    runtime['pdf_path'] = download_result.pdf_path or pdf_path
                    self.report.articles_downloaded += 1
                else:
                    self.report.failed_downloads += 1
        else:
            logger.debug(f"DOI {doi} já processado nesta execução, pulando novo download")

        if runtime['download_success'] and runtime.get('pdf_path'):
            result.pdf_downloaded = True

            # Fase 4: Extrair dados do PDF com PyMuPDF (v2)
            pdf_result = self._extract_from_pdf(
                pdf_path=Path(runtime['pdf_path']),
                gb_code=gb_code,
                doi=doi
            )
            if pdf_result:
                result.source = "pdf_table"
                result.found_voucher = pdf_result.voucher or None
                result.found_country = pdf_result.country or None
                result.found_species = pdf_result.species or None

                if result.found_voucher or result.found_country or result.found_species:
                    return result
        
        # Fase 3.3: Tentar material suplementar
        if runtime.get('article_url'):
            if not runtime['supplementary_attempted']:
                runtime['supplementary_attempted'] = True
                supp_dir = self.downloads_dir / "supplementary" / self._build_pdf_filename_from_doi(doi).replace('.pdf', '')
                runtime['supplementary_records'] = process_supplementary_materials(
                    article_url=runtime['article_url'],
                    output_dir=supp_dir
                )

            supp_records = runtime.get('supplementary_records') or []
            if supp_records:
                # Buscar registro pelo código GB
                found = find_record_by_gb_code(supp_records, gb_code)
                if found:
                    result.source = "supplementary"
                    result.found_voucher = found.voucher
                    result.found_country = found.country
                    result.found_species = found.species
                    
                    # Salvar no banco local
                    self.articles_db.add_record_from_supplementary(
                        doi=doi_result.doi,
                        gb_code=gb_code,
                        record=found
                    )
        
        return result

    def _build_pdf_filename_from_doi(self, doi: str) -> str:
        """Gera nome de arquivo de PDF estável por DOI (evita duplicatas por GB)."""
        sanitized = re.sub(r'[^A-Za-z0-9._-]+', '_', doi).strip('_')
        if not sanitized:
            sanitized = 'unknown_doi'
        return f"{sanitized}.pdf"
    
    def _extract_from_pdf(
        self,
        pdf_path: Path,
        gb_code: str,
        doi: str
    ) -> Optional[AccessionLookupResult]:
        """
        Extrai dados de um PDF usando phase4_pdf_extraction_v2.
        
        Workflow:
        1. Busca o accession code específico no PDF via PyMuPDF
        2. Se encontrado, também extrai todas as linhas para salvar no banco
        3. Salva resultados no articles_db para cache
        
        Args:
            pdf_path: Caminho do PDF
            gb_code: Código GenBank a buscar
            doi: DOI do artigo (para salvar no banco)
            
        Returns:
            AccessionLookupResult se encontrado, None caso contrário
        """
        try:
            pdf_path = Path(pdf_path)
            if not pdf_path.exists():
                logger.warning(f"PDF não encontrado: {pdf_path}")
                return None
            
            # Busca focada: apenas o accession que precisamos
            lookup_result = lookup_accession_in_pdf(pdf_path, gb_code)
            
            if not lookup_result or not lookup_result.accession:
                logger.debug(f"Accession {gb_code} não encontrado no PDF {pdf_path.name}")
                return None
            
            logger.info(
                f"[Fase 4] {gb_code} encontrado no PDF: "
                f"species={lookup_result.species}, "
                f"voucher={lookup_result.voucher}, "
                f"country={lookup_result.country} "
                f"(confidence={lookup_result.confidence})"
            )
            
            # Salvar no banco de artigos (extração completa para cache)
            self._save_pdf_extraction_to_db(pdf_path, doi)
            
            return lookup_result
            
        except Exception as e:
            logger.warning(f"Erro ao extrair dados do PDF {pdf_path}: {e}")
            return None
    
    def _save_pdf_extraction_to_db(self, pdf_path: Path, doi: str) -> None:
        """
        Extrai todas as linhas do PDF e salva no articles_db para cache.
        
        Usa extract_all_rows_from_pdf() para obter todos os registros,
        depois converte para o formato esperado pelo ArticleRecord.gb_table.
        
        extract_all_rows_from_pdf() retorna rows com gene names como keys diretas:
        {'ITS': 'GU461944', '28S': 'AY618202', 'species': '...', 'voucher': '...'}
        
        O articles_db espera keys no formato 'gb_*':
        {'gb_its': 'GU461944', 'gb_28s': 'AY618202', 'species': '...', 'voucher': '...'}
        """
        try:
            if self.articles_db.has_doi(doi):
                existing = self.articles_db.get_article(doi)
                if existing and existing.has_gb_table and existing.gb_table:
                    logger.debug(f"Artigo {doi} já tem tabela GB no cache")
                    return
            
            all_rows = extract_all_rows_from_pdf(pdf_path)
            
            if not all_rows:
                return
            
            # Known metadata keys (non-gene columns)
            meta_keys = {'species', 'voucher', 'country', 'reference', 'other_meta',
                         '_raw_pre', '_raw_acc', '_forward_filled'}
            
            # Converter para formato do articles_db
            gb_table = []
            gb_accessions_all = []
            species_all = set()
            vouchers_all = set()
            countries_all = set()
            
            for row in all_rows:
                entry = {
                    'species': row.get('species', ''),
                    'voucher': row.get('voucher', ''),
                    'country': row.get('country', ''),
                }
                
                # Gene names are direct keys in the row dict (ITS, 28S, TEF1, RPB2, etc.)
                # Convert to gb_* format and collect accession codes
                for key, value in row.items():
                    if key in meta_keys:
                        continue
                    # This is a gene column with an accession code value
                    if isinstance(value, str) and value.strip():
                        gene_key = f'gb_{key.lower().replace("-", "_").replace(" ", "_")}'
                        entry[gene_key] = value.strip()
                        gb_accessions_all.append(value.strip())
                
                gb_table.append(entry)
                
                if row.get('species'):
                    species_all.add(row['species'])
                if row.get('voucher'):
                    vouchers_all.add(row['voucher'])
                if row.get('country'):
                    countries_all.add(row['country'])
            
            # Criar ou atualizar ArticleRecord
            article = self.articles_db.get_article(doi) or ArticleRecord(doi=doi)
            article.gb_accessions = list(set(article.gb_accessions + gb_accessions_all))
            article.species_mentioned = list(set(article.species_mentioned) | species_all)
            article.vouchers_found = list(set(article.vouchers_found) | vouchers_all)
            article.countries_found = list(set(article.countries_found) | countries_all)
            article.has_gb_table = True
            article.pdf_downloaded = True
            article.pdf_path = str(pdf_path)
            article.gb_table = gb_table
            
            self.articles_db.add_article(article)
            logger.debug(f"Salvos {len(gb_table)} registros do PDF no banco para DOI {doi}")
            
        except Exception as e:
            logger.debug(f"Erro ao salvar extração do PDF no banco: {e}")
    
    def _apply_enrichment(
        self,
        df: pd.DataFrame,
        idx: int,
        result: ValidationResult
    ) -> pd.DataFrame:
        """Aplica dados enriquecidos ao DataFrame."""
        
        # Voucher
        if result.voucher.status == 'accepted' and result.voucher.value:
            current = df.at[idx, 'voucher'] if 'voucher' in df.columns else None
            if pd.isna(current) or not current or current.lower() == 'none':
                df.at[idx, 'voucher'] = result.voucher.value
                self.report.vouchers_filled += 1
        
        # Country
        country_col = 'geo_loc_name' if 'geo_loc_name' in df.columns else 'country'
        if result.country.status == 'accepted' and result.country.value:
            current = df.at[idx, country_col] if country_col in df.columns else None
            if pd.isna(current) or not current:
                df.at[idx, country_col] = result.country.value
                self.report.countries_filled += 1
        
        # Species
        species_col = 'Species' if 'Species' in df.columns else 'species'
        if result.species.status == 'accepted' and result.species.value:
            current = df.at[idx, species_col] if species_col in df.columns else None
            # Só atualizar se estava incompleto (sp., *aceae, etc.)
            if current and ('sp.' in str(current).lower() or 
                           str(current).endswith('aceae') or
                           str(current).endswith('ales') or
                           str(current).endswith('mycetes') or
                           str(current).endswith('mycota') or
                           len(str(current).split()) == 1):
                df.at[idx, species_col] = result.species.value
                self.report.species_updated += 1

        # Auditoria opcional
        if result.country_candidates:
            self._append_audit_value(df, idx, 'country_candidates', '|'.join(result.country_candidates))
        if result.voucher_conflict and result.voucher_conflict_new:
            self._append_audit_value(df, idx, 'voucher_conflict', result.voucher_conflict_new)
        if result.notes:
            self._append_audit_value(df, idx, 'validation_notes', '; '.join(result.notes))
        if result.source:
            self._append_audit_value(df, idx, 'validation_source', result.source)
        
        return df

    def _append_audit_value(self, df: pd.DataFrame, idx: int, col: str, value: str) -> None:
        """Acrescenta valor de auditoria em coluna textual sem duplicar entradas."""
        if not value:
            return

        current = df.at[idx, col] if col in df.columns else None
        if pd.isna(current) or not current:
            df.at[idx, col] = value
            return

        existing = {v.strip() for v in str(current).split(';') if v.strip()}
        if value not in existing:
            df.at[idx, col] = f"{current}; {value}"

    def _update_validation_report(self, validation: ValidationResult) -> None:
        """Atualiza métricas do relatório com base no resultado da Fase 5."""
        if validation.voucher.status == 'accepted':
            self.report.voucher_validated += 1
        elif validation.voucher.status == 'deferred' and validation.voucher.reason == 'conflict_with_existing':
            self.report.voucher_conflicts += 1
        elif validation.voucher.status == 'rejected':
            self.report.voucher_rejected += 1

        if validation.voucher_dict_updated:
            self.report.voucher_dict_updates += 1

        if validation.country.status == 'accepted':
            self.report.country_validated += 1
        elif validation.country.status == 'deferred' and validation.country.reason == 'ambiguous_country':
            self.report.country_ambiguous += 1
        elif validation.country.status == 'rejected':
            self.report.country_rejected += 1

        if validation.species.status == 'accepted':
            self.report.species_validated += 1
        elif validation.species.status == 'rejected':
            self.report.species_rejected += 1

    def _update_phase6_report(self, phase6: Phase6Report) -> None:
        self.report.phase6_countries_filled += phase6.countries_filled
        self.report.phase6_gbif_hits += phase6.gbif_hits
        self.report.phase6_idigbio_hits += phase6.idigbio_hits

    def _update_phase6_5_report(self, phase6_5: Phase6_5Report) -> None:
        self.report.phase6_5_countries_filled += phase6_5.countries_filled
        self.report.phase6_5_pdf_rows_with_matches += phase6_5.pdf_rows_with_matches
        self.report.phase6_5_pdf_rows_ambiguous += phase6_5.pdf_rows_ambiguous
        self.report.phase6_5_no_pdf_in_db += phase6_5.no_pdf_in_db
        self.report.phase6_5_pairs_extracted += phase6_5.pairs_extracted_total
        self.report.phase6_5_llm_fallback_hits += phase6_5.llm_fallback_hits

    def _update_phase7_report(self, phase7: Phase7Report) -> None:
        self.report.phase7_clusters_merged += phase7.clusters_with_merges
        self.report.phase7_rows_removed += phase7.rows_removed
        self.report.phase7_fields_merged += phase7.fields_merged

    def _save_phase6_5_audit(self, audit_df: pd.DataFrame, genus: str) -> None:
        """Salva auditoria de pares (voucher, gb_accession) -> country antes da Fase 7."""
        try:
            out_dir = self.logs_dir / "phase6_5_pairs"
            out_dir.mkdir(parents=True, exist_ok=True)

            safe_genus = re.sub(r"[^A-Za-z0-9._-]+", "_", (genus or "Unknown")).strip("_") or "Unknown"
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = out_dir / f"{safe_genus}_pairs_{stamp}.csv"

            if audit_df is None or audit_df.empty:
                pd.DataFrame(
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
                ).to_csv(csv_path, index=False)
            else:
                audit_df.to_csv(csv_path, index=False)

            logger.info(f"Fase 6.5 auditoria salva em: {csv_path}")
        except Exception as exc:
            logger.warning(f"Falha ao salvar auditoria da Fase 6.5: {exc}")
    
    def _log_final_report(self):
        """Gera log do relatório final."""
        logger.info("=" * 60)
        logger.info("RELATÓRIO DE QUALIFICAÇÃO")
        logger.info("=" * 60)
        logger.info(f"Total processados: {self.report.total_processed}")
        logger.info(f"Vouchers preenchidos: {self.report.vouchers_filled}")
        logger.info(f"Countries preenchidos: {self.report.countries_filled}")
        logger.info(f"Species atualizados: {self.report.species_updated}")
        logger.info(f"Species limpos: {self.report.species_cleaned}")
        logger.info(f"Artigos baixados: {self.report.articles_downloaded}")
        logger.info(f"Artigos do cache: {self.report.articles_from_cache}")
        logger.info(f"Downloads falhos: {self.report.failed_downloads}")
        logger.info(f"Voucher validados: {self.report.voucher_validated}")
        logger.info(f"Voucher rejeitados: {self.report.voucher_rejected}")
        logger.info(f"Voucher conflitos: {self.report.voucher_conflicts}")
        logger.info(f"Voucher_dict updates: {self.report.voucher_dict_updates}")
        logger.info(f"Countries validados: {self.report.country_validated}")
        logger.info(f"Countries rejeitados: {self.report.country_rejected}")
        logger.info(f"Countries ambíguos: {self.report.country_ambiguous}")
        logger.info(f"Species validados: {self.report.species_validated}")
        logger.info(f"Species rejeitados: {self.report.species_rejected}")
        logger.info(f"Fase 6 countries preenchidos: {self.report.phase6_countries_filled}")
        logger.info(f"Fase 6 hits GBIF: {self.report.phase6_gbif_hits}")
        logger.info(f"Fase 6 hits iDigBio: {self.report.phase6_idigbio_hits}")
        logger.info(f"Fase 6.5 countries preenchidos: {self.report.phase6_5_countries_filled}")
        logger.info(f"Fase 6.5 linhas com match em PDF: {self.report.phase6_5_pdf_rows_with_matches}")
        logger.info(f"Fase 6.5 linhas ambiguas: {self.report.phase6_5_pdf_rows_ambiguous}")
        logger.info(f"Fase 6.5 sem PDF no banco: {self.report.phase6_5_no_pdf_in_db}")
        logger.info(f"Fase 6.5 pares extraidos (bruto): {self.report.phase6_5_pairs_extracted}")
        logger.info(f"Fase 6.5 hits via fallback LLM: {self.report.phase6_5_llm_fallback_hits}")
        logger.info(f"Fase 7 clusters mesclados: {self.report.phase7_clusters_merged}")
        logger.info(f"Fase 7 linhas removidas: {self.report.phase7_rows_removed}")
        logger.info(f"Fase 7 campos mesclados: {self.report.phase7_fields_merged}")
        logger.info("=" * 60)


def qualify_genus_data(
    parquet_path: Path,
    output_path: Optional[Path] = None,
    voucher_dict_path: Optional[Path] = None,
    **kwargs
) -> Path:
    """
    Função de conveniência para qualificar dados de um gênero.
    
    Args:
        parquet_path: Caminho do arquivo parquet
        output_path: Caminho de saída (padrão: _enriched.parquet)
        voucher_dict_path: Caminho do voucher_dict.json
        **kwargs: Argumentos para MDQualifier
        
    Returns:
        Caminho do arquivo enriquecido
    """
    import json
    
    # Carregar dados
    df = pd.read_parquet(parquet_path)
    genus = parquet_path.stem.replace('_output_dm', '').replace('_processed', '')
    
    # Carregar voucher_dict se existir
    voucher_dict = None
    if voucher_dict_path and voucher_dict_path.exists():
        with open(voucher_dict_path, 'r') as f:
            voucher_dict = json.load(f)
    
    # Qualificar
    if "logs_dir" not in kwargs or kwargs.get("logs_dir") is None:
        kwargs["logs_dir"] = parquet_path.parent / "review"

    qualifier = MDQualifier(**kwargs)
    df_enriched = qualifier.qualify_dataframe(df, voucher_dict, genus)
    
    # Salvar
    if output_path is None:
        output_path = parquet_path.with_name(f"{parquet_path.stem}_enriched.parquet")
    
    df_enriched.to_parquet(output_path)
    logger.info(f"Dados enriquecidos salvos em: {output_path}")
    
    return output_path


if __name__ == "__main__":
    # Teste básico
    logging.basicConfig(level=logging.INFO)
    
    # Criar DataFrame de teste
    test_data = {
        'Species': ['Fomitiporia sp. FL01', 'Fomitiporia robusta', 'Polyporaceae'],
        'voucher': [None, 'CBS 123', None],
        'geo_loc_name': ['Brazil', None, None],
        'ITS': ['MK123456', 'MK123457', 'MK123458'],
        'title': ['Direct Submission', 'Some Paper Title', '']
    }
    df = pd.DataFrame(test_data)
    
    print("DataFrame original:")
    print(df)
    
    # Testar
    qualifier = MDQualifier(allow_scihub=False, allow_selenium=False)
    
    # Testar apenas detecção
    from .phase0_detection import detect_gaps
    report = detect_gaps(df)
    print(f"\nRelatório de lacunas: {report}")

"""
Fase 2: Banco de Dados de Artigos Processados

Estrutura em dois níveis:
1. Índice (Parquet): articles_index.parquet - busca rápida
2. Dados detalhados (JSON): articles_data/<doi_hash>.json - conteúdo completo

Permite:
- Verificar se um artigo já foi processado antes de buscar na internet
- Recuperar dados extraídos de artigos anteriores
- Evitar re-downloads desnecessários
"""

import os
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field, asdict
from collections.abc import Iterable

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ArticleRecord:
    """Registro de um artigo no banco de dados."""
    doi: str
    title: str = ""
    authors: list = field(default_factory=list)
    year: Optional[int] = None
    journal: str = ""
    
    # Dados extraídos
    gb_accessions: list = field(default_factory=list)
    species_mentioned: list = field(default_factory=list)
    vouchers_found: list = field(default_factory=list)
    countries_found: list = field(default_factory=list)
    
    # Flags
    has_gb_table: bool = False
    has_supplementary: bool = False
    pdf_downloaded: bool = False
    
    # Paths
    json_path: str = ""
    pdf_path: str = ""
    md_path: str = ""
    
    # Metadados
    processed_date: str = ""
    source: str = ""  # crossref, ncbi-elink, google-scholar
    
    # Tabela GB extraída (lista de dicts)
    gb_table: list = field(default_factory=list)
    
    def to_index_dict(self) -> dict:
        """Retorna dict para o índice (sem dados pesados)."""
        return {
            'doi': self.doi,
            'title': self.title,
            'year': self.year,
            'journal': self.journal,
            'gb_accessions': self.gb_accessions,
            'species_mentioned': self.species_mentioned,
            'vouchers_found': self.vouchers_found,
            'countries_found': self.countries_found,
            'has_gb_table': self.has_gb_table,
            'has_supplementary': self.has_supplementary,
            'pdf_downloaded': self.pdf_downloaded,
            'json_path': self.json_path,
            'processed_date': self.processed_date
        }
    
    def to_full_dict(self) -> dict:
        """Retorna dict completo para JSON."""
        return asdict(self)


def doi_to_hash(doi: str) -> str:
    """Converte DOI em hash para nome de arquivo."""
    return hashlib.md5(doi.encode()).hexdigest()


class ArticlesDatabase:
    """
    Banco de dados de artigos processados.
    
    Estrutura:
    - articles_db/
      ├── articles_index.parquet  (índice para busca rápida)
      └── articles_data/
          ├── <hash1>.json
          ├── <hash2>.json
          └── ...
    """
    
    def __init__(self, base_path: Path):
        """
        Inicializa o banco de dados.
        
        Args:
            base_path: Caminho base (ex: get_taxon_ref_/articles_db)
        """
        self.base_path = Path(base_path)
        self.index_path = self.base_path / "articles_index.parquet"
        self.data_path = self.base_path / "articles_data"
        
        # Criar diretórios se não existirem
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.data_path.mkdir(parents=True, exist_ok=True)
        
        # Carregar índice
        self._index_df = self._load_index()
        
        logger.info(f"ArticlesDatabase inicializado: {len(self._index_df)} artigos no índice")
    
    def _load_index(self) -> pd.DataFrame:
        """Carrega o índice do disco."""
        if self.index_path.exists():
            try:
                return pd.read_parquet(self.index_path)
            except Exception as e:
                logger.warning(f"Erro ao carregar índice: {e}")
        
        # Criar índice vazio
        return pd.DataFrame(columns=[
            'doi', 'title', 'year', 'journal', 
            'gb_accessions', 'species_mentioned', 'vouchers_found', 'countries_found',
            'has_gb_table', 'has_supplementary', 'pdf_downloaded',
            'json_path', 'processed_date'
        ])
    
    def _save_index(self) -> None:
        """Salva o índice no disco."""
        self._index_df.to_parquet(self.index_path, index=False)
    
    def has_doi(self, doi: str) -> bool:
        """Verifica se um DOI já está no banco."""
        if self._index_df.empty:
            return False
        return doi in self._index_df['doi'].values
    
    def has_gb_accession(self, gb_accession: str) -> bool:
        """Verifica se um código GenBank já está em algum artigo."""
        if self._index_df.empty:
            return False
        
        for accessions in self._index_df['gb_accessions']:
            if gb_accession in self._normalize_accessions_container(accessions):
                return True
        return False
    
    def find_by_gb_accession(self, gb_accession: str) -> Optional[ArticleRecord]:
        """
        Busca artigo que contém um código GenBank específico.
        
        Returns:
            ArticleRecord se encontrado, None caso contrário
        """
        if self._index_df.empty:
            return None
        
        for _, row in self._index_df.iterrows():
            accessions = row.get('gb_accessions', [])
            if gb_accession in self._normalize_accessions_container(accessions):
                return self.get_article(row['doi'])
        
        return None
    
    def find_by_doi(self, doi: str) -> Optional[ArticleRecord]:
        """Busca artigo por DOI."""
        return self.get_article(doi)
    
    def get_article(self, doi: str) -> Optional[ArticleRecord]:
        """
        Recupera registro completo de um artigo.
        
        Args:
            doi: DOI do artigo
            
        Returns:
            ArticleRecord se encontrado, None caso contrário
        """
        if not self.has_doi(doi):
            return None
        
        # Carregar JSON
        json_file = self.data_path / f"{doi_to_hash(doi)}.json"
        if json_file.exists():
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return ArticleRecord(**data)
            except Exception as e:
                logger.warning(f"Erro ao carregar artigo {doi}: {e}")
        
        return None
    
    def add_article(self, article: ArticleRecord) -> None:
        """
        Adiciona ou atualiza um artigo no banco.
        
        Args:
            article: ArticleRecord a adicionar
        """
        # Definir metadados
        article.processed_date = datetime.now().isoformat()
        article.json_path = str(self.data_path / f"{doi_to_hash(article.doi)}.json")
        
        # Salvar JSON completo
        json_file = Path(article.json_path)
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(article.to_full_dict(), f, indent=2, ensure_ascii=False)
        
        # Atualizar índice
        index_row = article.to_index_dict()
        
        if self.has_doi(article.doi):
            # Atualizar existente
            mask = self._index_df['doi'] == article.doi
            for col, val in index_row.items():
                self._index_df.loc[mask, col] = [val]
        else:
            # Adicionar novo
            new_row = pd.DataFrame([index_row])
            self._index_df = pd.concat([self._index_df, new_row], ignore_index=True)
        
        # Salvar índice
        self._save_index()
        
        logger.info(f"Artigo salvo no banco: {article.doi}")
    
    def get_data_for_gb(self, gb_accession: str) -> Optional[dict]:
        """
        Busca dados (species, voucher, country) para um código GenBank.
        
        Args:
            gb_accession: Código GenBank
            
        Returns:
            dict com species, voucher, country se encontrado
        """
        article = self.find_by_gb_accession(gb_accession)
        if not article or not article.gb_table:
            return None
        
        # Buscar na tabela GB do artigo
        for row in article.gb_table:
            # Verificar em todas as colunas de GB
            for key, value in row.items():
                if key.startswith('gb_') and value == gb_accession:
                    return {
                        'species': row.get('species'),
                        'voucher': row.get('voucher'),
                        'country': row.get('country'),
                        'source_doi': article.doi,
                        'source_title': article.title
                    }
        
        return None
    
    def stats(self) -> dict:
        """Retorna estatísticas do banco."""
        total = len(self._index_df)
        with_table = self._index_df['has_gb_table'].sum() if total > 0 else 0
        with_pdf = self._index_df['pdf_downloaded'].sum() if total > 0 else 0
        
        return {
            'total_articles': total,
            'with_gb_table': int(with_table),
            'with_pdf': int(with_pdf),
            'total_gb_codes': sum(
                len(self._normalize_accessions_container(acc))
                for acc in self._index_df.get('gb_accessions', [])
            )
        }

    @staticmethod
    def _normalize_accessions_container(value) -> list[str]:
        """Normaliza coluna gb_accessions para lista de strings.

        O parquet pode carregar listas como numpy.ndarray; tambem aceitamos tuple/set.
        """
        if value is None:
            return []

        if isinstance(value, str):
            txt = value.strip()
            return [txt] if txt else []

        if isinstance(value, Iterable):
            out = []
            for item in value:
                if item is None:
                    continue
                txt = str(item).strip()
                if txt:
                    out.append(txt)
            return out

        return []
    
    def add_record_from_supplementary(
        self, 
        doi: str, 
        gb_code: str, 
        record: 'ExtractedRecord'
    ) -> None:
        """
        Adiciona registro extraído de material suplementar.
        
        Args:
            doi: DOI do artigo
            gb_code: Código GenBank
            record: ExtractedRecord extraído
        """
        # Verificar se artigo já existe
        existing = self.get_article(doi)
        
        if existing:
            # Atualizar tabela GB existente
            new_entry = {
                'species': record.species,
                'voucher': record.voucher,
                'country': record.country,
            }
            new_entry.update({k: v for k, v in record.gb_codes.items()})
            
            existing.gb_table.append(new_entry)
            
            if gb_code not in existing.gb_accessions:
                existing.gb_accessions.append(gb_code)
            if record.species and record.species not in existing.species_mentioned:
                existing.species_mentioned.append(record.species)
            if record.voucher and record.voucher not in existing.vouchers_found:
                existing.vouchers_found.append(record.voucher)
            if record.country and record.country not in existing.countries_found:
                existing.countries_found.append(record.country)
            
            existing.has_supplementary = True
            self.add_article(existing)
        else:
            # Criar novo registro
            new_entry = {
                'species': record.species,
                'voucher': record.voucher,
                'country': record.country,
            }
            new_entry.update({k: v for k, v in record.gb_codes.items()})
            
            article = ArticleRecord(
                doi=doi,
                gb_accessions=[gb_code] + list(record.gb_codes.values()),
                species_mentioned=[record.species] if record.species else [],
                vouchers_found=[record.voucher] if record.voucher else [],
                countries_found=[record.country] if record.country else [],
                has_gb_table=True,
                has_supplementary=True,
                gb_table=[new_entry]
            )
            self.add_article(article)


if __name__ == "__main__":
    # Teste básico
    import tempfile
    logging.basicConfig(level=logging.DEBUG)
    
    # Criar banco temporário
    with tempfile.TemporaryDirectory() as tmpdir:
        db = ArticlesDatabase(Path(tmpdir))
        
        # Adicionar artigo de teste
        article = ArticleRecord(
            doi="10.1234/test.2024",
            title="Test Article",
            year=2024,
            gb_accessions=["KJ513293", "KJ513294"],
            species_mentioned=["Fomitiporia australiensis"],
            vouchers_found=["Dai 12086"],
            countries_found=["China"],
            has_gb_table=True,
            gb_table=[
                {
                    'species': 'Fomitiporia australiensis',
                    'voucher': 'Dai 12086',
                    'country': 'China',
                    'gb_its': 'KJ513293',
                    'gb_lsu': 'KJ513294'
                }
            ]
        )
        
        db.add_article(article)
        
        # Verificar
        print("Stats:", db.stats())
        print("Has DOI:", db.has_doi("10.1234/test.2024"))
        print("Has GB:", db.has_gb_accession("KJ513293"))
        
        # Buscar dados
        data = db.get_data_for_gb("KJ513293")
        print("Data for KJ513293:", data)

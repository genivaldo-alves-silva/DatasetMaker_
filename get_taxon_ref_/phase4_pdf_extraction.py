#!/usr/bin/env python3
"""
Phase 4: PDF Table Extraction

Extrai tabelas GenBank de artigos científicos (PDFs ou markdowns já convertidos).

Edge Cases tratados:
1. Tabela invertida/transposta (colunas são linhas)
2. Species não repetida em linhas consecutivas (forward fill)
3. Linhas mescladas (múltiplas espécies/vouchers na mesma linha)
"""

import re
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# Column name aliases para identificar tabelas GenBank
# ============================================================================

COLUMN_ALIASES = {
    'species': ['species', 'taxon', 'organism', 'name', 'taxa', 'species name', 
                'genus/species', 'genera/species'],
    'voucher': ['voucher', 'specimen', 'collection', 'herbarium', 'culture', 
                'strain', 'sample', 'sample no', 'collection reference'],
    'country': ['country', 'locality', 'location', 'origin', 'geo', 'region'],
    'gb_its': ['its', 'its1', 'its2', 'its1-5.8s-its2', 'its region'],
    'gb_lsu': ['lsu', '28s', 'nrlsu', 'd1/d2', 'nlsu'],
    'gb_ssu': ['ssu', '18s', 'nrssu'],
    'gb_tef1': ['tef1', 'tef', 'ef1', 'ef-1α', 'tef1-α', 'tef-1'],
    'gb_rpb1': ['rpb1', 'rpb-1'],
    'gb_rpb2': ['rpb2', 'rpb-2'],
    'gb_beta_tubulin': ['btub', 'β-tubulin', 'beta-tubulin', 'tub2'],
    'accession': ['accession', 'genbank', 'gb', 'accession no', 'genbank accession'],
}

# Padrões para detectar se uma coluna contém accession codes
GB_ACCESSION_PATTERN = re.compile(r'^[A-Z]{1,2}\d{5,8}$')
GB_MULTI_ACCESSION_PATTERN = re.compile(r'[A-Z]{1,2}\d{5,8}')


@dataclass
class ExtractedGBRecord:
    """Registro extraído de tabela GenBank."""
    species: str = ""
    voucher: str = ""
    country: str = ""
    gb_its: str = ""
    gb_lsu: str = ""
    gb_ssu: str = ""
    gb_tef1: str = ""
    gb_rpb1: str = ""
    gb_rpb2: str = ""
    gb_beta_tubulin: str = ""
    source_doi: str = ""
    source_table_index: int = 0
    raw_row: dict = field(default_factory=dict)


@dataclass
class TableExtractionResult:
    """Resultado da extração de tabelas."""
    records: list[ExtractedGBRecord] = field(default_factory=list)
    tables_found: int = 0
    gb_tables_found: int = 0
    was_transposed: bool = False
    had_forward_fill: bool = False
    had_merged_rows: bool = False
    errors: list[str] = field(default_factory=list)


# ============================================================================
# Parsing de Markdown
# ============================================================================

def parse_md_tables(md_text: str) -> list[pd.DataFrame]:
    """
    Extrai todas as tabelas de um texto markdown.
    
    Args:
        md_text: Conteúdo do arquivo markdown
        
    Returns:
        Lista de DataFrames, uma para cada tabela encontrada
    """
    tables = []
    lines = md_text.split('\n')
    in_table = False
    table_lines = []
    
    for line in lines:
        line_stripped = line.strip()
        if line_stripped.startswith('|') and line_stripped.endswith('|'):
            in_table = True
            table_lines.append(line_stripped)
        else:
            if in_table and table_lines:
                df = _md_table_lines_to_df(table_lines)
                if df is not None and len(df) > 0:
                    tables.append(df)
                table_lines = []
            in_table = False
    
    # Última tabela
    if table_lines:
        df = _md_table_lines_to_df(table_lines)
        if df is not None and len(df) > 0:
            tables.append(df)
    
    return tables


def _md_table_lines_to_df(lines: list[str]) -> Optional[pd.DataFrame]:
    """Converte linhas de tabela markdown para DataFrame."""
    if len(lines) < 2:
        return None
    
    rows = []
    for line in lines:
        cells = [c.strip() for c in line.strip('|').split('|')]
        rows.append(cells)
    
    header = rows[0]
    
    # Ignorar linha separadora (contém apenas - ou :)
    data_rows = []
    for row in rows[1:]:
        is_separator = all(
            set(c.replace('-', '').replace(':', '').strip()) == set() 
            or c.strip() == '' 
            for c in row
        )
        if not is_separator:
            data_rows.append(row)
    
    if not data_rows:
        return None
    
    # Normalizar número de colunas
    n_cols = len(header)
    data_rows = [
        r + [''] * (n_cols - len(r)) if len(r) < n_cols else r[:n_cols] 
        for r in data_rows
    ]
    
    return pd.DataFrame(data_rows, columns=header)


# ============================================================================
# Detecção de Tabela GenBank
# ============================================================================

def is_genbank_table(df: pd.DataFrame) -> bool:
    """
    Verifica se uma tabela contém dados GenBank.
    
    Critérios:
    - Tem colunas que parecem ser species/voucher/accession
    - OU tem células que parecem accession codes
    """
    columns_str = ' '.join([str(c).lower() for c in df.columns])
    
    # Verificar nomes de colunas
    gb_keywords = ['its', 'lsu', 'ssu', 'tef', 'rpb', 'accession', 'genbank']
    if any(kw in columns_str for kw in gb_keywords):
        return True
    
    # Verificar conteúdo das células
    accession_count = 0
    sample_size = min(50, len(df) * len(df.columns))
    
    for col in df.columns:
        for val in df[col].head(10):
            if isinstance(val, str) and GB_ACCESSION_PATTERN.match(val.strip()):
                accession_count += 1
                if accession_count >= 3:
                    return True
    
    return False


def identify_column_type(col_name: str, sample_values: list[str]) -> Optional[str]:
    """
    Identifica o tipo de uma coluna baseado no nome e valores.
    
    Returns:
        Tipo da coluna ('species', 'voucher', 'gb_its', etc.) ou None
    """
    col_lower = str(col_name).lower().strip()
    
    # Verificar por nome
    for col_type, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in col_lower:
                return col_type
    
    # Verificar por conteúdo (accession codes)
    accession_count = sum(
        1 for v in sample_values 
        if isinstance(v, str) and GB_ACCESSION_PATTERN.match(v.strip())
    )
    if accession_count >= len(sample_values) * 0.5:
        # É uma coluna de accessions - tentar identificar qual gene
        return 'accession'  # Genérico
    
    return None


# ============================================================================
# Edge Case 1: Detectar e corrigir tabela transposta
# ============================================================================

def detect_transposed_table(df: pd.DataFrame) -> bool:
    """
    Detecta se a tabela está transposta (colunas são linhas).
    
    Indicadores:
    - Primeira coluna contém termos como 'Accession', 'Species', 'Substrate'
    - Outras colunas contêm dados
    """
    if df.empty or len(df.columns) < 2:
        return False
    
    first_col = df.iloc[:, 0]
    transpose_keywords = [
        'accession', 'species', 'substrate', 'origin', 'locality', 
        'collection', 'voucher', 'genera', 'genus'
    ]
    
    keyword_matches = 0
    for val in first_col:
        if isinstance(val, str):
            val_lower = val.lower()
            if any(kw in val_lower for kw in transpose_keywords):
                keyword_matches += 1
    
    return keyword_matches >= 2


def transpose_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transpõe a tabela: primeira coluna vira header, resto vira dados.
    """
    # Primeira coluna são os nomes das colunas
    new_headers = df.iloc[:, 0].tolist()
    
    # Resto são os dados
    data_columns = df.iloc[:, 1:].T
    data_columns.columns = new_headers
    
    return data_columns.reset_index(drop=True)


# ============================================================================
# Edge Case 2: Forward fill para species não repetida
# ============================================================================

def forward_fill_species(df: pd.DataFrame, species_col: str) -> tuple[pd.DataFrame, bool]:
    """
    Preenche valores vazios de species com o valor da linha anterior.
    
    Detecta o padrão onde:
    - Linha tem species mas outras colunas vazias → header de grupo
    - Linhas seguintes tem vouchers mas species vazio → pertencem ao grupo
    
    PADRÃO ESPECIAL: Quando a coluna species também contém vouchers
    (ex: "F. apiahyna" seguido de "MUCL 51451" na mesma coluna)
    
    Returns:
        (DataFrame processado, bool se forward fill foi aplicado)
    """
    df = df.copy()
    applied = False
    
    if species_col not in df.columns:
        return df, False
    
    # Detectar colunas de dados (accession-like ou locality)
    data_cols = [c for c in df.columns if c != species_col and str(c).strip()]
    
    # Padrão para detectar nome de espécie válida
    # "F. apiahyna" ou "Fomitiporia apiahyna" ou "F. apiahyna (Author) Author"
    SPECIES_PATTERN = re.compile(
        r'^[A-Z][a-z]*\.?\s+[a-z]+(\s+\([^)]+\))?(\s+[A-Z][a-z]+)?'
    )
    
    # Padrão para detectar voucher/culture
    # "MUCL 44777" ou "CBS 123" ou "He 525"
    VOUCHER_PATTERN = re.compile(
        r'^[A-Z]{2,6}[-\s]?\d+|^[A-Z][a-z]+\s+\d+'
    )
    
    current_species = ""
    rows_to_drop = []
    new_species_col = []
    new_voucher_data = []
    
    for idx in df.index:
        val = str(df.at[idx, species_col]).strip()
        
        # Verificar se outras colunas estão vazias
        other_vals = [str(df.at[idx, col]).strip() for col in data_cols]
        other_vals_non_empty = [v for v in other_vals if v and v not in ('-', 'nan', 'None', '')]
        other_vals_empty = len(other_vals_non_empty) == 0
        
        # Ignorar linhas de sub-header (ex: "Voucher specimens / cultures reference")
        if 'voucher' in val.lower() and 'reference' in val.lower():
            rows_to_drop.append(idx)
            continue
        
        # Verificar se é nome de espécie
        is_species = bool(SPECIES_PATTERN.match(val))
        
        # Verificar se é voucher
        is_voucher = bool(VOUCHER_PATTERN.match(val)) and not is_species
        
        if is_species:
            current_species = val
            applied = True
            
            # Se é linha de header (só tem species, resto vazio) - marcar para remoção
            if other_vals_empty:
                rows_to_drop.append(idx)
            else:
                # Tem species E dados - manter como está
                new_species_col.append((idx, val, None))
        
        elif is_voucher and current_species:
            # É um voucher que pertence ao current_species
            new_species_col.append((idx, current_species, val))
            applied = True
        
        elif val and val not in ('-', 'nan', 'None', ''):
            # Outro valor - manter como está mas propagar species se disponível
            if current_species and other_vals_non_empty:
                # Tem outros dados, provavelmente é um voucher sem o padrão comum
                new_species_col.append((idx, current_species, val))
                applied = True
            else:
                new_species_col.append((idx, val, None))
        else:
            # Valor vazio
            if current_species:
                new_species_col.append((idx, current_species, None))
                applied = True
            else:
                new_species_col.append((idx, '', None))
    
    # Aplicar as mudanças
    # Primeiro, criar coluna de voucher se necessário
    has_voucher_to_add = any(v for (_, _, v) in new_species_col if v is not None)
    
    if has_voucher_to_add:
        # Tentar encontrar coluna de voucher existente
        voucher_col = None
        for col in df.columns:
            if 'voucher' in str(col).lower() or 'specimen' in str(col).lower() or 'sample' in str(col).lower():
                voucher_col = col
                break
        
        # Se não existe, criar uma
        if voucher_col is None:
            voucher_col = '_extracted_voucher'
            df[voucher_col] = ''
    else:
        voucher_col = None
    
    for idx, species, voucher in new_species_col:
        if idx not in rows_to_drop:
            df.at[idx, species_col] = species
            # Se detectamos voucher na coluna species, adicionar à coluna voucher
            if voucher and voucher_col:
                current_voucher = str(df.at[idx, voucher_col]).strip()
                if current_voucher in ('', '-', 'nan', 'None'):
                    df.at[idx, voucher_col] = voucher
    
    # Remover linhas de header/sub-header
    if rows_to_drop:
        df = df.drop(rows_to_drop).reset_index(drop=True)
    
    return df, applied


# ============================================================================
# Edge Case 3: Separar linhas mescladas
# ============================================================================

def detect_merged_rows(df: pd.DataFrame) -> list[int]:
    """
    Detecta linhas que parecem ter múltiplos registros mesclados.
    
    Indicadores:
    - Coluna species tem dois nomes de gênero
    - Coluna voucher tem dois vouchers separados por espaço
    - Coluna accession tem dois ou mais codes
    """
    merged_rows = []
    
    for idx in df.index:
        for col in df.columns:
            val = str(df.at[idx, col])
            
            # Verificar múltiplos accession codes
            accessions = GB_MULTI_ACCESSION_PATTERN.findall(val)
            if len(accessions) >= 2:
                # Verificar se não é esperado (ex: ITS e LSU na mesma célula)
                col_lower = col.lower()
                if not any(x in col_lower for x in ['its', 'lsu', 'tef', 'rpb']):
                    merged_rows.append(idx)
                    break
            
            # Verificar múltiplos vouchers (padrão: "He 525 He 536")
            voucher_pattern = re.compile(r'([A-Z][a-z]*\s*\d+)')
            vouchers = voucher_pattern.findall(val)
            if len(vouchers) >= 2 and ' ' in val:
                merged_rows.append(idx)
                break
    
    return list(set(merged_rows))


def split_merged_row(row: pd.Series, n_parts: int = 2) -> list[dict]:
    """
    Tenta dividir uma linha mesclada em múltiplas linhas.
    
    Estratégia heurística:
    - Encontrar padrões repetidos em cada célula
    - Dividir pelo número de repetições
    """
    result = [{} for _ in range(n_parts)]
    
    for col, val in row.items():
        val_str = str(val)
        
        # Tentar encontrar múltiplos accessions
        accessions = GB_MULTI_ACCESSION_PATTERN.findall(val_str)
        if len(accessions) >= n_parts:
            for i in range(n_parts):
                if i < len(accessions):
                    result[i][col] = accessions[i]
                else:
                    result[i][col] = ""
            continue
        
        # Tentar dividir por espaço (para vouchers concatenados)
        parts = val_str.split()
        if len(parts) >= n_parts * 2:  # Ex: "He 525 He 536" = 4 parts
            chunk_size = len(parts) // n_parts
            for i in range(n_parts):
                start = i * chunk_size
                end = start + chunk_size if i < n_parts - 1 else len(parts)
                result[i][col] = ' '.join(parts[start:end])
            continue
        
        # Não conseguiu dividir - usar mesmo valor para todos
        for i in range(n_parts):
            result[i][col] = val_str
    
    return result


def expand_merged_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expande linhas mescladas em múltiplas linhas.
    """
    merged_indices = detect_merged_rows(df)
    
    if not merged_indices:
        return df
    
    new_rows = []
    
    for idx in df.index:
        if idx in merged_indices:
            row = df.loc[idx]
            # Tentar detectar quantas linhas estão mescladas
            for col in df.columns:
                val = str(df.at[idx, col])
                accessions = GB_MULTI_ACCESSION_PATTERN.findall(val)
                if len(accessions) >= 2:
                    n_parts = len(accessions)
                    break
            else:
                n_parts = 2
            
            split_rows = split_merged_row(row, n_parts)
            new_rows.extend(split_rows)
        else:
            new_rows.append(df.loc[idx].to_dict())
    
    return pd.DataFrame(new_rows)


# ============================================================================
# Extração principal
# ============================================================================

def extract_gb_records_from_table(
    df: pd.DataFrame, 
    source_doi: str = "",
    table_index: int = 0
) -> list[ExtractedGBRecord]:
    """
    Extrai registros GenBank de uma tabela já processada.
    """
    records = []
    
    # Mapear colunas
    col_mapping = {}
    for col in df.columns:
        col_type = identify_column_type(col, df[col].head(10).tolist())
        if col_type:
            col_mapping[col_type] = col
    
    for idx in df.index:
        record = ExtractedGBRecord(
            source_doi=source_doi,
            source_table_index=table_index,
            raw_row=df.loc[idx].to_dict()
        )
        
        # Preencher campos mapeados
        for field_name in ['species', 'voucher', 'country', 'gb_its', 'gb_lsu', 
                           'gb_ssu', 'gb_tef1', 'gb_rpb1', 'gb_rpb2', 'gb_beta_tubulin']:
            if field_name in col_mapping:
                col = col_mapping[field_name]
                val = str(df.at[idx, col]).strip()
                if val and val not in ('-', 'nan', 'None', ''):
                    setattr(record, field_name, val)
        
        # Se não mapeou species mas tem coluna parecida, tentar
        if not record.species:
            for col in df.columns:
                col_lower = col.lower()
                if 'species' in col_lower or 'genus' in col_lower or 'name' in col_lower:
                    val = str(df.at[idx, col]).strip()
                    if val and val not in ('-', 'nan', 'None', ''):
                        record.species = val
                        break
        
        # Se não mapeou voucher mas tem coluna _extracted_voucher ou parecida
        if not record.voucher:
            for col in df.columns:
                col_lower = str(col).lower()
                if '_extracted_voucher' in col_lower or 'sample' in col_lower:
                    val = str(df.at[idx, col]).strip()
                    if val and val not in ('-', 'nan', 'None', ''):
                        record.voucher = val
                        break
        
        # Só adicionar se tem algum dado útil
        has_data = any([
            record.species, record.voucher, record.gb_its, 
            record.gb_lsu, record.gb_tef1
        ])
        if has_data:
            records.append(record)
    
    return records


def process_markdown_file(
    md_path: Path | str,
    source_doi: str = ""
) -> TableExtractionResult:
    """
    Processa um arquivo markdown e extrai tabelas GenBank.
    
    Args:
        md_path: Caminho para arquivo .md
        source_doi: DOI do artigo fonte
        
    Returns:
        TableExtractionResult com registros extraídos e metadados
    """
    result = TableExtractionResult()
    md_path = Path(md_path)
    
    if not md_path.exists():
        result.errors.append(f"Arquivo não encontrado: {md_path}")
        return result
    
    try:
        md_content = md_path.read_text(encoding='utf-8')
    except Exception as e:
        result.errors.append(f"Erro ao ler arquivo: {e}")
        return result
    
    # Extrair todas as tabelas
    tables = parse_md_tables(md_content)
    result.tables_found = len(tables)
    
    for tbl_idx, df in enumerate(tables):
        if not is_genbank_table(df):
            continue
        
        result.gb_tables_found += 1
        logger.info(f"Tabela {tbl_idx + 1}: detectada como tabela GenBank")
        
        # Edge Case 1: Verificar se está transposta
        if detect_transposed_table(df):
            logger.info(f"  → Transpondo tabela")
            df = transpose_table(df)
            result.was_transposed = True
        
        # Edge Case 3: Expandir linhas mescladas
        merged = detect_merged_rows(df)
        if merged:
            logger.info(f"  → Expandindo {len(merged)} linhas mescladas")
            df = expand_merged_rows(df)
            result.had_merged_rows = True
        
        # Edge Case 2: Forward fill species
        species_col = None
        for col in df.columns:
            col_lower = str(col).lower()
            if 'species' in col_lower or 'genus' in col_lower:
                species_col = col
                break
        
        if species_col:
            df, ff_applied = forward_fill_species(df, species_col)
            if ff_applied:
                logger.info(f"  → Forward fill aplicado em coluna '{species_col}'")
                result.had_forward_fill = True
        
        # Extrair registros
        records = extract_gb_records_from_table(df, source_doi, tbl_idx)
        result.records.extend(records)
        logger.info(f"  → Extraídos {len(records)} registros")
    
    return result


def find_record_by_gb_code(
    records: list[ExtractedGBRecord], 
    gb_code: str
) -> Optional[ExtractedGBRecord]:
    """
    Busca um registro pelo código GenBank.
    """
    gb_code = gb_code.strip().upper()
    
    for record in records:
        for field in ['gb_its', 'gb_lsu', 'gb_ssu', 'gb_tef1', 'gb_rpb1', 'gb_rpb2', 'gb_beta_tubulin']:
            val = getattr(record, field, '')
            if val and val.strip().upper() == gb_code:
                return record
    
    return None


# ============================================================================
# Interface para Docling (quando disponível)
# ============================================================================

def process_pdf_with_docling(
    pdf_path: Path | str,
    source_doi: str = "",
    output_md_path: Optional[Path] = None
) -> TableExtractionResult:
    """
    Processa um PDF diretamente com Docling.
    
    NOTA: Requer Docling instalado e pode ser lento em CPU.
    Use process_markdown_file() se já tiver o .md convertido.
    """
    result = TableExtractionResult()
    pdf_path = Path(pdf_path)
    
    if not pdf_path.exists():
        result.errors.append(f"PDF não encontrado: {pdf_path}")
        return result
    
    try:
        import os
        os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
        
        import torch
        torch.set_default_device("cpu")
        
        from docling.document_converter import DocumentConverter
        
        converter = DocumentConverter()
        doc_result = converter.convert(str(pdf_path))
        
        # Exportar para markdown
        md_content = doc_result.document.export_to_markdown()
        
        # Opcionalmente salvar
        if output_md_path:
            output_md_path.write_text(md_content, encoding='utf-8')
        
        # Processar markdown
        tables = parse_md_tables(md_content)
        result.tables_found = len(tables)
        
        # Mesmo processamento de process_markdown_file
        for tbl_idx, df in enumerate(tables):
            if not is_genbank_table(df):
                continue
            
            result.gb_tables_found += 1
            
            if detect_transposed_table(df):
                df = transpose_table(df)
                result.was_transposed = True
            
            merged = detect_merged_rows(df)
            if merged:
                df = expand_merged_rows(df)
                result.had_merged_rows = True
            
            species_col = None
            for col in df.columns:
                if 'species' in str(col).lower() or 'genus' in str(col).lower():
                    species_col = col
                    break
            
            if species_col:
                df, ff_applied = forward_fill_species(df, species_col)
                if ff_applied:
                    result.had_forward_fill = True
            
            records = extract_gb_records_from_table(df, source_doi, tbl_idx)
            result.records.extend(records)
        
    except ImportError:
        result.errors.append("Docling não instalado. Use process_markdown_file() com arquivo .md pré-convertido.")
    except Exception as e:
        result.errors.append(f"Erro ao processar PDF: {e}")
    
    return result


# ============================================================================
# CLI para teste
# ============================================================================

if __name__ == "__main__":
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    if len(sys.argv) < 2:
        print("Uso: python phase4_pdf_extraction.py <arquivo.md ou arquivo.pdf>")
        sys.exit(1)
    
    file_path = Path(sys.argv[1])
    
    if file_path.suffix.lower() == '.md':
        result = process_markdown_file(file_path)
    elif file_path.suffix.lower() == '.pdf':
        result = process_pdf_with_docling(file_path)
    else:
        print(f"Formato não suportado: {file_path.suffix}")
        sys.exit(1)
    
    print(f"\n=== Resultado ===")
    print(f"Tabelas encontradas: {result.tables_found}")
    print(f"Tabelas GenBank: {result.gb_tables_found}")
    print(f"Registros extraídos: {len(result.records)}")
    print(f"Foi transposta: {result.was_transposed}")
    print(f"Teve forward fill: {result.had_forward_fill}")
    print(f"Teve linhas mescladas: {result.had_merged_rows}")
    
    if result.errors:
        print(f"Erros: {result.errors}")
    
    if result.records:
        print(f"\nPrimeiros 5 registros:")
        for i, rec in enumerate(result.records[:5]):
            print(f"  {i+1}. {rec.species} | {rec.voucher} | ITS={rec.gb_its}")

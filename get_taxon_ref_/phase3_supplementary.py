"""
Fase 3.3: Extração de Material Suplementar

Detecta, baixa e processa material suplementar de artigos científicos:
- Detecta links por publisher (Elsevier, Springer, Wiley, etc.)
- Baixa diferentes formatos (.xlsx, .docx, .pdf, .csv, .zip)
- Extrai tabelas com códigos GenBank

Baseado em: Discussão de planejamento (March 2026)
"""

import os
import re
import logging
import zipfile
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from urllib.parse import urlparse, urljoin, unquote

import requests
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Timeout padrão
DEFAULT_TIMEOUT = 60


@dataclass
class PublisherConfig:
    """Configuração específica por publisher."""
    name: str
    domain_patterns: List[str]
    supp_link_selectors: List[str]
    supp_section_keywords: List[str]
    direct_supp_url_pattern: Optional[str] = None


# Configurações por publisher
PUBLISHER_CONFIGS = {
    "elsevier": PublisherConfig(
        name="Elsevier/ScienceDirect",
        domain_patterns=["sciencedirect.com", "elsevier.com"],
        supp_link_selectors=[
            "a.supplementary-content",
            "div.Appendices a[href*='mmc']",
            "a[href*='multimedia']",
            "section#appsec a[href]"
        ],
        supp_section_keywords=["supplementary", "appendix", "mmc"],
        direct_supp_url_pattern=r"https://.*?/mmc\d+\.(xlsx?|docx?|pdf)"
    ),
    
    "springer": PublisherConfig(
        name="Springer/Nature",
        domain_patterns=["springer.com", "nature.com", "springerlink.com"],
        supp_link_selectors=[
            "a[data-track-action='supplementary material']",
            "div.c-article-supplementary a",
            "a[href*='ESM']",
            "a[href*='supplementary']"
        ],
        supp_section_keywords=["electronic supplementary material", "ESM", "supplementary information"],
    ),
    
    "wiley": PublisherConfig(
        name="Wiley",
        domain_patterns=["wiley.com", "onlinelibrary.wiley.com"],
        supp_link_selectors=[
            "a.supporting-information__link",
            "div.article-section__supporting a",
            "a[href*='supp-info']"
        ],
        supp_section_keywords=["supporting information", "supplementary"],
    ),
    
    "taylor_francis": PublisherConfig(
        name="Taylor & Francis",
        domain_patterns=["tandfonline.com"],
        supp_link_selectors=[
            "a.show-pdf[href*='suppl']",
            "div.supplemental-material a",
            "a[href*='Supplemental']"
        ],
        supp_section_keywords=["supplemental material", "supplementary"],
    ),
    
    "mdpi": PublisherConfig(
        name="MDPI",
        domain_patterns=["mdpi.com"],
        supp_link_selectors=[
            "a[href*='s1']",
            "div.html-supp a",
            "a.download-suppl"
        ],
        supp_section_keywords=["supplementary materials"],
        direct_supp_url_pattern=r"https://www\.mdpi\.com/.*?/s\d+"
    ),
    
    "pensoft": PublisherConfig(
        name="Pensoft (MycoKeys, PhytoKeys)",
        domain_patterns=["mycokeys.com", "phytokeys.com", "pensoft.net", "mapress.com"],
        supp_link_selectors=[
            "a.suppl-file",
            "a[href*='suppl']",
            "div.suppl-materials a"
        ],
        supp_section_keywords=["suppl. file", "supplementary file"],
    ),
    
    "generic": PublisherConfig(
        name="Generic",
        domain_patterns=["*"],
        supp_link_selectors=[
            "a[href*='supplement']",
            "a[href*='supp']",
            "a[href*='appendix']",
            "a[href$='.xlsx']",
            "a[href$='.xls']",
            "a[href$='.docx']",
            "a[href$='.doc']"
        ],
        supp_section_keywords=["supplementary", "supporting", "appendix", "additional file"],
    )
}


@dataclass
class SupplementaryFile:
    """Informações de um arquivo suplementar."""
    url: str
    filename: str
    file_type: str  # excel, word, pdf, csv, zip, unknown
    method: str  # selector:xxx, keyword:xxx
    local_path: Optional[Path] = None
    downloaded: bool = False


@dataclass 
class ExtractedRecord:
    """Registro extraído de tabela suplementar."""
    species: Optional[str] = None
    voucher: Optional[str] = None
    country: Optional[str] = None
    gb_codes: Dict[str, str] = field(default_factory=dict)  # gene -> accession


# Padrão de código GenBank
GB_ACCESSION_PATTERN = re.compile(r'^[A-Z]{1,2}\d{5,8}$')

# Aliases de colunas
COLUMN_ALIASES = {
    'species': ['species', 'taxon', 'organism', 'name', 'taxa', 'scientific name'],
    'voucher': ['voucher', 'specimen', 'collection', 'herbarium', 'culture', 'strain', 'isolate'],
    'country': ['country', 'locality', 'location', 'origin', 'geo', 'collecting site', 'collection site'],
    'gb_its': ['its', 'its1', 'its2', 'its1-5.8s-its2', 'its nrdna'],
    'gb_lsu': ['lsu', '28s', 'nrlsu', 'd1/d2', 'nrlsu', 'nuc 28s'],
    'gb_tef1': ['tef1', 'tef', 'ef1', 'ef-1α', 'tef1-α', 'tef-1', 'ef-1a'],
    'gb_rpb1': ['rpb1', 'rpb 1'],
    'gb_rpb2': ['rpb2', 'rpb 2'],
}


def detect_publisher(url: str) -> PublisherConfig:
    """Identifica o publisher pela URL."""
    url_lower = url.lower()
    for key, config in PUBLISHER_CONFIGS.items():
        if key == "generic":
            continue
        for pattern in config.domain_patterns:
            if pattern in url_lower:
                logger.debug(f"Publisher detectado: {config.name}")
                return config
    logger.debug("Publisher não identificado, usando genérico")
    return PUBLISHER_CONFIGS["generic"]


def is_downloadable_file(href: str) -> bool:
    """Verifica se o href aponta para arquivo baixável."""
    extensions = ['.xlsx', '.xls', '.docx', '.doc', '.pdf', '.csv', '.zip', '.txt']
    href_lower = href.lower()
    return any(ext in href_lower for ext in extensions)


def guess_file_type(href: str) -> str:
    """Adivinha tipo de arquivo pela extensão."""
    href_lower = href.lower()
    if '.xlsx' in href_lower or '.xls' in href_lower:
        return 'excel'
    elif '.docx' in href_lower or '.doc' in href_lower:
        return 'word'
    elif '.pdf' in href_lower:
        return 'pdf'
    elif '.csv' in href_lower:
        return 'csv'
    elif '.zip' in href_lower:
        return 'zip'
    return 'unknown'


def extract_filename(href: str) -> str:
    """Extrai nome do arquivo da URL."""
    path = urlparse(href).path
    filename = unquote(path.split('/')[-1])
    return filename if '.' in filename else 'supplementary'


def get_page_content(url: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[str]:
    """Obtém conteúdo HTML de uma página."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=timeout, verify=False)
        if response.status_code == 200:
            return response.text
    except Exception as e:
        logger.debug(f"Erro ao obter página {url}: {e}")
    return None


def extract_supplementary_links(
    article_url: str,
    html_content: Optional[str] = None
) -> List[SupplementaryFile]:
    """
    Extrai links de material suplementar da página do artigo.
    
    Args:
        article_url: URL do artigo
        html_content: Conteúdo HTML (se já obtido)
        
    Returns:
        Lista de SupplementaryFile encontrados
    """
    publisher = detect_publisher(article_url)
    
    if not html_content:
        html_content = get_page_content(article_url)
    
    if not html_content:
        logger.warning(f"Não foi possível obter conteúdo de {article_url}")
        return []
    
    soup = BeautifulSoup(html_content, 'html.parser')
    found_links = []
    seen_urls = set()
    
    # 1. Tentar CSS selectors específicos do publisher
    for selector in publisher.supp_link_selectors:
        try:
            elements = soup.select(selector)
            for el in elements:
                href = el.get('href')
                if href and is_downloadable_file(href):
                    full_url = urljoin(article_url, href)
                    if full_url not in seen_urls:
                        seen_urls.add(full_url)
                        found_links.append(SupplementaryFile(
                            url=full_url,
                            filename=extract_filename(href),
                            file_type=guess_file_type(href),
                            method=f'selector:{selector}'
                        ))
        except Exception as e:
            logger.debug(f"Selector {selector} falhou: {e}")
    
    # 2. Buscar por keywords em seções
    for keyword in publisher.supp_section_keywords:
        try:
            sections = soup.find_all(string=re.compile(keyword, re.IGNORECASE))
            for section in sections:
                parent = section.find_parent(['div', 'section', 'aside', 'article'])
                if parent:
                    links = parent.find_all('a', href=True)
                    for link in links:
                        href = link['href']
                        if is_downloadable_file(href):
                            full_url = urljoin(article_url, href)
                            if full_url not in seen_urls:
                                seen_urls.add(full_url)
                                found_links.append(SupplementaryFile(
                                    url=full_url,
                                    filename=extract_filename(href),
                                    file_type=guess_file_type(href),
                                    method=f'keyword:{keyword}'
                                ))
        except Exception as e:
            logger.debug(f"Keyword {keyword} falhou: {e}")
    
    logger.info(f"Encontrados {len(found_links)} arquivos suplementares em {article_url}")
    return found_links


def download_supplementary_file(
    supp_file: SupplementaryFile,
    output_dir: Path,
    timeout: int = DEFAULT_TIMEOUT
) -> SupplementaryFile:
    """
    Baixa um arquivo suplementar.
    
    Args:
        supp_file: SupplementaryFile a baixar
        output_dir: Diretório destino
        timeout: Timeout em segundos
        
    Returns:
        SupplementaryFile atualizado com local_path e downloaded
    """
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Definir nome do arquivo
        filename = supp_file.filename
        output_path = output_dir / filename
        
        # Evitar sobrescrever
        if output_path.exists():
            stem = output_path.stem
            suffix = output_path.suffix
            output_path = output_dir / f"{stem}_dup{suffix}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(
            supp_file.url, 
            headers=headers,
            timeout=timeout, 
            verify=False, 
            stream=True
        )
        
        if response.status_code == 200:
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            supp_file.local_path = output_path
            supp_file.downloaded = True
            logger.info(f"Baixado: {filename}")
        else:
            logger.warning(f"Falha ao baixar {supp_file.url}: HTTP {response.status_code}")
            
    except Exception as e:
        logger.warning(f"Erro ao baixar {supp_file.url}: {e}")
    
    return supp_file


def extract_zip(zip_path: Path, output_dir: Path) -> List[SupplementaryFile]:
    """Extrai ZIP e retorna arquivos relevantes."""
    extracted = []
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for name in zf.namelist():
                if is_downloadable_file(name):
                    zf.extract(name, output_dir)
                    extracted_path = output_dir / name
                    extracted.append(SupplementaryFile(
                        url=f"zip://{zip_path}/{name}",
                        filename=Path(name).name,
                        file_type=guess_file_type(name),
                        method='extracted-from-zip',
                        local_path=extracted_path,
                        downloaded=True
                    ))
        logger.info(f"Extraídos {len(extracted)} arquivos do ZIP")
    except Exception as e:
        logger.warning(f"Erro ao extrair ZIP {zip_path}: {e}")
    return extracted


def download_supplementary_files(
    links: List[SupplementaryFile],
    output_dir: Path
) -> List[SupplementaryFile]:
    """
    Baixa todos os arquivos suplementares.
    
    Args:
        links: Lista de SupplementaryFile
        output_dir: Diretório destino
        
    Returns:
        Lista atualizada com arquivos baixados
    """
    downloaded = []
    
    for supp_file in links:
        result = download_supplementary_file(supp_file, output_dir)
        
        if result.downloaded:
            # Se for ZIP, extrair
            if result.file_type == 'zip' and result.local_path:
                extracted = extract_zip(result.local_path, output_dir)
                downloaded.extend(extracted)
            else:
                downloaded.append(result)
    
    return downloaded


def find_gb_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Identifica colunas que contêm códigos GenBank.
    
    Args:
        df: DataFrame a analisar
        
    Returns:
        Mapeamento: coluna_original → tipo_padronizado
    """
    found = {}
    
    for col in df.columns:
        col_lower = str(col).lower().strip()
        
        # Verificar aliases
        for standard_name, aliases in COLUMN_ALIASES.items():
            if any(alias in col_lower for alias in aliases):
                found[col] = standard_name
                break
        
        # Verificar se coluna contém códigos GB (mesmo sem nome reconhecido)
        if col not in found:
            try:
                sample = df[col].dropna().head(10).astype(str)
                gb_count = sum(1 for v in sample if GB_ACCESSION_PATTERN.match(v.strip()))
                if gb_count >= 3:
                    found[col] = f'gb_unknown_{len(found)}'
            except Exception:
                pass
    
    return found


def extract_records_from_df(
    df: pd.DataFrame, 
    column_mapping: Dict[str, str]
) -> List[ExtractedRecord]:
    """
    Extrai registros padronizados do DataFrame.
    
    Args:
        df: DataFrame com dados
        column_mapping: Mapeamento de colunas
        
    Returns:
        Lista de ExtractedRecord
    """
    records = []
    
    for _, row in df.iterrows():
        record = ExtractedRecord()
        has_gb = False
        
        for orig_col, standard_col in column_mapping.items():
            value = row.get(orig_col)
            if pd.notna(value):
                value = str(value).strip()
                if not value or value.lower() in ['nan', 'none', '-', 'n/a']:
                    continue
                    
                if standard_col == 'species':
                    record.species = value
                elif standard_col == 'voucher':
                    record.voucher = value
                elif standard_col == 'country':
                    record.country = value
                elif standard_col.startswith('gb_'):
                    if GB_ACCESSION_PATTERN.match(value):
                        record.gb_codes[standard_col] = value
                        has_gb = True
        
        if has_gb:
            records.append(record)
    
    return records


def parse_excel_gb_table(path: Path) -> List[ExtractedRecord]:
    """Extrai tabela GB de arquivo Excel."""
    records = []
    
    try:
        xl = pd.ExcelFile(path)
        for sheet_name in xl.sheet_names:
            try:
                df = pd.read_excel(xl, sheet_name=sheet_name)
                column_mapping = find_gb_columns(df)
                
                if column_mapping:
                    sheet_records = extract_records_from_df(df, column_mapping)
                    records.extend(sheet_records)
                    logger.debug(f"Extraídos {len(sheet_records)} registros da sheet '{sheet_name}'")
            except Exception as e:
                logger.debug(f"Erro ao processar sheet '{sheet_name}': {e}")
    except Exception as e:
        logger.warning(f"Erro ao ler Excel {path}: {e}")
    
    return records


def parse_csv_gb_table(path: Path) -> List[ExtractedRecord]:
    """Extrai tabela GB de arquivo CSV."""
    records = []
    
    # Tentar diferentes encodings
    for encoding in ['utf-8', 'latin-1', 'cp1252']:
        try:
            df = pd.read_csv(path, encoding=encoding)
            column_mapping = find_gb_columns(df)
            
            if column_mapping:
                records = extract_records_from_df(df, column_mapping)
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.debug(f"Erro ao ler CSV {path}: {e}")
            break
    
    return records


def parse_docx_gb_table(path: Path) -> List[ExtractedRecord]:
    """Extrai tabela GB de arquivo Word."""
    records = []
    
    try:
        from docx import Document
        doc = Document(path)
        
        for table in doc.tables:
            # Converter tabela Word para DataFrame
            data = []
            for row in table.rows:
                data.append([cell.text.strip() for cell in row.cells])
            
            if len(data) > 1:  # Precisa ter header + dados
                try:
                    df = pd.DataFrame(data[1:], columns=data[0])
                    column_mapping = find_gb_columns(df)
                    
                    if column_mapping:
                        table_records = extract_records_from_df(df, column_mapping)
                        records.extend(table_records)
                except Exception as e:
                    logger.debug(f"Erro ao processar tabela DOCX: {e}")
                    
    except ImportError:
        logger.warning("python-docx não instalado - instale com: pip install python-docx")
    except Exception as e:
        logger.warning(f"Erro ao ler DOCX {path}: {e}")
    
    return records


def parse_pdf_gb_table(path: Path) -> List[ExtractedRecord]:
    """
    Extrai tabela GB de arquivo PDF suplementar usando PyMuPDF (phase4 v2).
    
    Usa extract_all_rows_from_pdf() para obter todos os registros do PDF,
    depois converte cada um para ExtractedRecord.
    
    extract_all_rows_from_pdf() retorna rows com gene names como keys diretas:
    {'ITS': 'GU461944', '28S': 'AY618202', 'species': '...', 'voucher': '...'}
    
    Args:
        path: Caminho do PDF
        
    Returns:
        Lista de ExtractedRecord encontrados
    """
    records = []
    
    # Known non-gene keys from extract_all_rows_from_pdf
    meta_keys = {'species', 'voucher', 'country', 'reference', 'other_meta',
                 '_raw_pre', '_raw_acc', '_forward_filled'}
    
    try:
        from .phase4_pdf_extraction_v2 import extract_all_rows_from_pdf
        
        all_rows = extract_all_rows_from_pdf(path)
        
        for row in all_rows:
            gb_codes = {}
            
            # Gene names are direct keys (ITS, 28S, TEF1, RPB2, etc.)
            for key, value in row.items():
                if key in meta_keys:
                    continue
                if isinstance(value, str) and value.strip():
                    gene_key = f'gb_{key.lower().replace("-", "_").replace(" ", "_")}'
                    gb_codes[gene_key] = value.strip()
            
            if gb_codes:
                records.append(ExtractedRecord(
                    species=row.get('species') or None,
                    voucher=row.get('voucher') or None,
                    country=row.get('country') or None,
                    gb_codes=gb_codes
                ))
        
        if records:
            logger.info(f"Extraídos {len(records)} registros de PDF suplementar: {path.name}")
            
    except Exception as e:
        logger.warning(f"Erro ao extrair tabela GB de PDF {path}: {e}")
    
    return records


def parse_supplementary_for_gb_table(
    files: List[SupplementaryFile]
) -> List[ExtractedRecord]:
    """
    Processa arquivos suplementares buscando tabelas com códigos GenBank.
    
    Args:
        files: Lista de SupplementaryFile baixados
        
    Returns:
        Lista de ExtractedRecord encontrados
    """
    all_records = []
    
    for file_info in files:
        if not file_info.downloaded or not file_info.local_path:
            continue
            
        path = file_info.local_path
        tipo = file_info.file_type
        
        try:
            if tipo == 'excel':
                records = parse_excel_gb_table(path)
            elif tipo == 'csv':
                records = parse_csv_gb_table(path)
            elif tipo == 'word':
                records = parse_docx_gb_table(path)
            elif tipo == 'pdf':
                records = parse_pdf_gb_table(path)
            else:
                logger.debug(f"Tipo não suportado: {tipo}")
                continue
            
            if records:
                logger.info(f"Extraídos {len(records)} registros de {path.name}")
                all_records.extend(records)
                
        except Exception as e:
            logger.warning(f"Erro ao parsear {path}: {e}")
    
    return all_records


def process_supplementary_materials(
    article_url: str,
    output_dir: Path,
    html_content: Optional[str] = None
) -> List[ExtractedRecord]:
    """
    Função principal: processa material suplementar de um artigo.
    
    Args:
        article_url: URL do artigo
        output_dir: Diretório para salvar arquivos
        html_content: Conteúdo HTML (opcional)
        
    Returns:
        Lista de ExtractedRecord encontrados
    """
    # 1. Extrair links
    links = extract_supplementary_links(article_url, html_content)
    
    if not links:
        logger.info(f"Nenhum arquivo suplementar encontrado em {article_url}")
        return []
    
    # 2. Baixar arquivos
    downloaded = download_supplementary_files(links, output_dir)
    
    if not downloaded:
        logger.warning(f"Nenhum arquivo suplementar baixado de {article_url}")
        return []
    
    # 3. Extrair tabelas GB
    records = parse_supplementary_for_gb_table(downloaded)
    
    logger.info(f"Total de {len(records)} registros extraídos do material suplementar")
    return records


def find_record_by_gb_code(
    records: List[ExtractedRecord], 
    gb_code: str
) -> Optional[ExtractedRecord]:
    """
    Busca registro pelo código GenBank.
    
    Args:
        records: Lista de registros
        gb_code: Código GenBank a buscar
        
    Returns:
        ExtractedRecord ou None se não encontrado
    """
    for record in records:
        for gene, accession in record.gb_codes.items():
            if accession == gb_code:
                return record
    return None


if __name__ == "__main__":
    # Teste básico
    import tempfile
    logging.basicConfig(level=logging.DEBUG)
    
    # URL de teste (artigo com suplementar)
    test_url = "https://www.mdpi.com/2309-608X/10/5/327"  # Exemplo MDPI
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output = Path(tmpdir)
        
        print(f"Buscando material suplementar de: {test_url}")
        
        # Extrair links
        links = extract_supplementary_links(test_url)
        print(f"\nLinks encontrados: {len(links)}")
        for link in links:
            print(f"  - {link.filename} ({link.file_type}) via {link.method}")
        
        # Baixar e processar
        if links:
            records = process_supplementary_materials(test_url, output)
            print(f"\nRegistros extraídos: {len(records)}")
            for rec in records[:5]:  # Mostrar primeiros 5
                print(f"  - Species: {rec.species}, Voucher: {rec.voucher}, GB: {rec.gb_codes}")

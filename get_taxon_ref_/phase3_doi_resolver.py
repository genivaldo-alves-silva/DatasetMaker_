"""
Fase 3.1: Resolução de DOI

Obtém DOI para um registro usando cascata de métodos:
1. CrossRef (busca por título)
2. NCBI ELink (GB → PubMed → DOI)
3. Google Scholar via pop8query (fallback, pode falhar com CAPTCHA)

Tratamento da coluna title:
- (i)   Vazio             → ir direto para NCBI ELink
- (ii)  'Direct Submission' → ir direto para NCBI ELink
- (iii) 'title1 | title2' → tentar cada título
- (iv)  'Direct Submission | title' → ignorar Direct Submission, tentar resto
- (v)   'title'           → fluxo normal
"""

import os
import re
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, Tuple
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)

# Timeout padrão para requisições HTTP
DEFAULT_TIMEOUT = 15

# API Key do NCBI (opcional mas recomendado)
NCBI_API_KEY = os.getenv("NCBI_API_KEY", "")


@dataclass
class DOIResult:
    """Resultado da resolução de DOI."""
    doi: Optional[str]
    method: str  # crossref-title, ncbi-elink, google-scholar, not-found
    title_used: Optional[str] = None
    pubmed_id: Optional[str] = None
    confidence: float = 1.0  # 0-1, menor se título não bateu exatamente


def parse_title_column(title: str) -> list[str]:
    """
    Processa a coluna title e retorna lista de títulos válidos para busca.
    
    Cenários:
    (i)   Vazio             → []
    (ii)  'Direct Submission' → []
    (iii) 'title1 | title2' → ['title1', 'title2']
    (iv)  'Direct Submission | title' → ['title']
    (v)   'title'           → ['title']
    """
    if not title or not str(title).strip():
        return []
    
    title_str = str(title).strip()
    
    # Separar por pipe
    parts = [p.strip() for p in title_str.split('|')]
    
    # Filtrar 'Direct Submission' e strings vazias
    valid_titles = [
        p for p in parts 
        if p and p.lower() != 'direct submission'
    ]
    
    return valid_titles


def crossref_search_by_title(title: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[str]:
    """
    Busca DOI no CrossRef pelo título.
    
    Args:
        title: Título do artigo
        timeout: Timeout em segundos
        
    Returns:
        DOI se encontrado, None caso contrário
    """
    url = "https://api.crossref.org/works"
    params = {"query.title": title, "rows": 1}
    
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        if resp.status_code == 200:
            items = resp.json().get("message", {}).get("items", [])
            if items:
                doi = items[0].get("DOI")
                # Verificar se título bate minimamente
                found_title = items[0].get("title", [""])[0].lower()
                if _titles_match(title.lower(), found_title):
                    logger.debug(f"CrossRef: DOI encontrado para '{title[:50]}...'")
                    return doi
                else:
                    logger.debug(f"CrossRef: Título não bate - buscado: '{title[:30]}', encontrado: '{found_title[:30]}'")
    except requests.RequestException as e:
        logger.debug(f"CrossRef falhou para '{title[:50]}': {e}")
    
    return None


def _titles_match(title1: str, title2: str, threshold: float = 0.6) -> bool:
    """
    Verifica se dois títulos são suficientemente similares.
    Usa comparação simples de palavras.
    """
    words1 = set(re.findall(r'\w+', title1.lower()))
    words2 = set(re.findall(r'\w+', title2.lower()))
    
    if not words1 or not words2:
        return False
    
    intersection = words1 & words2
    union = words1 | words2
    
    similarity = len(intersection) / len(union)
    return similarity >= threshold


def ncbi_elink_gb_to_pubmed(gb_accession: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[str]:
    """
    Busca PubMed ID associado ao código GenBank via NCBI ELink.
    
    Args:
        gb_accession: Código GenBank (ex: KJ513293)
        timeout: Timeout em segundos
        
    Returns:
        PubMed ID se encontrado, None caso contrário
    """
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
    params = {
        "dbfrom": "nuccore",
        "db": "pubmed",
        "id": gb_accession,
        "retmode": "json"
    }
    
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        if resp.status_code == 200:
            data = resp.json()
            linksets = data.get("linksets", [{}])
            if linksets:
                linksetdbs = linksets[0].get("linksetdbs", [])
                for ldb in linksetdbs:
                    if ldb.get("dbto") == "pubmed":
                        links = ldb.get("links", [])
                        if links:
                            pubmed_id = str(links[0])
                            logger.debug(f"NCBI ELink: {gb_accession} → PubMed {pubmed_id}")
                            return pubmed_id
    except requests.RequestException as e:
        logger.debug(f"NCBI ELink falhou para {gb_accession}: {e}")
    
    return None


def ncbi_get_doi_from_pubmed(pubmed_id: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[Optional[str], Optional[str]]:
    """
    Obtém DOI e título a partir do PubMed ID via ESummary.
    
    Args:
        pubmed_id: PubMed ID
        timeout: Timeout em segundos
        
    Returns:
        Tuple (DOI, título) - ambos podem ser None
    """
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {
        "db": "pubmed",
        "id": pubmed_id,
        "retmode": "json"
    }
    
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        if resp.status_code == 200:
            data = resp.json()
            result = data.get("result", {}).get(str(pubmed_id), {})
            
            title = result.get("title", "")
            
            # Tentar elocationid primeiro (formato: "doi: 10.xxxx/xxxxx")
            elocationid = result.get("elocationid", "")
            if elocationid.startswith("doi:"):
                doi = elocationid.replace("doi:", "").strip()
                logger.debug(f"NCBI ESummary: PubMed {pubmed_id} → DOI {doi}")
                return doi, title
            
            # Fallback: buscar em articleids
            for aid in result.get("articleids", []):
                if aid.get("idtype") == "doi":
                    doi = aid.get("value")
                    logger.debug(f"NCBI ESummary (articleids): PubMed {pubmed_id} → DOI {doi}")
                    return doi, title
            
            # DOI não encontrado mas temos título
            return None, title
            
    except requests.RequestException as e:
        logger.debug(f"NCBI ESummary falhou para PubMed {pubmed_id}: {e}")
    
    return None, None


def google_scholar_search_gb(gb_accession: str, 
                              pop8_path: Path = None,
                              timeout: int = 120) -> Optional[str]:
    """
    Fallback: busca no Google Scholar via pop8query.
    
    NOTA: Pode falhar com CAPTCHA/blocking.
    
    Args:
        gb_accession: Código GenBank
        pop8_path: Caminho para o executável pop8query
        timeout: Timeout em segundos
        
    Returns:
        DOI se encontrado, None caso contrário
    """
    if not pop8_path or not Path(pop8_path).exists():
        logger.debug("pop8query não disponível para Google Scholar")
        return None
    
    output_file = tempfile.mktemp(suffix=".csv")
    
    try:
        result = subprocess.run([
            str(pop8_path),
            "--gscholar",
            "--keywords", gb_accession,
            "--years", "2000-2030",
            "--max", "5",
            output_file
        ], capture_output=True, text=True, timeout=timeout)
        
        if result.returncode == 0 and os.path.exists(output_file):
            import pandas as pd
            try:
                df = pd.read_csv(output_file)
                if not df.empty and 'DOI' in df.columns:
                    doi = df.iloc[0].get('DOI')
                    if doi and pd.notna(doi) and str(doi).strip():
                        logger.debug(f"Google Scholar: {gb_accession} → DOI {doi}")
                        return str(doi).strip()
            except Exception:
                pass
        else:
            # Verificar se foi erro de blocking
            if "Invalid data" in result.stderr or "522" in result.stderr:
                logger.warning("Google Scholar bloqueou a requisição (CAPTCHA)")
            
    except subprocess.TimeoutExpired:
        logger.debug(f"Google Scholar timeout para {gb_accession}")
    except Exception as e:
        logger.debug(f"Google Scholar falhou para {gb_accession}: {e}")
    finally:
        if os.path.exists(output_file):
            try:
                os.remove(output_file)
            except Exception:
                pass
    
    return None


def get_doi_for_record(gb_accession: str, 
                       title: str = None,
                       pop8_path: Path = None) -> DOIResult:
    """
    Função principal: obtém DOI para um registro.
    
    Cascata:
    1. Se tem título válido → CrossRef
    2. NCBI ELink (GB → PubMed → DOI)
    3. Google Scholar (fallback)
    
    Args:
        gb_accession: Código GenBank
        title: Conteúdo da coluna title (pode ter pipes, Direct Submission, etc)
        pop8_path: Caminho para pop8query (opcional)
        
    Returns:
        DOIResult com doi e método usado
    """
    logger.info(f"Buscando DOI para {gb_accession}...")
    
    # 1. Parse título
    titles = parse_title_column(title) if title else []
    
    # 2. Se tem títulos válidos, tentar CrossRef
    if titles:
        for t in titles:
            doi = crossref_search_by_title(t)
            if doi:
                return DOIResult(
                    doi=doi,
                    method="crossref-title",
                    title_used=t
                )
        logger.debug(f"CrossRef não encontrou DOI para nenhum dos {len(titles)} títulos")
    
    # 3. NCBI ELink (sempre tenta se CrossRef falhou ou title vazio)
    pubmed_id = ncbi_elink_gb_to_pubmed(gb_accession)
    if pubmed_id:
        doi, found_title = ncbi_get_doi_from_pubmed(pubmed_id)
        if doi:
            return DOIResult(
                doi=doi,
                method="ncbi-elink",
                pubmed_id=pubmed_id,
                title_used=found_title
            )
        elif found_title:
            # Temos título do PubMed mas não DOI - tentar CrossRef com ele
            doi = crossref_search_by_title(found_title)
            if doi:
                return DOIResult(
                    doi=doi,
                    method="ncbi-elink+crossref",
                    pubmed_id=pubmed_id,
                    title_used=found_title
                )
    
    # 4. Google Scholar (fallback, pode falhar)
    if pop8_path:
        doi = google_scholar_search_gb(gb_accession, pop8_path)
        if doi:
            return DOIResult(
                doi=doi,
                method="google-scholar"
            )
    
    logger.debug(f"Nenhum DOI encontrado para {gb_accession}")
    return DOIResult(doi=None, method="not-found")


def get_article_metadata_from_doi(doi: str, timeout: int = DEFAULT_TIMEOUT) -> dict:
    """
    Obtém metadados de um artigo a partir do DOI via CrossRef.
    
    Args:
        doi: DOI do artigo
        timeout: Timeout em segundos
        
    Returns:
        dict com title, authors, year, journal
    """
    url = f"https://api.crossref.org/works/{doi}"
    
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            message = resp.json().get("message", {})
            
            # Extrair autores
            authors = []
            for author in message.get("author", []):
                name = f"{author.get('given', '')} {author.get('family', '')}".strip()
                if name:
                    authors.append(name)
            
            # Extrair ano
            year = None
            published = message.get("published-print") or message.get("published-online")
            if published and "date-parts" in published:
                date_parts = published["date-parts"]
                if date_parts and date_parts[0]:
                    year = date_parts[0][0]
            
            return {
                'title': message.get("title", [""])[0],
                'authors': authors,
                'year': year,
                'journal': message.get("container-title", [""])[0],
                'publisher': message.get("publisher", "")
            }
    except Exception as e:
        logger.debug(f"Erro ao obter metadados para DOI {doi}: {e}")
    
    return {}


if __name__ == "__main__":
    # Teste básico
    logging.basicConfig(level=logging.DEBUG)
    
    # Testar parse de títulos
    test_titles = [
        "",
        "Direct Submission",
        "Some Article Title",
        "Direct Submission | Real Title",
        "Title 1 | Title 2 | Title 3",
    ]
    
    print("=== Teste de parse_title_column ===")
    for t in test_titles:
        result = parse_title_column(t)
        print(f"'{t}' → {result}")
    
    print("\n=== Teste de resolução de DOI ===")
    # Testar com código GenBank conhecido
    result = get_doi_for_record("PV389820", title="")
    print(f"PV389820: DOI={result.doi}, method={result.method}")
    
    if result.doi:
        print("\n=== Teste de metadados ===")
        metadata = get_article_metadata_from_doi(result.doi)
        print(f"Metadata: {metadata}")

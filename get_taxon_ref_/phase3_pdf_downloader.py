"""
Fase 3.2: Download de PDFs

Tenta baixar PDF de um artigo usando cascata de métodos:
1. Unpaywall (Open Access)
2. Europe PMC (Open Access)
3. CrossRef (link direto)
4. Sci-Hub (requests)
5. Sci-Hub (Selenium - fallback)
6. URL direta do artigo (Selenium)

Baseado em: get-taxonREF/porp8/downloader.py
"""

import os
import re
import time
import shutil
import logging
import tempfile
from pathlib import Path
from typing import Optional, Tuple
from dataclasses import dataclass

import requests
import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

logger = logging.getLogger(__name__)

# Suprimir warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Domínios Sci-Hub conhecidos
SCIHUB_DOMAINS = [
    "https://sci-hub.se",
    "https://sci-hub.st",
    "https://sci-hub.ru",
    "https://sci-hub.wf",
]

# Timeout padrão
DEFAULT_TIMEOUT = 30


@dataclass
class DownloadResult:
    """Resultado do download."""
    success: bool
    method: str  # unpaywall, europepmc, crossref, scihub, selenium-scihub, selenium-direct, failed
    pdf_path: Optional[Path] = None
    error: Optional[str] = None


def sanitize_filename(name: str, max_length: int = 100) -> str:
    """Sanitiza nome de arquivo."""
    name = re.sub(r'[\\/*?:"<>|]', '', name)
    return name[:max_length].strip()


def download_pdf_from_url(url: str, 
                          output_path: Path, 
                          verify_ssl: bool = True,
                          timeout: int = DEFAULT_TIMEOUT) -> bool:
    """
    Baixa PDF de uma URL.
    
    Args:
        url: URL do PDF
        output_path: Caminho para salvar
        verify_ssl: Verificar certificado SSL
        timeout: Timeout em segundos
        
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        response = requests.get(url, stream=True, timeout=timeout, verify=verify_ssl)
        
        content_type = response.headers.get("Content-Type", "")
        if response.status_code == 200 and "pdf" in content_type.lower():
            with open(output_path, 'wb') as f:
                f.write(response.content)
            logger.debug(f"PDF baixado: {output_path}")
            return True
        else:
            logger.debug(f"Resposta não é PDF: {url} (status={response.status_code}, type={content_type})")
            
    except requests.RequestException as e:
        logger.debug(f"Erro ao baixar PDF de {url}: {e}")
    
    return False


def try_unpaywall(doi: str, email: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[Optional[str], str]:
    """
    Tenta obter URL do PDF via Unpaywall.
    
    Args:
        doi: DOI do artigo
        email: Email para API
        timeout: Timeout
        
    Returns:
        Tuple (url_pdf, fonte)
    """
    try:
        url = f"https://api.unpaywall.org/v2/{doi}?email={email}"
        response = requests.get(url, timeout=timeout)
        
        if response.status_code == 200:
            data = response.json()
            
            # Tentar best_oa_location primeiro
            best_oa = data.get("best_oa_location", {})
            if best_oa:
                pdf_url = best_oa.get("url_for_pdf")
                if pdf_url:
                    logger.debug(f"Unpaywall: encontrado URL para {doi}")
                    return pdf_url, "unpaywall"
            
            # Fallback para outras locations
            for location in data.get("oa_locations", []):
                pdf_url = location.get("url_for_pdf")
                if pdf_url:
                    return pdf_url, "unpaywall"
                    
    except Exception as e:
        logger.debug(f"Unpaywall falhou para {doi}: {e}")
    
    return None, ""


def try_europe_pmc(doi: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[Optional[str], str]:
    """
    Tenta obter PDF via Europe PMC.
    
    Args:
        doi: DOI do artigo
        timeout: Timeout
        
    Returns:
        Tuple (url_pdf, fonte)
    """
    try:
        # Buscar no Europe PMC
        search_url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:{doi}&format=json"
        response = requests.get(search_url, timeout=timeout)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get("resultList", {}).get("result", [])
            
            if results:
                result = results[0]
                pmcid = result.get("pmcid")
                
                if pmcid:
                    # Tentar PDF do PMC
                    pdf_url = f"https://europepmc.org/backend/ptpmcrender.fcgi?accid={pmcid}&blobtype=pdf"
                    logger.debug(f"Europe PMC: encontrado PMCID {pmcid} para {doi}")
                    return pdf_url, "europepmc"
                    
    except Exception as e:
        logger.debug(f"Europe PMC falhou para {doi}: {e}")
    
    return None, ""


def try_crossref_link(doi: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[Optional[str], str]:
    """
    Tenta obter link PDF direto via CrossRef.
    
    Args:
        doi: DOI do artigo
        timeout: Timeout
        
    Returns:
        Tuple (url_pdf, fonte)
    """
    try:
        url = f"https://api.crossref.org/works/{doi}"
        response = requests.get(url, timeout=timeout)
        
        if response.status_code == 200:
            data = response.json()
            links = data.get("message", {}).get("link", [])
            
            for link in links:
                if link.get("content-type") == "application/pdf":
                    pdf_url = link.get("URL")
                    if pdf_url:
                        logger.debug(f"CrossRef: encontrado link PDF para {doi}")
                        return pdf_url, "crossref"
                        
    except Exception as e:
        logger.debug(f"CrossRef link falhou para {doi}: {e}")
    
    return None, ""


def try_scihub_requests(doi: str, verify_ssl: bool = False, timeout: int = DEFAULT_TIMEOUT) -> Tuple[Optional[str], str]:
    """
    Tenta obter PDF via Sci-Hub usando requests.
    
    Args:
        doi: DOI do artigo
        verify_ssl: Verificar SSL
        timeout: Timeout
        
    Returns:
        Tuple (url_pdf, fonte)
    """
    for domain in SCIHUB_DOMAINS:
        try:
            url = f"{domain}/{doi}"
            response = requests.get(url, verify=verify_ssl, timeout=timeout)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Buscar iframe ou embed com PDF
                iframe = soup.find("iframe")
                if iframe and iframe.get("src"):
                    src = iframe["src"]
                    if src.startswith("//"):
                        src = "https:" + src
                    elif src.startswith("/"):
                        src = domain + src
                    logger.debug(f"Sci-Hub ({domain}): encontrado iframe para {doi}")
                    return src, "scihub-requests"
                
                # Buscar link direto de PDF
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    if ".pdf" in href.lower():
                        if href.startswith("//"):
                            href = "https:" + href
                        elif href.startswith("/"):
                            href = domain + href
                        return href, "scihub-requests"
                        
        except Exception as e:
            logger.debug(f"Sci-Hub ({domain}) falhou para {doi}: {e}")
            continue
    
    return None, ""


def try_scihub_selenium(doi: str, 
                        chrome_binary: Path = None,
                        chromedriver: Path = None,
                        timeout: int = 60) -> Tuple[Optional[str], str]:
    """
    Tenta obter PDF via Sci-Hub usando Selenium (fallback).
    
    Args:
        doi: DOI do artigo
        chrome_binary: Caminho para Chrome
        chromedriver: Caminho para chromedriver
        timeout: Timeout
        
    Returns:
        Tuple (url_pdf, fonte)
    """
    if not chrome_binary or not chromedriver:
        logger.debug("Selenium não configurado, pulando")
        return None, ""
    
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        
        # Configurar Chrome
        chrome_options = Options()
        chrome_options.binary_location = str(chrome_binary)
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Diretório temporário
        user_data_dir = tempfile.mkdtemp(prefix="scihub-")
        chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
        
        service = Service(str(chromedriver))
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        try:
            for domain in SCIHUB_DOMAINS:
                try:
                    driver.get(f"{domain}/{doi}")
                    time.sleep(5)
                    
                    # Buscar elementos com PDF
                    candidates = (
                        driver.find_elements(By.TAG_NAME, "iframe") +
                        driver.find_elements(By.TAG_NAME, "embed") +
                        driver.find_elements(By.TAG_NAME, "a")
                    )
                    
                    for el in candidates:
                        href = el.get_attribute("href") or el.get_attribute("src") or ""
                        if any(sub in href for sub in [".pdf", "/pdf/"]):
                            if href.startswith("/"):
                                href = domain + href
                            logger.debug(f"Sci-Hub Selenium ({domain}): encontrado para {doi}")
                            return href, "scihub-selenium"
                            
                except Exception:
                    continue
                    
        finally:
            driver.quit()
            try:
                shutil.rmtree(user_data_dir, ignore_errors=True)
            except Exception:
                pass
                
    except ImportError:
        logger.debug("Selenium não instalado")
    except Exception as e:
        logger.debug(f"Sci-Hub Selenium falhou para {doi}: {e}")
    
    return None, ""


def try_direct_url_selenium(article_url: str,
                            chrome_binary: Path = None,
                            chromedriver: Path = None,
                            timeout: int = 30) -> Tuple[Optional[str], str]:
    """
    Tenta encontrar PDF na página do artigo usando Selenium.
    
    Args:
        article_url: URL da página do artigo
        chrome_binary: Caminho para Chrome
        chromedriver: Caminho para chromedriver
        timeout: Timeout
        
    Returns:
        Tuple (url_pdf, fonte)
    """
    if not chrome_binary or not chromedriver:
        return None, ""
    
    if not article_url or not str(article_url).startswith("http"):
        return None, ""
    
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        
        chrome_options = Options()
        chrome_options.binary_location = str(chrome_binary)
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        user_data_dir = tempfile.mkdtemp(prefix="direct-")
        chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
        
        service = Service(str(chromedriver))
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        try:
            driver.get(article_url)
            time.sleep(5)
            
            candidates = (
                driver.find_elements(By.TAG_NAME, "a") +
                driver.find_elements(By.TAG_NAME, "iframe") +
                driver.find_elements(By.TAG_NAME, "embed")
            )
            
            for el in candidates:
                href = el.get_attribute("href") or el.get_attribute("src") or ""
                text = (el.text or "").lower()
                title = (el.get_attribute("title") or "").lower()
                
                if any(sub in href for sub in [".pdf", "/pdf/", "/doi/pdf"]) or \
                   "download pdf" in text or "download pdf" in title:
                    if href.startswith("/"):
                        parsed = urlparse(article_url)
                        href = f"{parsed.scheme}://{parsed.netloc}{href}"
                    logger.debug(f"Direct URL Selenium: encontrado PDF em {article_url}")
                    return href, "selenium-direct"
                    
        finally:
            driver.quit()
            try:
                shutil.rmtree(user_data_dir, ignore_errors=True)
            except Exception:
                pass
                
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"Direct URL Selenium falhou: {e}")
    
    return None, ""


def download_article_pdf(doi: str,
                         output_path: Path,
                         email: str = None,
                         chrome_binary: Path = None,
                         chromedriver: Path = None,
                         article_url: str = None,
                         allow_scihub: bool = True,
                         allow_selenium: bool = True) -> DownloadResult:
    """
    Tenta baixar PDF de um artigo usando cascata de métodos.
    
    Args:
        doi: DOI do artigo
        output_path: Caminho para salvar o PDF
        email: Email para Unpaywall
        chrome_binary: Caminho para Chrome (opcional)
        chromedriver: Caminho para chromedriver (opcional)
        article_url: URL da página do artigo (opcional)
        allow_scihub: Permitir tentativas via Sci-Hub
        allow_selenium: Permitir uso de Selenium
        
    Returns:
        DownloadResult com status e método usado
    """
    logger.info(f"Tentando baixar PDF para DOI: {doi}")
    
    email = email or os.getenv("UNPAYWALL_EMAIL", "")
    
    # Lista de métodos a tentar (em ordem)
    methods = [
        ("unpaywall", lambda: try_unpaywall(doi, email) if email else (None, "")),
        ("europepmc", lambda: try_europe_pmc(doi)),
        ("crossref", lambda: try_crossref_link(doi)),
    ]
    
    if allow_scihub:
        methods.append(("scihub-requests", lambda: try_scihub_requests(doi)))
        
        if allow_selenium and chrome_binary and chromedriver:
            methods.append(("scihub-selenium", 
                lambda: try_scihub_selenium(doi, chrome_binary, chromedriver)))
    
    if allow_selenium and chrome_binary and chromedriver and article_url:
        methods.append(("selenium-direct",
            lambda: try_direct_url_selenium(article_url, chrome_binary, chromedriver)))
    
    # Tentar cada método
    for method_name, method_fn in methods:
        try:
            pdf_url, source = method_fn()
            
            if pdf_url:
                logger.debug(f"Tentando baixar de {method_name}: {pdf_url[:80]}...")
                
                # Criar diretório se não existir
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                if download_pdf_from_url(pdf_url, output_path, verify_ssl=False):
                    logger.info(f"PDF baixado via {method_name}: {output_path}")
                    return DownloadResult(
                        success=True,
                        method=method_name,
                        pdf_path=output_path
                    )
                    
        except Exception as e:
            logger.debug(f"Método {method_name} falhou: {e}")
            continue
    
    logger.warning(f"Falha ao baixar PDF para DOI: {doi}")
    return DownloadResult(
        success=False,
        method="failed",
        error="Todos os métodos falharam"
    )


if __name__ == "__main__":
    # Teste básico
    import tempfile
    logging.basicConfig(level=logging.DEBUG)
    
    # DOI de teste (artigo open access)
    test_doi = "10.1080/00275514.2020.1741316"
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output = Path(tmpdir) / "test.pdf"
        
        result = download_article_pdf(
            doi=test_doi,
            output_path=output,
            email="test@example.com",
            allow_scihub=False,  # Não usar Sci-Hub no teste
            allow_selenium=False
        )
        
        print(f"Resultado: success={result.success}, method={result.method}")
        if result.success:
            print(f"PDF salvo em: {result.pdf_path}")
            print(f"Tamanho: {result.pdf_path.stat().st_size} bytes")

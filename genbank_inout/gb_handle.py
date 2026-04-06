'''

'''


############### voucher dict ok
#################### uso do voucher dict ok
####################### Tentando a inclusão de title e host - host ok - title ainda falta
##### Obtenção e processamento de dados GenBank por gênero

import os
import csv
import json
import pandas as pd
import re
import logging
import datetime # Importa o módulo datetime para verificar a data
import time
import urllib.request
import urllib.error
from Bio import SeqIO
from Bio.Seq import UndefinedSequenceError
from collections import defaultdict, Counter # Importa Counter para consolidação
from openpyxl import Workbook
import shutil
import sys
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial
import multiprocessing
from dotenv import load_dotenv

# --- Configuração de Caminhos ---
# Usa o diretório do script para caminhos relativos funcionarem corretamente
# independentemente de onde o script é chamado
base_dir = os.path.dirname(os.path.abspath(__file__))

# Carrega variáveis de ambiente do arquivo .env
dotenv_path = os.path.join(base_dir, '..', '.env')
load_dotenv(dotenv_path)

# Caminho para a pasta de entrada (genbank_in)
input_folder_root = os.path.join(base_dir, 'genbank_in')
# Caminho para a pasta de saída (genbank_out)
output_folder_root = os.path.join(base_dir, 'genbank_out')

# --- Configuração de Log ---
# O log agora será configurado dinamicamente para cada gênero, garantindo que
# o arquivo de log esteja na pasta de saída correta e com o nome do gênero.
# O novo formato de log incluirá:
# - Data e hora
# - Nível da mensagem (INFO, WARNING, ERROR)
# - Mensagem detalhada

# 🔹 Campos que podem ser selecionados para extração
SELECTED_ANNOTATIONS = {
    "Species": True,
    "Order": False,
    "Family": False,
    "Genus": False,
    "GBn": True,
    "Description": True,
    "bp": True,
    "Authors": False,
    "Title": True,
    "Journal": False,
    "Sequence": True
}

SELECTED_QUALIFIERS = {
    "strain": True,
    "specimen_voucher": True,
    "isolate": True,
    "environmental_sample": True,
    "culture_collection": True,
    "bio_material": True,
    "geo_loc_name": True,
    "country": False,
    "isolation_source": False,
    "host": True,
    "db_xref": False,
    "type_material": True,
    "note": True  # 🆕 Adicionado para capturar vouchers alternativos
}

# 🔹 Obtém campos ativos com base na seleção
ACTIVE_ANNOTATIONS = [k for k, v in SELECTED_ANNOTATIONS.items() if v]
ACTIVE_QUALIFIERS = [k for k, v in SELECTED_QUALIFIERS.items() if v]
CSV_HEADERS = ACTIVE_ANNOTATIONS + ACTIVE_QUALIFIERS

# Use Parquet internamente quando possível
USE_PARQUET = True

# --- Configuração de LLM Fallback para extração de vouchers ---
# Quando True, usa LLM para extrair vouchers de notes quando regex falha
USE_LLM_FALLBACK = True
LLM_API_URL = "https://openrouter.ai/api/v1/chat/completions"
LLM_MODEL = "liquid/lfm-2.5-1.2b-instruct:free"  # Modelo gratuito com bom desempenho
# A API key pode ser definida aqui ou via variável de ambiente OPENROUTER_API_KEY
LLM_API_KEY = os.environ.get("OPENROUTER_API_KEY")

# --- Fallback Regex para genes de cópia única ---
# Usado quando o match exato com gendict.json falha.
# Estes regex são cuidadosamente construídos para evitar falsos positivos.
FALLBACK_REGEX = {
    # TEF1: translation/transcription elongation factor 1-alpha
    # Aceita "transcription" pois é erro comum de depósito no GenBank
    # EXCLUI: EF-G, EF-Tu, SPT4/5/6, elongation factor 2/3, 1-gamma/1-beta/1-delta
    "TEF1": re.compile(
        r"(?:(?:translation|transcription)\s+)?(?:elongation\s+factor|EF)[-\s]*"
        r"(?:"
        r"1[-\s]?alpha|"                          # 1-alpha, 1 alpha
        r"alpha[-\s]?1|"                          # alpha-1, alpha 1  
        r"1(?![-\s]?(?:gamma|beta|delta))\b"      # 1 (mas não 1-gamma, 1-beta, 1-delta)
        r")"
        r"(?!\s*(?:G\b|Tu\b|2|3))",                # não seguido de G, Tu, 2, 3
        re.IGNORECASE
    ),
    
    # RPB2: RNA polymerase II second largest subunit
    "RPB2": re.compile(
        r"\bRPB2\b|"
        r"RNA\s+polymerase\s+(?:II|2|B)\s+(?:second\s+(?:largest\s+)?)?subunit",
        re.IGNORECASE
    ),
    
    # RPB1: RNA polymerase II largest subunit (não "second")
    "RPB1": re.compile(
        r"\bRPB1\b|"
        r"RNA\s+polymerase\s+(?:II|2)\s+largest\s+subunit(?!\s*2)",
        re.IGNORECASE
    ),
    
    # TUB: beta-tubulin APENAS
    # EXCLUI: alpha-tubulin, gamma-tubulin, tubulin-tyrosine ligase, etc.
    "TUB": re.compile(
        r"beta[-\s]?tubulin(?!\s*(?:like|domain))|"
        r"\btub2?\b(?=.*(?:gene|partial|cds))",
        re.IGNORECASE
    ),
    
    # ATP6: ATP synthase subunit 6
    "ATP6": re.compile(
        r"\bATP6\b|"
        r"ATP(?:ase)?\s+synthase\s+(?:subunit\s+)?6",
        re.IGNORECASE
    ),
}

# --- Regex para extração de vouchers de notes ---
# Padrões para capturar vouchers adicionais do campo 'note' do GenBank
# Exemplo: "strain also named VPRI22859" → VPRI22859
NOTE_VOUCHER_PATTERNS = [
    # "strain also named VPRI22859" ou "strain named CBS 386.66"
    (re.compile(r'strain\s+(?:also\s+)?named\s+([A-Z0-9][A-Za-z0-9\s.:-]+?)(?:;|$)', re.IGNORECASE), 'strain_named'),
    
    # "identical sequence found in strain VPRI22859"
    (re.compile(r'identical\s+sequence\s+found\s+in\s+strain\s+([A-Z0-9][A-Za-z0-9\s.:-]+?)(?:;|$)', re.IGNORECASE), 'identical_strain'),
    
    # "also known as CBS 123.45"
    (re.compile(r'also\s+(?:known\s+)?as\s+([A-Z]{2,}[\s:-]?\d+(?:\.\d+)?)', re.IGNORECASE), 'also_known_as'),
]


# --- Estruturas pré-compiladas para find_gene_marker otimizado ---
# Cache global para regex compilados do gendict.json
_COMPILED_GENDICT = None
_COMPILED_GENDICT_LOCK = multiprocessing.Lock() if hasattr(multiprocessing, 'Lock') else None


def compile_gendict(gene_dict: dict) -> list:
    """
    Pré-compila todos os padrões do gendict.json para regex.
    
    Retorna uma lista de tuplas (gene_name, compiled_regex, pattern_length)
    ordenada por pattern_length decrescente para priorizar matches mais longos.
    
    Args:
        gene_dict: Dicionário {gene_name: [pattern1, pattern2, ...]}
    
    Returns:
        Lista de tuplas (gene_name, compiled_regex, pattern_length) ordenada
    """
    compiled_patterns = []
    for gene_name, patterns in gene_dict.items():
        for pattern in patterns:
            try:
                # Compila o pattern escapado (match literal)
                compiled = re.compile(re.escape(pattern), re.IGNORECASE)
                compiled_patterns.append((gene_name, compiled, len(pattern)))
            except re.error:
                # Ignora patterns inválidos
                continue
    
    # Ordena por tamanho do pattern (decrescente) para priorizar matches mais específicos
    compiled_patterns.sort(key=lambda x: x[2], reverse=True)
    return compiled_patterns


def get_compiled_gendict(gendict_path: str = None) -> list:
    """
    Obtém o gendict compilado, usando cache se disponível.
    
    Thread-safe através de lock global.
    """
    global _COMPILED_GENDICT
    
    if _COMPILED_GENDICT is not None:
        return _COMPILED_GENDICT
    
    if gendict_path is None:
        gendict_path = os.path.join(base_dir, "gendict.json")
    
    try:
        with open(gendict_path, "r", encoding="utf-8") as f:
            gene_dict = json.load(f)
        _COMPILED_GENDICT = compile_gendict(gene_dict)
        return _COMPILED_GENDICT
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"⚠️ Erro ao carregar gendict.json: {e}")
        return []


def find_gene_marker_optimized(description: str, compiled_gendict: list = None, 
                                fallback_logger=None) -> str:
    """
    Encontra o marcador genético na descrição usando regex pré-compilados.
    
    Versão otimizada que usa patterns pré-compilados ao invés de compilar
    em cada chamada.
    
    Args:
        description: String de descrição do GenBank
        compiled_gendict: Lista de tuplas (gene_name, compiled_regex, length)
                         Se None, usa o cache global
        fallback_logger: Logger para registrar matches via fallback regex
    
    Returns:
        Nome do gene encontrado ou None
    """
    if not description:
        return None
    
    # Usa cache global se não fornecido
    if compiled_gendict is None:
        compiled_gendict = get_compiled_gendict()
    
    # Passo 1: Match com gendict pré-compilado (já ordenado por tamanho)
    for gene_name, compiled_regex, _ in compiled_gendict:
        if compiled_regex.search(description):
            return gene_name
    
    # Passo 2: Fallback regex para genes de cópia única
    for gene_name, regex in FALLBACK_REGEX.items():
        if regex.search(description):
            if fallback_logger:
                fallback_logger.info(f"[FALLBACK] '{description[:60]}...' → {gene_name}")
            return gene_name
    
    return None


def _process_row_for_gene(row_data: tuple, compiled_gendict: list) -> tuple:
    """
    Processa uma única row para encontrar o gene marker.
    
    Função auxiliar para processamento paralelo.
    
    Args:
        row_data: Tupla (index, description)
        compiled_gendict: Lista de regex pré-compilados
    
    Returns:
        Tupla (index, gene_name)
    """
    idx, description = row_data
    gene = find_gene_marker_optimized(description, compiled_gendict)
    return (idx, gene)


def _extract_record_data(record_text: str, selected_annotations: dict, 
                         active_qualifiers: list) -> dict:
    """
    Extrai dados de um registro GenBank a partir de seu texto.
    
    Função de nível de módulo para uso com multiprocessing.
    
    Args:
        record_text: Texto do registro GenBank
        selected_annotations: Dict de anotações selecionadas
        active_qualifiers: Lista de qualifiers ativos
    
    Returns:
        Dict com os dados extraídos ou None se falhar
    """
    try:
        from io import StringIO
        records = list(SeqIO.parse(StringIO(record_text), "genbank"))
        if not records:
            return None
        
        seq_record = records[0]
        taxonomy = seq_record.annotations.get("taxonomy", [])
        references = seq_record.annotations.get("references", [{}])[0]
        
        record_data = {}
        
        if selected_annotations.get("Species"):
            record_data["Species"] = seq_record.annotations.get("organism", "")
        if selected_annotations.get("Order"):
            record_data["Order"] = taxonomy[-3] if len(taxonomy) > 2 else ""
        if selected_annotations.get("Family"):
            record_data["Family"] = taxonomy[-2] if len(taxonomy) > 1 else ""
        if selected_annotations.get("Genus"):
            record_data["Genus"] = taxonomy[-1] if len(taxonomy) > 0 else ""
        if selected_annotations.get("GBn"):
            record_data["GBn"] = seq_record.name
        if selected_annotations.get("Description"):
            record_data["Description"] = seq_record.description
        if selected_annotations.get("bp"):
            record_data["bp"] = len(seq_record.seq)
        if selected_annotations.get("Authors"):
            record_data["Authors"] = getattr(references, "authors", "")
        if selected_annotations.get("Title"):
            record_data["Title"] = getattr(references, "title", "")
        if selected_annotations.get("Journal"):
            record_data["Journal"] = getattr(references, "journal", "")
        if selected_annotations.get("Sequence"):
            try:
                record_data["Sequence"] = str(seq_record.seq)
            except UndefinedSequenceError:
                record_data["Sequence"] = ""
        
        if seq_record.features:
            source_feature = next((f for f in seq_record.features if f.type == "source"), None)
            if source_feature:
                for qualifier in active_qualifiers:
                    record_data[qualifier] = source_feature.qualifiers.get(qualifier, [""])[0]
        
        return record_data
    except Exception:
        return None


def _process_gene_chunk(args: tuple) -> list:
    """
    Processa um chunk de descriptions para encontrar genes.
    
    Função de nível de módulo para ProcessPoolExecutor (bypass GIL).
    
    Args:
        args: Tupla (chunk de (idx, description), compiled_gendict)
    
    Returns:
        Lista de tuplas (idx, gene_name)
    """
    chunk, compiled_gendict = args
    results = []
    for idx, description in chunk:
        gene = find_gene_marker_optimized(description, compiled_gendict)
        results.append((idx, gene))
    return results


def _call_llm_for_vouchers(note: str) -> list:
    """
    Chama LLM (OpenRouter) para extrair vouchers de uma note quando regex falha.
    
    Args:
        note: String do campo 'note' do GenBank
    
    Returns:
        Lista de vouchers extraídos pelo LLM
    """
    if not LLM_API_KEY or not USE_LLM_FALLBACK:
        return []
    
    prompt = f"""Extract specimen/strain voucher codes from this GenBank note field.
Return ONLY a JSON object with format: {{"vouchers": ["CODE1", "CODE2"]}}
If no vouchers found, return: {{"vouchers": []}}
Do NOT include species names, gene names, or general descriptions.
Voucher codes are typically alphanumeric identifiers like "CBS 123.45", "VPRI22859", "MUCL 49406".

Note: {note}"""
    
    payload = {
        "model": LLM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 150
    }
    
    headers = {
        "Authorization": f"Bearer {LLM_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/DatasetMaker",
        "X-Title": "DatasetMaker"
    }
    
    try:
        req = urllib.request.Request(
            LLM_API_URL,
            data=json.dumps(payload).encode('utf-8'),
            headers=headers,
            method='POST'
        )
        
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode('utf-8'))
        
        # Extrai o conteúdo da resposta
        content = result.get('choices', [{}])[0].get('message', {}).get('content', '')
        
        # Tenta parsear como JSON
        # Remove possíveis marcadores de código markdown
        content = re.sub(r'^```json\s*', '', content.strip())
        content = re.sub(r'\s*```$', '', content)
        
        parsed = json.loads(content)
        vouchers = parsed.get('vouchers', [])
        
        # Filtra vouchers válidos
        valid_vouchers = []
        for v in vouchers:
            if isinstance(v, str) and v.strip():
                v = v.strip()
                # Ignora se parece com nome de espécie
                if not re.match(r'^[A-Z][a-z]+\s+[a-z]+', v):
                    valid_vouchers.append(v)
        
        return valid_vouchers
        
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, KeyError, TimeoutError) as e:
        # Silenciosamente falha - LLM é apenas fallback
        return []
    except Exception:
        return []


def extract_vouchers_from_note(note: str, use_llm_fallback: bool = True) -> list:
    """
    Extrai vouchers adicionais do campo 'note' de um registro GenBank.
    
    Primeiro tenta usar padrões regex. Se não encontrar nada e LLM estiver
    habilitado, usa LLM como fallback.
    
    Padrões regex reconhecidos:
    - "strain also named VPRI22859"
    - "identical sequence found in strain CBS 386.66"
    - "also known as MUCL 49406"
    
    Args:
        note: String do campo 'note' do GenBank
        use_llm_fallback: Se deve usar LLM quando regex não encontra nada
    
    Returns:
        Lista de vouchers extraídos (strings)
    """
    if not note or not isinstance(note, str):
        return []
    
    extracted = []
    for pattern, _ in NOTE_VOUCHER_PATTERNS:
        matches = pattern.findall(note)
        for match in matches:
            voucher = match.strip()
            # Remove pontuação final
            voucher = re.sub(r'[;,.]\s*$', '', voucher)
            # Ignora se parece com nome de espécie (começa com maiúscula seguido de minúsculas)
            if voucher and not re.match(r'^[A-Z][a-z]+\s+[a-z]+', voucher):
                extracted.append(voucher)
    
    # 🆕 Fallback LLM: se regex não encontrou nada e LLM está habilitado
    if not extracted and use_llm_fallback and USE_LLM_FALLBACK:
        llm_vouchers = _call_llm_for_vouchers(note)
        if llm_vouchers:
            extracted.extend(llm_vouchers)
    
    return extracted


# --- Função de Normalização de Vouchers ---
#def normalize_voucher(voucher_str):
#    """Normaliza uma string de voucher para criar uma chave consistente."""
#    if not isinstance(voucher_str, str):
#        return ""
#    # Remove espaços, dois-pontos e hifens e converte para maiúsculas
#    return re.sub(r'[\s:/-]', '', voucher_str).upper()


# --- Função de Normalização de Vouchers ---
def normalize_voucher(voucher_str):
    """Normaliza uma string de voucher para criar uma chave consistente (removendo caracteres e texto entre parênteses)."""
    if not isinstance(voucher_str, str):
        return ""
    # 🔹 Remove o texto entre parênteses e os parênteses em si
    normalized = re.sub(r'\(.*?\)', '', voucher_str)
    # 🔹 Remove espaços, dois-pontos e hifens e converte para maiúsculas
    return re.sub(r'[\s:/-]', '', normalized).upper()


def is_invalid_voucher(voucher_str):
    """
    Verifica se um voucher é inválido para consolidação.
    
    Vouchers inválidos são aqueles muito genéricos que podem causar
    agrupamentos incorretos, como números de 1 caractere ("1", "6", etc.).
    
    Args:
        voucher_str: String do voucher (original ou normalizado)
    
    Returns:
        True se o voucher é inválido para consolidação
    """
    if not voucher_str:
        return True
    
    # Remove espaços para a verificação
    cleaned = str(voucher_str).strip()
    
    # Vouchers numéricos de 1 caractere são inválidos
    if cleaned.isdigit() and len(cleaned) == 1:
        return True
    
    return False


# --- Classe Union-Find para agrupamento de vouchers ---
class UnionFind:
    """
    Estrutura Union-Find (Disjoint Set Union) para agrupar vouchers
    que compartilham valores em comum entre diferentes linhas do CSV.
    """
    def __init__(self):
        self.parent = {}
        self.rank = {}
    
    def find(self, x):
        """Encontra o representante do conjunto que contém x (com path compression)."""
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])  # Path compression
        return self.parent[x]
    
    def union(self, x, y):
        """Une os conjuntos que contêm x e y (com union by rank)."""
        root_x = self.find(x)
        root_y = self.find(y)
        
        if root_x == root_y:
            return
        
        # Union by rank
        if self.rank[root_x] < self.rank[root_y]:
            self.parent[root_x] = root_y
        elif self.rank[root_x] > self.rank[root_y]:
            self.parent[root_y] = root_x
        else:
            self.parent[root_y] = root_x
            self.rank[root_x] += 1
    
    def connected(self, x, y):
        """Verifica se x e y estão no mesmo conjunto."""
        return self.find(x) == self.find(y)

# --- Configuração Global de Workers ---
# Pode ser sobrescrito via parâmetro ou variável de ambiente
_GLOBAL_MAX_WORKERS = None


def set_max_workers(n_workers: int):
    """Define o número máximo de workers para todas as operações paralelas."""
    global _GLOBAL_MAX_WORKERS
    _GLOBAL_MAX_WORKERS = n_workers


def get_max_workers(default: int = None) -> int:
    """Obtém o número de workers configurado ou um valor padrão."""
    global _GLOBAL_MAX_WORKERS
    if _GLOBAL_MAX_WORKERS is not None:
        return _GLOBAL_MAX_WORKERS
    if default is not None:
        return default
    return min(8, os.cpu_count() or 2)


# --- Função de Processamento Principal ---
def process_genus_folder(genus_name, input_path, output_path, gene_dict, max_workers=None):
    """
    Processa todos os arquivos .gb de uma pasta de gênero específica.

    Args:
        genus_name (str): O nome do gênero.
        input_path (str): O caminho da pasta de entrada para este gênero.
        output_path (str): O caminho da pasta de saída para este gênero.
        gene_dict (dict): O dicionário de genes.
        max_workers (int): Número máximo de workers para paralelização interna.
                          Se None, usa o valor global ou auto-detecta.
    """
    # Configura workers para este processamento
    n_workers = max_workers or get_max_workers()
    
    print(f"\nIniciando o processamento para o gênero: {genus_name}")
    print(f"   🔧 Workers configurados: {n_workers}")
    
    # Define os caminhos dos arquivos de saída
    all_gb_file = os.path.join(output_path, f"{genus_name}_All_GBfiles.gb")
    alldata_file = os.path.join(output_path, f"{genus_name}_Alldata.txt")
    no_duplicates_file = os.path.join(output_path, f"{genus_name}_no_duplicates.txt")
    processed_file = os.path.join(output_path, f"{genus_name}_processed.txt")
    csv_file = os.path.join(output_path, f"{genus_name}_SpecimensList.csv")
    voucher_dict_file = os.path.join(output_path, f"{genus_name}_voucher_dict.json")
    output_dm_csv = os.path.join(output_path, f"{genus_name}_output_dm.csv")
    output_dm_xlsx = os.path.join(output_path, f"{genus_name}_output_dm.xlsx")
    output_dm_parquet = os.path.join(output_path, f"{genus_name}_output_dm.parquet")
    
    # Configura um logger individual para este gênero (thread-safe)
    log_file = os.path.join(output_path, f"{genus_name}_genes_ignorados.log")
    genus_logger = logging.getLogger(f"genus_{genus_name}")
    genus_logger.setLevel(logging.INFO)
    genus_logger.handlers.clear()  # Remove handlers existentes para evitar duplicação
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    genus_logger.addHandler(file_handler)
    genus_logger.propagate = False  # Evita propagação para o root logger
    
    # 🔹 Funções internas de processamento
    timings = {}
    overall_start = time.time()
    def concatenate_gb_files():
        """Passo 1: Concatena todos os arquivos GenBank em um único arquivo."""
        t0 = time.time()
        with open(all_gb_file, "w", encoding="utf-8") as outfile:
            for filename in os.listdir(input_path):
                if filename.endswith(".gb") or filename.endswith(".gbk"):
                    file_path = os.path.join(input_path, filename)
                    with open(file_path, "r", encoding="utf-8") as infile:
                        outfile.write(infile.read())
        timings['concatenate_gb_files'] = time.time() - t0
        print(f"⏱️ concatenate_gb_files: {timings['concatenate_gb_files']:.2f}s")

    def extract_genbank_data():
        """Passo 2: Extrai dados relevantes do arquivo GenBank.
        
        Versão otimizada com multiprocessing para datasets grandes.
        """
        t0 = time.time()
        
        # Configurações de paralelização
        PARALLEL_THRESHOLD_EXTRACT = 1000  # Mínimo de registros para paralelizar
        USE_PARALLEL_EXTRACT = True
        
        # Primeiro, conta os registros para decidir estratégia
        with open(all_gb_file, "r", encoding="utf-8") as handle:
            # Conta "LOCUS" que marca início de cada registro
            content = handle.read()
            n_records = content.count("\nLOCUS ") + (1 if content.startswith("LOCUS ") else 0)
        
        print(f"   📊 Registros GenBank a processar: {n_records}")
        
        # Se poucos registros, usa processamento sequencial (overhead de paralelização não compensa)
        if not USE_PARALLEL_EXTRACT or n_records < PARALLEL_THRESHOLD_EXTRACT:
            # Processamento sequencial original
            with open(all_gb_file, "r", encoding="utf-8") as handle, \
                 open(alldata_file, "w", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=CSV_HEADERS, delimiter="\t")
                writer.writeheader()
                
                for seq_record in SeqIO.parse(handle, "genbank"):
                    taxonomy = seq_record.annotations.get("taxonomy", [])
                    references = seq_record.annotations.get("references", [{}])[0]

                    record_data = {}

                    if SELECTED_ANNOTATIONS["Species"]:
                        record_data["Species"] = seq_record.annotations.get("organism", "")
                    if SELECTED_ANNOTATIONS["Order"]:
                        record_data["Order"] = taxonomy[-3] if len(taxonomy) > 2 else ""
                    if SELECTED_ANNOTATIONS["Family"]:
                        record_data["Family"] = taxonomy[-2] if len(taxonomy) > 1 else ""
                    if SELECTED_ANNOTATIONS["Genus"]:
                        record_data["Genus"] = taxonomy[-1] if len(taxonomy) > 0 else ""
                    if SELECTED_ANNOTATIONS["GBn"]:
                        record_data["GBn"] = seq_record.name
                    if SELECTED_ANNOTATIONS["Description"]:
                        record_data["Description"] = seq_record.description
                    if SELECTED_ANNOTATIONS["bp"]:
                        record_data["bp"] = len(seq_record.seq)
                    if SELECTED_ANNOTATIONS["Authors"]:
                        record_data["Authors"] = getattr(references, "authors", "")
                    if SELECTED_ANNOTATIONS["Title"]:
                        record_data["Title"] = getattr(references, "title", "")
                    if SELECTED_ANNOTATIONS["Journal"]:
                        record_data["Journal"] = getattr(references, "journal", "")
                    if SELECTED_ANNOTATIONS["Sequence"]:
                        try:
                            record_data["Sequence"] = str(seq_record.seq)
                        except UndefinedSequenceError:
                            record_data["Sequence"] = ""
                    
                    if seq_record.features:
                        source_feature = next((f for f in seq_record.features if f.type == "source"), None)
                        if source_feature:
                            for qualifier in ACTIVE_QUALIFIERS:
                                record_data[qualifier] = source_feature.qualifiers.get(qualifier, [""])[0]

                    writer.writerow(record_data)
        else:
            # Processamento paralelo com ThreadPoolExecutor
            # (ProcessPoolExecutor tem muito overhead para serialização de dados grandes)
            # Usa n_workers do escopo externo (definido em process_genus_folder)
            print(f"   🔄 Extraindo dados em paralelo ({n_workers} workers)...")
            
            # Divide o arquivo em registros individuais
            record_texts = []
            current_record = []
            
            with open(all_gb_file, "r", encoding="utf-8") as handle:
                for line in handle:
                    if line.startswith("LOCUS ") and current_record:
                        record_texts.append("".join(current_record))
                        current_record = [line]
                    else:
                        current_record.append(line)
                if current_record:
                    record_texts.append("".join(current_record))
            
            # Processa em paralelo usando ThreadPoolExecutor
            # (evita overhead de serialização do ProcessPoolExecutor)
            results = []
            
            def process_single_record(record_text):
                return _extract_record_data(record_text, SELECTED_ANNOTATIONS, ACTIVE_QUALIFIERS)
            
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                results = list(executor.map(process_single_record, record_texts))
            
            # Escreve resultados
            with open(alldata_file, "w", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=CSV_HEADERS, delimiter="\t")
                writer.writeheader()
                for record_data in results:
                    if record_data:
                        writer.writerow(record_data)
        
        timings['extract_genbank_data'] = time.time() - t0
        print(f"⏱️ extract_genbank_data: {timings['extract_genbank_data']:.2f}s")

    def remove_duplicates():
        """Passo 3: Remove linhas duplicadas usando Pandas."""
        t0 = time.time()
        df = pd.read_csv(alldata_file, sep="\t")
        df.drop_duplicates(inplace=True)
        # Escreve em Parquet internamente se configurado
        no_dup_parquet = os.path.join(output_path, f"{genus_name}_no_duplicates.parquet")
        try:
            if USE_PARQUET:
                try:
                    df.to_parquet(no_dup_parquet, index=False)
                except Exception as e:
                    print(f"⚠️ Falha ao gravar Parquet ({no_dup_parquet}): {e}\nFazendo fallback para TSV.")
                    df.to_csv(no_duplicates_file, sep="\t", index=False)
            else:
                df.to_csv(no_duplicates_file, sep="\t", index=False)
        except Exception as e:
            # Garantir que pelo menos o TSV seja escrito em caso de erro
            print(f"❌ Erro ao salvar no_duplicates: {e}")
            df.to_csv(no_duplicates_file, sep="\t", index=False)
        timings['remove_duplicates'] = time.time() - t0
        print(f"⏱️ remove_duplicates: {timings['remove_duplicates']:.2f}s")

    def process_data():
        """Passo 4: Formata os dados extraídos."""
        t0 = time.time()
        # Se Parquet está habilitado, lê do parquet gerado em remove_duplicates
        no_dup_parquet = os.path.join(output_path, f"{genus_name}_no_duplicates.parquet")
        processed_parquet = os.path.join(output_path, f"{genus_name}_processed.parquet")
        try:
            if USE_PARQUET and os.path.exists(no_dup_parquet):
                df = pd.read_parquet(no_dup_parquet)
                # Limpeza de colunas de texto removendo colchetes e aspas
                for col in df.select_dtypes(include=[object]).columns:
                    df[col] = df[col].fillna("").astype(str).str.replace(r"[\[\]'\\]", "", regex=True)
                # grava o parquet processado
                try:
                    df.to_parquet(processed_parquet, index=False)
                except Exception as e:
                    print(f"⚠️ Falha ao gravar processed Parquet: {e}")
                    # fallback para arquivo de texto
                    df.to_csv(processed_file, sep="\t", index=False)
            else:
                # Fallback legível linha-a-linha
                with open(no_duplicates_file, "r", encoding="utf-8") as infile, open(processed_file, "w", encoding="utf-8") as outfile:
                    for line in infile:
                        formatted_line = (
                            line.replace("['", "").replace("']", "")
                            .replace("[", "").replace("]", "")
                            .replace("'", "")
                        )
                        outfile.write(formatted_line)
        except Exception as e:
            print(f"❌ Erro ao processar dados: {e}")
            raise
        timings['process_data'] = time.time() - t0
        print(f"⏱️ process_data: {timings['process_data']:.2f}s")

    def txt_to_csv():
        """Passo 5: Converte o arquivo de texto processado para o formato CSV."""
        t0 = time.time()
        # Se existe parquet processado, use-o como fonte. Caso contrário, leia o arquivo de texto processado.
        processed_parquet = os.path.join(output_path, f"{genus_name}_processed.parquet")
        try:
            if USE_PARQUET and os.path.exists(processed_parquet):
                df = pd.read_parquet(processed_parquet)
            else:
                df = pd.read_csv(processed_file, sep="\t")
            df.to_csv(csv_file, index=False)
            # também salve uma versão parquet do CSV para consumo interno
            try:
                parquet_equiv = os.path.join(output_path, f"{genus_name}_SpecimensList.parquet")
                df.to_parquet(parquet_equiv, index=False)
            except Exception:
                pass
        except Exception as e:
            print(f"❌ Erro ao converter para CSV: {e}")
            raise
        timings['txt_to_csv'] = time.time() - t0
        print(f"⏱️ txt_to_csv: {timings['txt_to_csv']:.2f}s")

    def build_voucher_dict():
        """
        Passo 6: Constrói o dicionário de vouchers específico para este gênero.
        
        Usa Union-Find para mesclar grupos de vouchers que compartilham valores
        normalizados em comum, mesmo que estejam em linhas diferentes do CSV.
        
        Versão otimizada com:
        - Operações vetorizadas do Pandas (evita iterrows)
        - LLM desabilitado por padrão (muito lento para extração em massa)
        - Processamento paralelo da Fase 1
        """
        t0 = time.time()
        if not os.path.exists(csv_file):
            print(f"⚠️ O arquivo CSV '{csv_file}' não foi encontrado. Pulando a criação do dicionário de vouchers.")
            return {}
        
        df = pd.read_csv(csv_file)
        n_rows = len(df)
        
        priority = [
            'specimen_voucher', 'strain', 'culture_collection', 'isolate',
            'environmental_sample', 'bio_material'
        ]
        
        # 🔹 FASE 1 OTIMIZADA: Coleta vetorizada de valores
        t_phase1 = time.time()
        
        # Pré-processa colunas de voucher (vetorizado)
        row_values = {}
        all_normalized_values = set()
        note_extractions_count = 0
        
        # Extrai todos os valores de uma vez usando Pandas
        # Cria uma coluna auxiliar para cada coluna de prioridade
        voucher_data = {}
        for col in priority:
            if col in df.columns:
                # Converte para string e limpa
                voucher_data[col] = df[col].fillna('').astype(str).str.strip()
        
        # Função para processar uma linha (usada em paralelo)
        def process_row_vouchers(idx):
            """Processa vouchers de uma linha específica."""
            values = []
            for col in priority:
                if col not in voucher_data:
                    continue
                val = voucher_data[col].iloc[idx]
                if val and val != '':
                    original = val
                    if is_invalid_voucher(original):
                        continue
                    normalized = normalize_voucher(original)
                    if normalized:
                        values.append((original, normalized))
            return (idx, values)
        
        # Processamento paralelo ou sequencial baseado no tamanho
        PARALLEL_THRESHOLD_VOUCHER = 1000
        
        if n_rows >= PARALLEL_THRESHOLD_VOUCHER:
            # Usa n_workers do escopo externo (definido em process_genus_folder)
            print(f"   🔄 Coletando vouchers em paralelo ({n_workers} workers, {n_rows} rows)...")
            
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                results = list(executor.map(process_row_vouchers, range(n_rows)))
            
            for idx, values in results:
                if values:
                    row_values[idx] = values
                    for _, normalized in values:
                        all_normalized_values.add(normalized)
        else:
            for idx in range(n_rows):
                _, values = process_row_vouchers(idx)
                if values:
                    row_values[idx] = values
                    for _, normalized in values:
                        all_normalized_values.add(normalized)
        
        # 🔹 Extração de notes - APENAS com regex (LLM muito lento para bulk)
        # LLM pode ser usado posteriormente em casos específicos se necessário
        if 'note' in df.columns:
            notes_series = df['note'].fillna('').astype(str)
            for idx, note_val in enumerate(notes_series):
                if note_val.strip():
                    # Usa apenas regex, sem LLM (use_llm_fallback=False)
                    note_vouchers = extract_vouchers_from_note(note_val, use_llm_fallback=False)
                    for voucher in note_vouchers:
                        if not is_invalid_voucher(voucher):
                            normalized = normalize_voucher(voucher)
                            if normalized:
                                # 🔹 IMPORTANTE: Adiciona o voucher à linha MESMO se já existir
                                # em outra linha. Isso permite que o Union-Find una as linhas
                                # que compartilham o mesmo voucher (ex: MUCL49406 com note 
                                # "strain also named VPRI22859" deve ser unido com linha VPRI22859)
                                if idx not in row_values:
                                    row_values[idx] = []
                                
                                # Verifica se este normalized já está nesta linha específica
                                already_in_row = any(n == normalized for _, n in row_values[idx])
                                if not already_in_row:
                                    row_values[idx].append((voucher, normalized))
                                    # Só conta como "novo" se não existia em nenhum lugar
                                    if normalized not in all_normalized_values:
                                        note_extractions_count += 1
                                    all_normalized_values.add(normalized)
        
        print(f"   ⏱️ Fase 1 (coleta): {time.time() - t_phase1:.2f}s")
        
        if not row_values:
            print(f"⚠️ Nenhum voucher encontrado no CSV. Pulando a criação do dicionário.")
            return {}
        
        # 🆕 Log de vouchers extraídos de notes
        if note_extractions_count > 0:
            print(f"   🔍 Vouchers extraídos de 'note': {note_extractions_count}")
        
        # 🔹 FASE 2: Criar mapeamento de valor normalizado -> linhas que o contêm
        t_phase2 = time.time()
        normalized_to_rows = defaultdict(set)
        for row_idx, values in row_values.items():
            for _, normalized in values:
                normalized_to_rows[normalized].add(row_idx)
        print(f"   ⏱️ Fase 2 (mapeamento): {time.time() - t_phase2:.2f}s")
        
        # 🔹 FASE 3: Usar Union-Find para agrupar linhas que compartilham valores
        t_phase3 = time.time()
        uf = UnionFind()
        
        # Para cada valor normalizado, unir todas as linhas que o contêm
        for normalized, rows in normalized_to_rows.items():
            rows_list = list(rows)
            if len(rows_list) > 1:
                # Une todas as linhas que compartilham este valor normalizado
                first_row = rows_list[0]
                for other_row in rows_list[1:]:
                    uf.union(first_row, other_row)
            elif len(rows_list) == 1:
                # Garante que a linha esteja registrada no Union-Find
                uf.find(rows_list[0])
        print(f"   ⏱️ Fase 3 (union-find): {time.time() - t_phase3:.2f}s")
        
        # 🔹 FASE 4: Agrupar todas as linhas pelo seu representante
        t_phase4 = time.time()
        groups = defaultdict(set)  # representative -> set of row indices
        for row_idx in row_values.keys():
            representative = uf.find(row_idx)
            groups[representative].add(row_idx)
        print(f"   ⏱️ Fase 4 (agrupamento): {time.time() - t_phase4:.2f}s")
        
        # 🔹 FASE 5: Para cada grupo, coletar todos os valores originais e normalizados
        t_phase5 = time.time()
        genus_voucher_dict = {}
        
        for representative, row_indices in groups.items():
            all_originals = set()
            all_normalized = set()
            
            for row_idx in row_indices:
                for original, normalized in row_values[row_idx]:
                    all_originals.add(original)
                    all_normalized.add(normalized)
            
            # A chave será o menor valor normalizado por comprimento (não alfabético)
            # Mas preferimos chaves que não sejam apenas números (são mais descritivas)
            # Se houver empate no comprimento, usa ordem alfabética como desempate
            non_numeric_keys = [k for k in all_normalized if not k.isdigit()]
            
            if non_numeric_keys:
                # Prefere chaves alfanuméricas (não só números)
                dict_key = min(non_numeric_keys, key=lambda x: (len(x), x))
            else:
                # Fallback: se todas forem numéricas, usa a menor
                dict_key = min(all_normalized, key=lambda x: (len(x), x))
            
            # Valores ordenados para consistência
            sorted_originals = sorted(all_originals)
            
            if sorted_originals:
                genus_voucher_dict[dict_key] = sorted_originals
        
        print(f"   ⏱️ Fase 5 (construção dict): {time.time() - t_phase5:.2f}s")
        
        # Ordena o dicionário por chave para output consistente
        genus_voucher_dict = dict(sorted(genus_voucher_dict.items()))
        
        with open(voucher_dict_file, "w", encoding="utf-8") as f:
            json.dump(genus_voucher_dict, f, indent=4)
        
        timings['build_voucher_dict'] = time.time() - t0
        print(f"✅ Dicionário de vouchers para {genus_name} salvo em '{voucher_dict_file}'.")
        print(f"   📊 Total de grupos: {len(genus_voucher_dict)}")
        print(f"⏱️ build_voucher_dict: {timings['build_voucher_dict']:.2f}s")
        return genus_voucher_dict

    def generate_dm_files(input_csv, output_csv, output_dm_parquet, output_xlsx, voucher_dict):
        """Passo 7: Gera os arquivos de matriz de dados (DM) em CSV e XLSX.
        
        Versão otimizada com:
        - Regex pré-compilados para find_gene_marker
        - Processamento paralelo opcional para datasets grandes
        - Operações vetorizadas com Pandas onde possível
        """
        t0 = time.time()
        
        # Configuração de paralelização interna
        PARALLEL_THRESHOLD = 500  # Mínimo de rows para ativar paralelização
        USE_PARALLEL_GENE_DETECTION = True  # Pode ser desativado para debug
        
        # Preferir Parquet interno se existir
        parquet_input = os.path.join(output_path, f"{genus_name}_SpecimensList.parquet")
        if USE_PARQUET and os.path.exists(parquet_input):
            try:
                df = pd.read_parquet(parquet_input)
            except Exception as e:
                genus_logger.error(f"Falha ao ler Parquet {parquet_input}: {e}")
                df = None
        else:
            df = None

        if df is None:
            if not os.path.exists(input_csv):
                genus_logger.error(f"O arquivo de entrada '{input_csv}' não foi encontrado. Pulando a geração da DM.")
                return
            df = pd.read_csv(input_csv)

        # 🔹 Pré-compila o gendict (usa cache global)
        gendict_path = os.path.join(base_dir, "gendict.json")
        compiled_gendict = get_compiled_gendict(gendict_path)
        
        if not compiled_gendict:
            genus_logger.error(f"Falha ao compilar gendict.json. Abortando.")
            return

        data_dict = defaultdict(dict)
        genes_detectados = set()
        ignored_genes_summary = Counter() # Inicializa um contador para o sumário do log

        def clean_value(val):
            """Converte valor para string e trata NaN/None como string vazia."""
            s = str(val) if val is not None else ""
            # Trata 'nan', 'None', 'NaN' como vazio
            if s.lower() in ('nan', 'none', ''):
                return ""
            return s

        # 🔹 Usa DataFrame diretamente (já carregado acima)
        data_iter = df.to_dict(orient="records")
        n_rows = len(data_iter)
        
        # 🔹 ETAPA 2: Pré-processamento - detectar genes em paralelo
        t_gene_start = time.time()
        
        # Extrai todas as descriptions para processamento em batch
        descriptions = [clean_value(row.get("Description", "")) for row in data_iter]
        
        # Configuração de paralelização
        USE_PROCESS_POOL = True  # Usar ProcessPool (bypass GIL) vs ThreadPool
        
        # Decide se usa paralelização baseado no tamanho do dataset
        # Usa n_workers do escopo externo (definido em process_genus_folder)
        if USE_PARALLEL_GENE_DETECTION and n_rows >= PARALLEL_THRESHOLD:
            # Prepara dados para processamento paralelo
            indexed_descriptions = list(enumerate(descriptions))
            
            # Divide em chunks (chunks maiores reduzem overhead de IPC)
            chunk_size = max(100, n_rows // n_workers)
            chunks = [indexed_descriptions[i:i + chunk_size] 
                     for i in range(0, n_rows, chunk_size)]
            
            gene_results = {}
            
            if USE_PROCESS_POOL and n_rows >= 2000:
                # ProcessPoolExecutor - bypass GIL para operações CPU-bound
                # Melhor para datasets grandes (>2000) onde o overhead de IPC é compensado
                print(f"   🔄 Detectando genes com ProcessPool ({n_workers} workers, {n_rows} rows)...")
                
                try:
                    # Prepara argumentos: cada chunk + gendict compilado
                    chunk_args = [(chunk, compiled_gendict) for chunk in chunks]
                    
                    with ProcessPoolExecutor(max_workers=n_workers) as executor:
                        results_list = list(executor.map(_process_gene_chunk, chunk_args))
                    
                    for chunk_results in results_list:
                        for idx, gene in chunk_results:
                            gene_results[idx] = gene
                            
                except Exception as e:
                    # Fallback para ThreadPool se ProcessPool falhar
                    print(f"   ⚠️ ProcessPool falhou ({e}), usando ThreadPool...")
                    gene_results = {}
                    
                    def process_description_chunk(chunk):
                        results = []
                        for idx, desc in chunk:
                            gene = find_gene_marker_optimized(desc, compiled_gendict)
                            results.append((idx, gene))
                        return results
                    
                    with ThreadPoolExecutor(max_workers=n_workers) as executor:
                        futures = [executor.submit(process_description_chunk, chunk) for chunk in chunks]
                        for future in concurrent.futures.as_completed(futures):
                            for idx, gene in future.result():
                                gene_results[idx] = gene
            else:
                # ThreadPoolExecutor - menor overhead, bom para datasets médios
                print(f"   🔄 Detectando genes com ThreadPool ({n_workers} workers, {n_rows} rows)...")
                
                def process_description_chunk(chunk):
                    results = []
                    for idx, desc in chunk:
                        gene = find_gene_marker_optimized(desc, compiled_gendict)
                        results.append((idx, gene))
                    return results
                
                with ThreadPoolExecutor(max_workers=n_workers) as executor:
                    futures = [executor.submit(process_description_chunk, chunk) for chunk in chunks]
                    for future in concurrent.futures.as_completed(futures):
                        for idx, gene in future.result():
                            gene_results[idx] = gene
        else:
            # Processamento sequencial (datasets pequenos ou debug)
            gene_results = {}
            for idx, desc in enumerate(descriptions):
                gene_results[idx] = find_gene_marker_optimized(desc, compiled_gendict, fallback_logger=genus_logger)
        
        t_gene_end = time.time()
        print(f"   ⏱️ Detecção de genes: {t_gene_end - t_gene_start:.2f}s")

        # 🔹 Cria mapa reverso: valor_normalizado -> chave_do_dict
        # Isso permite encontrar o grupo correto a partir de QUALQUER valor normalizado
        # Ex: normalize("3/22.7") = "3227" -> encontra grupo "CUW"
        reverse_voucher_map = {}
        for dict_key, original_values in voucher_dict.items():
            for orig in original_values:
                norm = normalize_voucher(orig)
                if norm:
                    reverse_voucher_map[norm] = dict_key

        # Contador para entradas sem voucher (cada uma terá chave única)
        no_voucher_counter = 0

        # 🔹 ETAPA 3: Consolidação (sequencial - depende de estado)
        t_consolidate_start = time.time()
        
        for idx, row in enumerate(data_iter):
            species = clean_value(row.get("Species", ""))
            specimen_voucher = clean_value(row.get("specimen_voucher", ""))
            strain = clean_value(row.get("strain", ""))
            isolate = clean_value(row.get("isolate", ""))
            culture_collection = clean_value(row.get("culture_collection", ""))
            bio_material = clean_value(row.get("bio_material", ""))
            geo_loc = clean_value(row.get("geo_loc_name", ""))
            gbn = clean_value(row.get("GBn", ""))
            sequence = clean_value(row.get("Sequence", ""))
            type_material = clean_value(row.get("type_material", ""))
            host = clean_value(row.get("host", ""))
            title = clean_value(row.get("Title", ""))  # 'Title' com T maiúsculo
            description = descriptions[idx]  # Já extraído acima

            # Encontra o voucher preferencial
            voucher_key = (
                specimen_voucher or strain or isolate or
                culture_collection or bio_material
            )

            # 🔹 Trata entradas sem voucher OU com voucher inválido: cada uma fica em linha separada
            if not voucher_key or is_invalid_voucher(voucher_key):
                no_voucher_counter += 1
                # Usa uma chave interna única para separar as entradas
                key = f"__NO_VOUCHER_{no_voucher_counter}__"
                voucher = "none"  # Valor que aparecerá na coluna voucher
            else:
                # 🔹 Normaliza a chave do voucher antes de buscar no dicionário
                normalized_voucher_key = normalize_voucher(voucher_key)

                # 🔹 Usa o mapa reverso para encontrar a chave do grupo no voucher_dict
                # Isso resolve o caso onde a chave do dict é "CUW" mas estamos buscando "3227"
                group_key = reverse_voucher_map.get(normalized_voucher_key)
                
                if group_key:
                    # Encontrou o grupo - usa o primeiro valor como representante
                    found_vouchers = voucher_dict.get(group_key, [voucher_key])
                else:
                    # Não encontrou - usa o valor original como fallback
                    found_vouchers = [voucher_key]

                # 🔹 A chave do dicionário final será o primeiro voucher encontrado para esse grupo
                voucher = found_vouchers[0]
                
                # 🔹 A chave agora é apenas o voucher (não mais species+voucher)
                # Isso permite consolidar registros com espécies diferentes para o mesmo voucher
                key = voucher

            if "voucher" not in data_dict[key]:
                data_dict[key]["voucher"] = voucher
                data_dict[key]["_species"] = set()  # Conjunto para acumular espécies únicas
                data_dict[key]["_type_materials"] = set()  # Conjunto para acumular type_material únicos
                data_dict[key]["_hosts"] = set()  # Conjunto para acumular hosts únicos
                data_dict[key]["_titles"] = set()  # Conjunto para acumular títulos únicos
                data_dict[key]["_geo_locs"] = set()  # Conjunto para acumular países únicos

            # Acumula todas as espécies únicas (não vazias)
            if species:
                data_dict[key]["_species"].add(species)

            # Acumula todos os títulos únicos (não vazios)
            if title:
                data_dict[key]["_titles"].add(title)
            
            # Acumula todos os geo_loc_name únicos (não vazios)
            if geo_loc:
                data_dict[key]["_geo_locs"].add(geo_loc)
            
            # Acumula todos os type_material únicos (não vazios)
            if type_material:
                data_dict[key]["_type_materials"].add(type_material)
            
            # Acumula todos os hosts únicos (não vazios)
            if host:
                data_dict[key]["_hosts"].add(host)

            # 🔹 Usa o gene pré-calculado na etapa paralela
            gene_name = gene_results.get(idx)

            if gene_name:
                genes_detectados.add(gene_name)
                data_dict[key][gene_name] = gbn
                data_dict[key][f"seq{gene_name}"] = sequence
            else:
                description_clean = description.strip()
                ignored_genes_summary[description_clean] += 1
                genus_logger.info(f"[{species}] {description_clean} → SEM GENE DETECTADO")
        
        t_consolidate_end = time.time()
        print(f"   ⏱️ Consolidação de dados: {t_consolidate_end - t_consolidate_start:.2f}s")
        
        # 🔹 Adiciona a consolidação dos genes ignorados ao log
        if ignored_genes_summary:
            genus_logger.info("--- CONSOLIDAÇÃO DE GENES IGNORADOS ---")
            sorted_summary = ignored_genes_summary.most_common()
            for description, count in sorted_summary:
                genus_logger.info(f"Ocorrências: {count} | Descrição: {description}")
        
        gene_fields = []
        for gene in sorted(genes_detectados):
            gene_fields.extend([gene, f"seq{gene}"])

        fieldnames = ["Species", "geo_loc_name", "voucher", "type_material", "host"] + gene_fields + ["title", "how_multiloci"]

        # Escrever o CSV
        with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter="\t")
            writer.writeheader()
            for entry in data_dict.values():
                how_multiloci = sum(1 for field in gene_fields if not field.startswith("seq") and entry.get(field))
                entry["how_multiloci"] = how_multiloci
                
                # Converte o conjunto de espécies em string separada por " | "
                species_set = entry.pop("_species", set())
                entry["Species"] = " | ".join(sorted(species_set)) if species_set else ""
                
                # Converte o conjunto de títulos em string separada por " | "
                titles_set = entry.pop("_titles", set())
                entry["title"] = " | ".join(sorted(titles_set)) if titles_set else ""
                
                # Converte o conjunto de geo_loc_name em string separada por " | "
                geo_locs_set = entry.pop("_geo_locs", set())
                entry["geo_loc_name"] = " | ".join(sorted(geo_locs_set)) if geo_locs_set else ""
                
                # Converte o conjunto de type_material em string separada por " | "
                type_materials_set = entry.pop("_type_materials", set())
                entry["type_material"] = " | ".join(sorted(type_materials_set)) if type_materials_set else ""
                
                # Converte o conjunto de hosts em string separada por " | "
                hosts_set = entry.pop("_hosts", set())
                entry["host"] = " | ".join(sorted(hosts_set)) if hosts_set else ""
                
                for field in fieldnames:
                    if field not in entry:
                        entry[field] = ""
                writer.writerow(entry)

        print(f"✅ Arquivo '{output_csv}' gerado com sucesso!")

        # Tentar gerar Parquet do DM a partir do CSV
        try:
            # CSV foi escrito com tab delimiter
            df_dm = pd.read_csv(output_csv, sep="\t")
            try:
                df_dm.to_parquet(output_dm_parquet, index=False)
                print(f"✅ Arquivo Parquet '{output_dm_parquet}' gerado com sucesso!")
            except Exception as e:
                print(f"⚠️ Falha ao gravar Parquet '{output_dm_parquet}': {e}")
        except Exception as e:
            print(f"⚠️ Falha ao ler CSV gerado para produzir Parquet: {e}")

        # Escrever o XLSX
        wb = Workbook()
        ws = wb.active
        ws.title = "Dados processados"
        ws.append(fieldnames)
        for entry in data_dict.values():
            row_data = [entry.get(field, "") for field in fieldnames]
            ws.append(row_data)
        wb.save(output_xlsx)
        print(f"📘 Arquivo Excel '{output_xlsx}' gerado com sucesso!")
        timings['generate_dm_files'] = time.time() - t0
        print(f"⏱️ generate_dm_files: {timings['generate_dm_files']:.2f}s")

    def cleanup_files():
        """Passo 8: Remove arquivos temporários."""
        files_to_remove = [all_gb_file, alldata_file, no_duplicates_file, processed_file]
        for file in files_to_remove:
            if os.path.exists(file):
                os.remove(file)
                print(f"Removido: {os.path.basename(file)}")
    
    # Executa a sequência de passos
    try:
        concatenate_gb_files()
        extract_genbank_data()
        remove_duplicates()
        process_data()
        txt_to_csv()
        # 🔹 Corrigido: a função build_voucher_dict é chamada e seu retorno é armazenado
        genus_voucher_dict = build_voucher_dict() 
        # 🔹 Corrigido: o dicionário de vouchers é passado corretamente
        # Ajuste de ordem de argumentos: (input_csv, output_csv, output_dm_parquet, output_xlsx, voucher_dict)
        generate_dm_files(csv_file, output_dm_csv, output_dm_parquet, output_dm_xlsx, genus_voucher_dict) 
        cleanup_files()
        print(f"✅ Processamento completo para o gênero: {genus_name}\n")
        # Tempo total
        overall_end = time.time()
        timings['total'] = overall_end - overall_start
        # Imprime resumo de tempos
        print("\n⏱️ Resumo de tempos:")
        for k, v in timings.items():
            print(f"  - {k}: {v:.2f}s")

        # Grava resumo no log do gênero
        try:
            genus_logger.info("--- RESUMO DE TEMPOS ---")
            for k, v in timings.items():
                genus_logger.info(f"{k};{v:.4f}")
        except Exception:
            pass
        finally:
            # Fecha o handler para liberar o arquivo
            for handler in genus_logger.handlers[:]:
                handler.close()
                genus_logger.removeHandler(handler)
    except Exception as e:
        genus_logger.error(f"Erro durante o processamento do gênero {genus_name}: {e}")
        # Fecha o handler mesmo em caso de erro
        for handler in genus_logger.handlers[:]:
            handler.close()
            genus_logger.removeHandler(handler)
        print(f"❌ Ocorreu um erro no processamento de {genus_name}. Verifique o log para mais detalhes.")
        
# --- Execução Principal ---
def gb_handle(input_folder=None, output_folder=None, aggregate_all=False, parallel=True, max_workers=None, genus_filter=None, force_run=False, sppcomplex_mode=False):
    """
    Função principal para processar arquivos GenBank e gerar datasets.
    
    Args:
        input_folder: Pasta de entrada com arquivos .gb/.gbk. Se None, usa 'genbank_in'.
        output_folder: Pasta de saída. Se None, usa 'genbank_out'.
        aggregate_all: Se True, processa todos os gêneros como um único dataset.
        parallel: Se True, processa gêneros em paralelo.
        max_workers: Número máximo de threads. Se None, usa o padrão.
        genus_filter: Se especificado, processa apenas este gênero (ex: 'Fomitiporia').
                      Em sppcomplex_mode, este é o nome do complexo a processar.
        force_run: Se True, força reprocessamento mesmo sem arquivos novos.
        sppcomplex_mode: Se True, processa a pasta do complexo como dataset único.
                         Neste modo, genus_filter deve conter o nome do complexo.
    """
    # Configuração de caminhos
    script_base_dir = os.path.dirname(os.path.abspath(__file__))
    input_root = input_folder or os.path.join(script_base_dir, 'genbank_in')
    output_root = output_folder or os.path.join(script_base_dir, 'genbank_out')
    
    # Configuração de paralelização
    # workers_between_genera: para processar múltiplos gêneros em paralelo
    # workers_internal: para operações internas (extração, detecção de genes, etc.)
    workers_between_genera = max_workers or min(30, (os.cpu_count() or 1) * 2)
    
    # Define workers internos: se processando 1 gênero, usa todos os workers
    # Se processando múltiplos em paralelo, limita workers internos para evitar oversubscription
    workers_internal = max_workers or min(8, os.cpu_count() or 2)
    
    # Configura o valor global para que funções internas usem
    set_max_workers(workers_internal)
    
    print(f"   ⚙️ Configuração de workers: {workers_internal} (interno) / {workers_between_genera} (entre gêneros)")
    
    batch_size = workers_between_genera
    use_batching = False
    
    # Carrega o dicionário de genes
    gendict_path = os.path.join(script_base_dir, "gendict.json")
    try:
        with open(gendict_path, "r", encoding="utf-8") as f:
            gene_dict = json.load(f)
    except FileNotFoundError:
        print(f"❌ Erro: O arquivo 'gendict.json' não foi encontrado em '{gendict_path}'.")
        return

    # Se a opção aggregate estiver ativa, processa tudo junto
    if aggregate_all:
        print("\n🔗 Modo agregado: processando todas as pastas como um único dataset...")
        os.makedirs(output_root, exist_ok=True)
        _process_all_genera_internal(input_root, output_root, gene_dict)
    elif sppcomplex_mode and genus_filter:
        # 🔹 Modo complexo de espécies: processa pasta do complexo como dataset único
        complex_name = genus_filter
        complex_folder = os.path.join(input_root, complex_name)
        
        if not os.path.isdir(complex_folder):
            print(f"❌ Pasta do complexo não encontrada: {complex_folder}")
            return
        
        print(f"\n🔗 Modo complexo de espécies: processando '{complex_name}' como dataset único...")
        
        # Detecta pasta gb/ ou usa raiz
        gb_folder = os.path.join(complex_folder, "gb")
        if os.path.isdir(gb_folder):
            input_path = gb_folder
        else:
            input_path = complex_folder
        
        output_path = os.path.join(output_root, complex_name)
        os.makedirs(output_path, exist_ok=True)
        
        # Processa como um único "gênero" (que na verdade é o complexo)
        process_genus_folder(complex_name, input_path, output_path, gene_dict, workers_internal)
        print(f"✅ Processamento do complexo '{complex_name}' concluído.")
    else:
        # Reunir a lista de gêneros a processar
        genera_to_process = []
        for genus_name in os.listdir(input_root):
            # Se genus_filter especificado, processa apenas esse gênero
            if genus_filter and genus_name != genus_filter:
                continue
                
            genus_folder = os.path.join(input_root, genus_name)
            if not os.path.isdir(genus_folder):
                continue
            
            # 🔹 Arquivos .gb agora ficam em <genus>/gb/
            gb_folder = os.path.join(genus_folder, "gb")
            
            # Se a pasta /gb/ não existir, tenta a pasta raiz do gênero (compatibilidade)
            if os.path.isdir(gb_folder):
                input_path = gb_folder
            else:
                input_path = genus_folder
            
            output_path = os.path.join(output_root, genus_name)

            # 🔹 Verificação se deve reprocessar
            # force_run=True: sempre reprocessa
            # Caso contrário: só reprocessa se houver arquivos .gb novos (modificados hoje)
            reprocess_required = force_run
            
            if force_run:
                print(f"\n⚡ --force-run ativo: reprocessando '{genus_name}'")
            elif os.path.exists(output_path) and os.path.isdir(output_path):
                today = datetime.date.today()
                for filename in os.listdir(input_path):
                    if filename.endswith((".gb", ".gbk")):
                        file_path = os.path.join(input_path, filename)
                        mod_date = datetime.date.fromtimestamp(os.path.getmtime(file_path))
                        if mod_date >= today:
                            reprocess_required = True
                            print(f"\n📂 Novo arquivo em '{genus_name}': '{filename}' — será reprocessado.")
                            break
                if not reprocess_required:
                    print(f"\n✅ Nenhum arquivo novo encontrado em '{genus_name}'. Pulando.")
                    continue
            else:
                # Diretório de saída não existe - precisa processar
                reprocess_required = True
                print(f"\n✨ Diretório de saída para '{genus_name}' não existe. Criando e processando...")

            os.makedirs(output_path, exist_ok=True)
            genera_to_process.append((genus_name, input_path, output_path))

        if not genera_to_process:
            print("⚠️ Nenhum gênero para processar.")
            return

        if parallel and len(genera_to_process) > 1:
            print(f"\n⚡ Processando {len(genera_to_process)} gêneros em paralelo (até {workers_between_genera} threads)...")
            
            # Quando processando múltiplos gêneros em paralelo, reduz workers internos
            # para evitar oversubscription (muitas threads competindo por CPU)
            internal_workers_for_parallel = max(2, workers_internal // len(genera_to_process))
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers_between_genera) as executor:
                future_to_genus = {
                    executor.submit(process_genus_folder, g, i, o, gene_dict, internal_workers_for_parallel): g 
                    for (g, i, o) in genera_to_process
                }
                for future in concurrent.futures.as_completed(future_to_genus):
                    genus = future_to_genus[future]
                    try:
                        future.result()
                        print(f"✅ Processamento concluído para gênero: {genus}")
                    except Exception as e:
                        print(f"❌ Erro ao processar {genus}: {e}")
        else:
            # Processamento sequencial - usa todos os workers internos
            for genus_name, input_path, output_path in genera_to_process:
                process_genus_folder(genus_name, input_path, output_path, gene_dict, workers_internal)


def _process_all_genera_internal(input_root, output_root, gene_dict):
    """Função interna para processar todos os gêneros em modo agregado."""
    temp_input_dir = os.path.join(output_root, "_combined_input_temp")
    os.makedirs(temp_input_dir, exist_ok=True)

    files_copied = 0
    for dirpath, dirnames, filenames in os.walk(input_root):
        for fname in filenames:
            if fname.endswith(('.gb', '.gbk')):
                src = os.path.join(dirpath, fname)
                dst = os.path.join(temp_input_dir, f"{files_copied:06d}_{fname}")
                try:
                    shutil.copy2(src, dst)
                    files_copied += 1
                except Exception as e:
                    print(f"⚠️  Falha ao copiar {src} para temp: {e}")

    if files_copied == 0:
        print("⚠️ Nenhum arquivo .gb/.gbk encontrado para processar em modo agregado.")
        try:
            os.rmdir(temp_input_dir)
        except OSError:
            pass
        return

    aggregate_genus_name = "ALL_GENERA"
    # Usa workers globais para modo agregado
    process_genus_folder(aggregate_genus_name, temp_input_dir, output_root, gene_dict, get_max_workers())

    # Renomeia arquivos gerados
    for ext in ["csv", "xlsx", "parquet"]:
        src = os.path.join(output_root, f"{aggregate_genus_name}_output_dm.{ext}")
        dst = os.path.join(output_root, f"output_dm.{ext}")
        try:
            if os.path.exists(src):
                if os.path.exists(dst):
                    os.remove(dst)
                shutil.move(src, dst)
        except Exception as e:
            print(f"❌ Erro ao mover arquivo {ext}: {e}")

    # Cleanup temp dir
    try:
        for f in os.listdir(temp_input_dir):
            os.remove(os.path.join(temp_input_dir, f))
        os.rmdir(temp_input_dir)
    except Exception:
        pass
    
    print(f"✅ Arquivos consolidados gerados em: {output_root}")


if __name__ == "__main__":
    # Mantém compatibilidade com execução direta
    gb_handle()

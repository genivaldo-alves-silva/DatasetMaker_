# =============================== VERSÃO SOAP API ===============================
# Versão refatorada usando API SOAP do Index Fungorum
# Substitui web scraping por chamadas estruturadas à API
# Mantém compatibilidade com o formato de saída JSON original
# Inclui verificação híbrida HTML para capturar Epitypes/Lectotypes adicionais

'''
Veja dsmaker.err: apareceu algo como 'requests.exceptions.HTTPError: 
404 Client Error: Not Found for url:
 https://www.indexfungorum.org/ixfwebservice/fungus.asmx?WSDL'

 Tenho que testar como ficou o acesso a LLM.
'''

import asyncio
import json
import os
import re
import sys
import requests
from zeep import Client
from zeep.exceptions import Fault

# Adiciona o diretório atual ao path para imports relativos funcionarem
_script_dir = os.path.dirname(os.path.abspath(__file__))
if _script_dir not in sys.path:
    sys.path.insert(0, _script_dir)

# Importa country_detector robusto (com geopy, pycountry e cache)
from country_detector import (
    detectar_pais, 
    carregar_cache_local, 
    salvar_cache_local
)

# Carrega cache de países ao iniciar
carregar_cache_local()

# Inicializa cliente SOAP (singleton)
WSDL_URLS = [
    'https://www.indexfungorum.org/ixfwebservice/fungus.asmx?WSDL',
    'https://indexfungorum.org/ixfwebservice/fungus.asmx?WSDL',
    'http://www.indexfungorum.org/ixfwebservice/fungus.asmx?WSDL',
    'http://indexfungorum.org/ixfwebservice/fungus.asmx?WSDL',
]
HTML_BASE_URL = 'https://www.indexfungorum.org/Names/NamesRecord.asp'
_client = None
_wsdl_in_use = None

def get_client():
    """Retorna cliente SOAP singleton."""
    global _client, _wsdl_in_use
    if _client is None:
        last_error = None
        for wsdl_url in WSDL_URLS:
            try:
                # Pré-check HTTP curto para evitar falha lenta dentro do zeep.
                r = requests.get(wsdl_url, timeout=10)
                if r.status_code != 200:
                    last_error = RuntimeError(f"WSDL status={r.status_code} em {wsdl_url}")
                    continue
                _client = Client(wsdl_url)
                _wsdl_in_use = wsdl_url
                break
            except Exception as exc:
                last_error = exc
                continue

        if _client is None:
            raise RuntimeError(f"Falha ao inicializar cliente SOAP do IF. Último erro: {last_error}")
    return _client


def xml_to_dict(xml_element):
    """Converte elemento XML para dicionário, limpando nomes de campos."""
    data = {}
    if xml_element is None:
        return data
    
    for child in xml_element:
        # Limpa nomes de campos (remove encoding XML)
        tag = child.tag
        tag = tag.replace("_x0020_", " ")
        tag = tag.replace("_x002F_", "/")
        tag = tag.replace("_x0026_", "&")
        data[tag] = child.text
    return data


def parse_typification(typification_raw):
    """
    Extrai tipos e materiais da string de tipificação.
    Suporta múltiplos tipos (Holotype, Epitype, etc.) na mesma string.
    
    Retorna:
        - types_array: lista de dicts com {"type_of_type": "Holotype", "material": ["LPS", "Puiggar 1438"]}
    """
    if not typification_raw:
        return []
    
    # Remove prefixo "Typification:" se existir
    typification = re.sub(r"^Typification:\s*", "", typification_raw, flags=re.I).strip()
    
    # Lista de tipos conhecidos
    type_names = ["Holotype", "Isotype", "Lectotype", "Neotype", "Epitype", "Paratype", "Topotype", "Syntype"]
    type_pattern = r"\b(" + "|".join(type_names) + r")\b"
    
    # Encontrar todos os tipos na string
    types_array = []
    matches = list(re.finditer(type_pattern, typification, re.IGNORECASE))
    
    if matches:
        for i, match in enumerate(matches):
            type_name = match.group(1).capitalize()
            start = match.end()
            
            # Encontra o fim (próximo tipo ou fim da string)
            if i + 1 < len(matches):
                end = matches[i + 1].start()
            else:
                end = len(typification)
            
            # Extrai o material entre este tipo e o próximo
            material_raw = typification[start:end].strip()
            # Remove pontuação inicial (:, ,)
            material_raw = re.sub(r"^[:\s,]+", "", material_raw).strip()
            # Remove parênteses de designação no final se houver (Designated by...)
            material_raw = re.sub(r"\s*\(Designated by.*$", "", material_raw, flags=re.I).strip()
            
            # Separa materiais por vírgula
            materials = [mat.strip() for mat in material_raw.split(",") if mat.strip()]
            
            types_array.append({
                "type_of_type": type_name,
                "material": materials
            })
    else:
        # Sem tipo identificado, retorna o material bruto
        materials = [mat.strip() for mat in typification.split(",") if mat.strip()]
        if materials:
            types_array.append({
                "type_of_type": "",
                "material": materials
            })
    
    return types_array


# ========================= VERIFICAÇÃO HTML (HÍBRIDA) =========================

TYPE_NAMES = ["Holotype", "Isotype", "Lectotype", "Neotype", "Epitype", "Paratype", "Topotype", "Syntype"]


def check_html_for_extra_types(record_id, api_types_count=0):
    """
    Faz uma verificação rápida do HTML para detectar tipos adicionais.
    Retorna (has_extra, html_content) onde has_extra indica se há mais tipos no HTML.
    """
    try:
        url = f"{HTML_BASE_URL}?RecordID={record_id}"
        response = requests.get(url, timeout=10)
        html = response.text
        
        # Contar tipos no HTML
        html_types_count = sum(1 for tipo in TYPE_NAMES if tipo in html)
        
        # Se HTML tem mais tipos que a API, retorna True e o HTML
        if html_types_count > api_types_count:
            return True, html
        return False, None
        
    except Exception as e:
        print(f"    ⚠️ Erro ao verificar HTML para {record_id}: {e}")
        return False, None


def extract_types_from_html(html_content):
    """
    Extrai tipos e materiais do HTML da página do Index Fungorum.
    Retorna um array de tipos similar ao parse_typification.
    """
    if not html_content:
        return []
    
    types_array = []
    
    # Encontrar seção de Typification Details no HTML
    # Padrão: <b>Typification Details: </b><br>Holotype ... <br>Epitype ...
    match = re.search(
        r'Typification Details[:\s]*</b>.*?<br>(.*?)(?:</p>|<b>Host)',
        html_content,
        re.IGNORECASE | re.DOTALL
    )
    
    if not match:
        return []
    
    typif_section = match.group(1)
    
    # Limpar HTML entities
    typif_section = typif_section.replace('&#243;', 'ó')
    typif_section = typif_section.replace('&#227;', 'ã')
    typif_section = typif_section.replace('&amp;', '&')
    typif_section = re.sub(r'<[^>]+>', ' ', typif_section)  # Remove tags HTML
    typif_section = re.sub(r'\s+', ' ', typif_section).strip()
    
    # Usar o mesmo parser para extrair tipos
    type_pattern = r"\b(" + "|".join(TYPE_NAMES) + r")\b"
    matches = list(re.finditer(type_pattern, typif_section, re.IGNORECASE))
    
    if matches:
        for i, match in enumerate(matches):
            type_name = match.group(1).capitalize()
            start = match.end()
            
            # Encontra o fim (próximo tipo ou fim da string)
            if i + 1 < len(matches):
                end = matches[i + 1].start()
            else:
                end = len(typif_section)
            
            # Extrai o material entre este tipo e o próximo
            material_raw = typif_section[start:end].strip()
            # Remove pontuação inicial
            material_raw = re.sub(r"^[:\s,]+", "", material_raw).strip()
            # Remove parênteses de designação (Designated by...) e tudo após
            material_raw = re.sub(r"\s*\(Designated by[^)]*\).*$", "", material_raw, flags=re.I).strip()
            # Remove Registration Identifier
            material_raw = re.sub(r"\s*Registration Identifier.*$", "", material_raw, flags=re.I).strip()
            # Remove referências de publicação residuais (ex: ": 769-790. 2020).")
            material_raw = re.sub(r"[:\s]*\d+[-–]\d+\.\s*\d{4}\)?\.?$", "", material_raw).strip()
            # Remove pontuação final
            material_raw = material_raw.rstrip('.,;:)')
            
            # Separa materiais por vírgula
            materials = [mat.strip() for mat in material_raw.split(",") if mat.strip()]
            
            if materials:
                types_array.append({
                    "type_of_type": type_name,
                    "material": materials
                })
    
    return types_array


def merge_types_arrays(api_types, html_types):
    """
    Mescla tipos da API com tipos do HTML, evitando duplicatas.
    Prioriza dados do HTML quando há conflito (mais completos).
    """
    if not html_types:
        return api_types
    
    if not api_types:
        return html_types
    
    # Criar set de tipos já presentes na API
    api_type_names = {t.get("type_of_type", "").lower() for t in api_types if t.get("type_of_type")}
    
    merged = list(api_types)  # Começa com os da API
    
    # Adiciona tipos do HTML que não estão na API
    for html_type in html_types:
        type_name = html_type.get("type_of_type", "").lower()
        if type_name and type_name not in api_type_names:
            merged.append(html_type)
    
    return merged


def get_species_list_from_if(genus, max_results=50000):
    """
    Obtém lista de espécies de um gênero usando a API SOAP do Index Fungorum.
    Substitui a chamada ao ChecklistBank.
    
    Args:
        genus: Nome do gênero a buscar
        max_results: Limite máximo de resultados (padrão: 50000, suficiente para 
                     os maiores gêneros como Cortinarius ~4000 ou Agaricus ~11000)
    
    Note:
        A API não tem paginação, então usamos um valor alto para garantir que
        todos os registros sejam retornados. Se o número de resultados igualar
        max_results, um aviso será exibido.
    """
    client = get_client()
    
    # Busca pelo nome do gênero
    result = client.service.NameSearch(
        SearchText=genus,
        AnywhereInText=False,
        MaxNumber=max_results
    )
    
    species_list = []
    
    if result is None:
        return species_list
    
    for record in result:
        data = xml_to_dict(record)
        
        # Filtra espécies, formas e variedades (não gêneros ou subespécies)
        rank = data.get('INFRASPECIFIC RANK', '').strip()
        name_status = data.get('NAME STATUS', '').strip()
        current_name_id = data.get('CURRENT NAME RECORD NUMBER', '')
        record_id = data.get('RECORD NUMBER', '')
        current_name = data.get('CURRENT NAME', '').strip()
        
        # Ranks aceitos: sp., f. (forma), var. (variedade)
        valid_ranks = ['sp.', 'f.', 'var.']
        
        # Status aceitos: Legitimate, Orthographic variant, Invalid
        valid_statuses = ['Legitimate', 'Orthographic variant', 'Invalid']
        
        if rank in valid_ranks and name_status in valid_statuses:
            # Determina o status taxonômico
            if name_status == 'Invalid':
                tax_status = 'invalid'
            elif name_status == 'Orthographic variant':
                tax_status = 'orthographic_variant'
            elif current_name_id == record_id:
                tax_status = 'accepted'
            elif current_name:
                tax_status = 'synonym'
            else:
                tax_status = 'unresolved'
            
            species_list.append({
                'id': record_id,
                'scientificName': data.get('NAME OF FUNGUS', ''),
                'authorship': data.get('AUTHORS', ''),
                'rank': rank,
                'name_status': name_status,
                'status': tax_status,
                'current_name': current_name if tax_status in ['synonym', 'orthographic_variant'] else '',
                'current_name_id': current_name_id if tax_status in ['synonym', 'orthographic_variant'] else ''
            })
    
    # Aviso se o limite foi atingido (pode haver mais registros)
    if len(species_list) >= max_results:
        print(f"    ⚠️ AVISO: Limite de {max_results} registros atingido para '{genus}'!")
        print(f"       Pode haver mais espécies. Considere aumentar max_results.")
    
    return species_list


def get_species_details(record_id):
    """
    Obtém detalhes completos de uma espécie pelo ID usando NameByKey.
    Retorna dicionário com todos os campos relevantes.
    """
    client = get_client()
    
    try:
        result = client.service.NameByKey(NameKey=int(record_id))
    except (Fault, Exception) as e:
        print(f"  ⚠️ Erro ao buscar ID {record_id}: {e}")
        return None
    
    if result is None:
        return None
    
    # A API retorna um DataSet com um registro
    for record in result:
        return xml_to_dict(record)
    
    return None


def extract_species_data(record_id, data, check_html=True, name_status=None):
    """
    Extrai e formata dados de uma espécie a partir do dicionário da API.
    Mantém formato compatível com o JSON original.
    
    Args:
        record_id: ID do registro no Index Fungorum
        data: Dicionário com dados da API SOAP
        check_html: Se True, verifica HTML para tipos adicionais (Epitype, etc.)
        name_status: Status do nome (Legitimate, Orthographic variant, Invalid)
    """
    species = data.get('NAME OF FUNGUS', '')
    author = data.get('AUTHORS', '')
    
    # Se não foi passado, tenta obter do data
    if name_status is None:
        name_status = data.get('NAME STATUS', '').strip()
    
    # Monta referência do protólogo
    pub_abbr = data.get('pubIMIAbbr', '')
    volume = data.get('VOLUME', '')
    part = data.get('PART', '')
    page = data.get('PAGE', '')
    year = data.get('YEAR OF PUBLICATION', '')
    
    protologo_parts = []
    if pub_abbr:
        protologo_parts.append(pub_abbr)
    if volume:
        vol_str = volume
        if part:
            vol_str += f" ({part})"
        protologo_parts.append(vol_str)
    if page:
        protologo_parts.append(f": {page}")
    if year:
        protologo_parts.append(f"({year})")
    
    protologo = ' '.join(protologo_parts) if protologo_parts else ""
    
    # Tipificação da API
    typification_raw = data.get('TYPIFICATION DETAILS', '')
    types_array = parse_typification(typification_raw)
    
    # Verificação HTML para tipos adicionais (Epitype, Lectotype, etc.)
    html_checked = False
    if check_html and types_array:
        has_extra, html_content = check_html_for_extra_types(record_id, len(types_array))
        if has_extra:
            html_types = extract_types_from_html(html_content)
            if html_types:
                types_array = merge_types_arrays(types_array, html_types)
                html_checked = True
    
    # Localidade
    location_raw = data.get('LOCATION', '')
    country = detectar_pais(location_raw)
    
    # Host (hospedeiro)
    host = data.get('HOST', '')
    if host:
        # Remove tags HTML do host
        host = re.sub(r'<[^>]+>', '', host)
    
    # Basionym
    basionym_id = data.get('BASIONYM RECORD NUMBER', '')
    
    # Status taxonômico (do SOAP)
    current_name = data.get('CURRENT NAME', '').strip()
    current_name_id = data.get('CURRENT NAME RECORD NUMBER', '').strip()
    
    # Rank infraespecífico
    rank = data.get('INFRASPECIFIC RANK', '').strip()
    
    # Determina status taxonômico
    if name_status == 'Invalid':
        taxonomic_status = 'invalid'
    elif name_status == 'Orthographic variant':
        taxonomic_status = 'orthographic_variant'
    elif current_name_id == record_id:
        taxonomic_status = 'accepted'
    elif current_name:
        taxonomic_status = 'synonym'
    else:
        taxonomic_status = 'unresolved'
    
    result = {
        "species": species,
        "author": author,
        "protologo record0": protologo,
        "Basionym": "",
        "Basionym_author": "",
        "protologo basionym": "",
        "basionym recordID": basionym_id if basionym_id != record_id else "",
        "typification": typification_raw,
        "types": types_array,  # Array de tipos [{"type_of_type": "Holotype", "material": [...]}]
        "locality raw": location_raw,
        "country": country,
        "host": host,
        # Campos extras da API SOAP
        "_rank": rank,
        "_name_status": name_status,
        "_taxonomic_status": taxonomic_status,
        "_current_name": current_name if taxonomic_status in ['synonym', 'orthographic_variant'] else '',
        "_current_name_id": current_name_id if taxonomic_status in ['synonym', 'orthographic_variant'] else '',
        "_family": data.get('Family name', ''),
        "_order": data.get('Order name', ''),
    }
    
    # Marca se tipos extras foram encontrados no HTML
    if html_checked:
        result["_html_types_added"] = True
    
    return result


def extract_basionym_data(basionym_id, data, check_html=True):
    """
    Extrai dados do basiônimo para herdar tipificação.
    Também verifica HTML para tipos adicionais.
    """
    species = data.get('NAME OF FUNGUS', '')
    author = data.get('AUTHORS', '')
    
    # Monta referência do protólogo do basiônimo
    pub_abbr = data.get('pubIMIAbbr', '')
    volume = data.get('VOLUME', '')
    part = data.get('PART', '')
    page = data.get('PAGE', '')
    year = data.get('YEAR OF PUBLICATION', '')
    
    protologo_parts = []
    if pub_abbr:
        protologo_parts.append(pub_abbr)
    if volume:
        vol_str = volume
        if part:
            vol_str += f" ({part})"
        protologo_parts.append(vol_str)
    if page:
        protologo_parts.append(f": {page}")
    if year:
        protologo_parts.append(f"({year})")
    
    protologo = ' '.join(protologo_parts) if protologo_parts else ""
    
    # Tipificação da API
    typification_raw = data.get('TYPIFICATION DETAILS', '')
    types_array = parse_typification(typification_raw)
    
    # Verificação HTML para tipos adicionais
    if check_html and types_array:
        has_extra, html_content = check_html_for_extra_types(basionym_id, len(types_array))
        if has_extra:
            html_types = extract_types_from_html(html_content)
            if html_types:
                types_array = merge_types_arrays(types_array, html_types)
    
    # Localidade
    location_raw = data.get('LOCATION', '')
    country = detectar_pais(location_raw)
    
    # Host
    host = data.get('HOST', '')
    if host:
        host = re.sub(r'<[^>]+>', '', host)
    
    return {
        "species": species,
        "author": author,
        "protologo basionym": protologo,
        "typification": typification_raw,
        "types": types_array,
        "locality raw": location_raw,
        "country": country,
        "host": host,
    }


async def collect_basionyms(species_data):
    """
    Coleta dados dos basiônimos que ainda não foram processados.
    """
    basionym_data = {}
    
    for record_id, dados in species_data.items():
        basionym_id = dados.get("basionym recordID", "").strip()
        
        # Se tem basiônimo diferente e ainda não foi coletado
        if basionym_id and basionym_id != record_id and basionym_id not in basionym_data:
            print(f"  Coletando basiônimo {basionym_id} para {record_id}...")
            
            data = get_species_details(basionym_id)
            if data:
                basionym_data[basionym_id] = extract_basionym_data(basionym_id, data)
    
    return basionym_data


def fill_from_basionym(species_data, basionym_data):
    """
    Preenche campos vazios da espécie com dados do basiônimo.
    """
    for record_id, dados in species_data.items():
        basionym_id = dados.get("basionym recordID", "").strip()
        
        if basionym_id and basionym_id in basionym_data:
            base = basionym_data[basionym_id]
            
            # Herda campos se estiverem vazios
            for campo in ["typification", "types", "type_of_type", "type_material", "locality raw", "country", "host"]:
                if not dados.get(campo):
                    dados[campo] = base.get(campo, "")
            
            dados["herdado_do_basionym"] = True
            dados["Basionym"] = base.get("species", "")
            dados["Basionym_author"] = base.get("author", "")
            dados["protologo basionym"] = base.get("protologo basionym", "")


async def process_genus(genus):
    """
    Processa todas as espécies de um gênero.
    Função principal compatível com o script original.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    types_dict_dir = os.path.join(script_dir, "types_dict")
    json_filepath = os.path.join(types_dict_dir, f"{genus}_soap.json")
    
    existing_data = {}
    existing_ids = set()
    
    # Carrega dados existentes se houver
    if os.path.exists(json_filepath):
        print(f"📂 Arquivo '{json_filepath}' encontrado. Carregando dados existentes...")
        with open(json_filepath, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
            existing_ids = set(existing_data.keys())
    
    print(f"🔍 Buscando espécies do gênero {genus} via API SOAP...")
    species_list = get_species_list_from_if(genus)
    
    # Filtra espécies já processadas
    species_to_process = [sp for sp in species_list if str(sp['id']) not in existing_ids]
    
    if not species_to_process:
        print(f"✅ Nenhuma nova espécie para processar. Base de dados atualizada.")
        print(f"   Total de espécies no arquivo: {len(existing_data)}")
        return
    
    print(f"📊 Encontradas {len(species_to_process)} novas espécies para processar.")
    
    new_species_data = {}
    
    for sp in species_to_process:
        record_id = str(sp["id"])
        print(f"  Coletando {sp['scientificName']} ({record_id})...")
        
        data = get_species_details(record_id)
        if data:
            extracted = extract_species_data(record_id, data)
            new_species_data[record_id] = extracted
    
    # Coleta basiônimos
    if new_species_data:
        print("\n🔗 Coletando dados dos basiônimos...")
        basionym_data = await collect_basionyms(new_species_data)
        fill_from_basionym(new_species_data, basionym_data)
    
    # Merge com dados existentes
    updated_data = {**existing_data, **new_species_data}
    
    # Salva arquivo
    os.makedirs(types_dict_dir, exist_ok=True)
    with open(json_filepath, "w", encoding="utf-8") as f:
        json.dump(updated_data, f, indent=2, ensure_ascii=False)
    
    # Salva cache de países
    salvar_cache_local()
    
    print(f"\n✅ Total final para '{genus}': {len(updated_data)} registros.")
    print(f"📁 Arquivo salvo em: {json_filepath}")


async def process_all_genuses(genera_list=None):
    """
    Processa todos os gêneros fornecidos ou detecta automaticamente 
    a partir dos datasets existentes no GenBank output.
    
    Args:
        genera_list: Lista de gêneros a processar. Se None, detecta automaticamente
                     a partir das pastas em genbank_inout/genbank_out/
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    if genera_list is None:
        # Detecta gêneros a partir das pastas de saída do GenBank
        genbank_out_dir = os.path.join(script_dir, "..", "genbank_inout", "genbank_out")
        if os.path.exists(genbank_out_dir):
            genera_list = [
                d for d in os.listdir(genbank_out_dir) 
                if os.path.isdir(os.path.join(genbank_out_dir, d)) and not d.startswith("_")
            ]
        else:
            print(f"⚠️ Diretório de saída do GenBank não encontrado: {genbank_out_dir}")
            genera_list = []
    
    if not genera_list:
        print("⚠️ Nenhum gênero para processar.")
        return
    
    print(f"📊 Processando {len(genera_list)} gêneros: {', '.join(genera_list)}")
    
    for genus in genera_list:
        print(f"\n{'='*60}")
        print(f"  Processando gênero: {genus}")
        print(f"{'='*60}")
        await process_genus(genus)
    
    print(f"\n🎉 Processamento de todos os gêneros concluído!")


def test_api_connection():
    """Testa conexão com a API."""
    client = get_client()
    try:
        is_alive = client.service.IsAlive()
        print(f"🟢 API Index Fungorum: {'Online' if is_alive else 'Offline'}")
        return is_alive
    except Exception as e:
        print(f"🔴 Erro ao conectar com API: {e}")
        return False


# ========================= MAIN =========================
if __name__ == "__main__":
    import sys
    
    print("=" * 60)
    print("  Index Fungorum - Type Data Collector (SOAP API Version)")
    print("=" * 60)
    
    # Testa conexão
    if not test_api_connection():
        sys.exit(1)
    
    # Se passou um gênero como argumento
    if len(sys.argv) > 1:
        genus = sys.argv[1]
        asyncio.run(process_genus(genus))
    else:
        # Modo interativo
        print("\nUso: python IFget_types_soap.py <Genero>")
        print("Exemplo: python IFget_types_soap.py Echinodontium")
        print("\nOu importe e use:")
        print("  from IFget_types_soap import process_genus")
        print("  import asyncio")
        print("  asyncio.run(process_genus('Echinodontium'))")

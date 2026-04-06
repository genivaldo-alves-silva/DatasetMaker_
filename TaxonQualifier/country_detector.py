import json
import pycountry
import unicodedata
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut
import os
import time
import re

# Variáveis globais (carregadas uma vez)
lookup_por_variacao = {}
agrupado_por_pais = {}
lista_pycountry = {}
geolocator = Nominatim(user_agent="country_detector")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, swallow_exceptions=True)
cache_local_para_pais = {}

# Caminho absoluto para o cache (dentro de TaxonQualifier)
_script_dir = os.path.dirname(os.path.abspath(__file__))
CAMINHO_CACHE_PAISES = os.path.join(_script_dir, "cache_local_para_pais.json")

def carregar_cache_local():
    global cache_local_para_pais
    if os.path.exists(CAMINHO_CACHE_PAISES):
        with open(CAMINHO_CACHE_PAISES, "r", encoding="utf-8") as f:
            cache_local_para_pais = json.load(f)

def salvar_cache_local():
    with open(CAMINHO_CACHE_PAISES, "w", encoding="utf-8") as f:
        json.dump(cache_local_para_pais, f, ensure_ascii=False, indent=2)

# Remove acentos e normaliza
def normalizar_texto(texto):
    if not texto:
        return ""
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').lower()

def contem_apenas_ascii(texto):
    return all(ord(c) < 128 for c in texto)

# Carrega JSON de países + gera lookup
def carregar_json_inteligente(caminho_arquivo="countries_json.json"):
    global agrupado_por_pais, lookup_por_variacao

    if not os.path.exists(caminho_arquivo):
        agrupado_por_pais, lookup_por_variacao = {}, {}
        return

    with open(caminho_arquivo, "r", encoding="utf-8") as f:
        dados = json.load(f)

    agrupado_por_pais = {}
    lookup_por_variacao = {}

    for pais_oficial, variacoes in dados.items():
        variacoes_norm = []
        for var in variacoes:
            if contem_apenas_ascii(var):
                var_norm = normalizar_texto(var)
            else:
                var_norm = var.strip().lower()
            variacoes_norm.append(var_norm)
            lookup_por_variacao[var_norm] = pais_oficial
        
        agrupado_por_pais[pais_oficial] = variacoes_norm

def gerar_lista_paises_pycountry():
    global lista_pycountry
    lista_pycountry = {normalizar_texto(country.name): country.name for country in pycountry.countries}

def detectar_paises_multiplos_com_geopy(localidade, limite=5):
    try:
        locations = geocode(localidade, exactly_one=False, limit=limite)
        if not locations:
            return []

        paises = set()
        for loc in locations:
            raw = loc.raw
            # Filtros para evitar ruído
            if raw.get('class') == 'boundary' and raw.get('type') in ('administrative', 'country', 'state'):
                partes = raw.get('display_name', '').split(',')
                if partes:
                    pais = partes[-1].strip()
                    if pais:
                        paises.add(pais)
        return list(paises)
    except GeocoderTimedOut:
        return []

def detectar_pais_simples(localidade):
    if not localidade:
        return ""

    # Primeiro tenta match direto com texto original (para caracteres não-ASCII como 日本)
    local_strip = localidade.strip().lower()
    if local_strip in lookup_por_variacao:
        return lookup_por_variacao[local_strip]

    # Normaliza para ASCII
    local_norm = normalizar_texto(localidade).replace(",", " ").strip()
    
    # Se normalização resultou em string vazia (texto era todo não-ASCII), já retorna
    if not local_norm:
        return ""
    
    # Tenta match exato com a localidade completa normalizada
    if local_norm in lookup_por_variacao:
        return lookup_por_variacao[local_norm]
    
    # Depois tenta bigramas (duas palavras consecutivas)
    palavras = local_norm.split()
    for i in range(len(palavras) - 1):
        bigrama = f"{palavras[i]} {palavras[i+1]}"
        if bigrama in lookup_por_variacao:
            return lookup_por_variacao[bigrama]
    
    # Depois tenta palavras individuais
    for palavra in palavras:
        if palavra in lookup_por_variacao:
            return lookup_por_variacao[palavra]
        if palavra in lista_pycountry:
            return lista_pycountry[palavra]
    return ""

def detectar_pais_com_geopy(localidade):
    try:
        location = geocode(localidade)
        if location:
            if location.raw and 'display_name' in location.raw:
                partes = location.raw['display_name'].split(',')
                if partes:
                    pais = partes[-1].strip()
                    return pais
        return ""
    except GeocoderTimedOut:
        return ""

def revisar_nome_detectado(nome_detectado):
    if not nome_detectado:
        return ""
    
    nome_detectado_strip = nome_detectado.strip()
    if nome_detectado_strip in lookup_por_variacao:
        return lookup_por_variacao[nome_detectado_strip]
    if nome_detectado_strip in lista_pycountry.values():
        return nome_detectado_strip

    nome_norm = normalizar_texto(nome_detectado)
    if nome_norm in lookup_por_variacao:
        return lookup_por_variacao[nome_norm]
    if nome_norm in lista_pycountry:
        return lista_pycountry[nome_norm]

    return nome_detectado

'''
# Função MÁGICA final para importar e usar direto
def detectar_pais(localidade):
    if not lookup_por_variacao or not lista_pycountry:
        carregar_json_inteligente()
        gerar_lista_paises_pycountry()

    pais = detectar_pais_simples(localidade)
    if not pais:
        pais = detectar_pais_com_geopy(localidade)

    pais = revisar_nome_detectado(pais)

    if not pais:
        # Tentar obter múltiplos países possíveis
        possiveis_paises = detectar_paises_multiplos_com_geopy(localidade)
        revisados = [revisar_nome_detectado(p) for p in possiveis_paises if p]
        revisados_unicos = list(set(filter(None, revisados)))

        if len(revisados_unicos) == 1:
            return revisados_unicos[0]
        elif len(revisados_unicos) > 1:
            return revisados_unicos  # retorna lista se múltiplos candidatos forem encontrados

    return pais
'''

def detectar_pais(localidade):
    if not localidade:
        return ""

    localidade = re.split(r"(Page|Position|Citations|Published)", localidade, flags=re.IGNORECASE)[0].strip()

    if not lookup_por_variacao or not lista_pycountry:
        carregar_json_inteligente()
        gerar_lista_paises_pycountry()

    pais = detectar_pais_simples(localidade)
    
    local_norm = normalizar_texto(localidade)

    # Verifica se local já está em cache
    if local_norm in cache_local_para_pais:
        return cache_local_para_pais[local_norm]

    # Detecta país com geopy se necessário
    if not pais:
        pais = detectar_pais_com_geopy(localidade)

    pais = revisar_nome_detectado(pais)

    if not pais:
        possiveis_paises = detectar_paises_multiplos_com_geopy(localidade)
        revisados = [revisar_nome_detectado(p) for p in possiveis_paises if p]
        revisados_unicos = list(set(filter(None, revisados)))

        if len(revisados_unicos) == 1:
            pais = revisados_unicos[0]
        elif len(revisados_unicos) > 1:
            pais = revisados_unicos

    # Salva no cache SOMENTE se país foi identificado (não vazio)
    if pais:
        cache_local_para_pais[local_norm] = pais

    return pais
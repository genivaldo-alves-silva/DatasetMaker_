############# do gemini com a verificação 
# por gênero para a buscar limitada a data de release - APRIMORADO
# com argv para chamar o input python3 script.py ./genbank_in/
# Melhorado: estrutura de pastas gb/, detecção de data de .gb existentes, --force-run

import os
import re
import time
import argparse
import requests
from datetime import datetime
from Bio import SeqIO
from Bio.Blast import NCBIWWW, NCBIXML
from dotenv import load_dotenv

# 🗝️ Carregando variáveis de ambiente do arquivo .env
script_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(script_dir, '..', '.env')
load_dotenv(dotenv_path)

api_key = os.getenv("NCBI_API_KEY")
if not api_key:
    raise ValueError("❌ NCBI_API_KEY não encontrada no arquivo .env")

# Definir a API key como variável de ambiente (necessário para qblast)
os.environ["NCBI_API_KEY"] = api_key

###################### Funções de Parsing ######################

def parse_fasta(file_path):
    """Lê um arquivo FASTA e retorna um dicionário de espécimes e sequências."""
    specimens = {}
    try:
        for record in SeqIO.parse(file_path, "fasta"):
            specimen_id = record.id
            sequence = str(record.seq)
            specimens[specimen_id] = sequence
    except Exception as e:
        print(f"❌ Erro ao ler arquivo FASTA {file_path}: {e}")
        return None
    return specimens

def parse_txt(file_path):
    """Lê um arquivo de texto e retorna uma lista de gêneros."""
    genera = []
    try:
        with open(file_path, "r") as f:
            for line in f:
                genus = line.strip()
                if genus:
                    genera.append(genus)
    except Exception as e:
        print(f"❌ Erro ao ler arquivo TXT {file_path}: {e}")
        return None
    return genera

def get_inputs(input_dir):
    """
    Busca por arquivos de input (.fas ou .txt) no diretório especificado
    e os parseia de acordo com o tipo.
    """
    inputs = {}
    for fname in os.listdir(input_dir):
        if fname.startswith("input_") and (fname.endswith(".fas") or fname.endswith(".txt")):
            fpath = os.path.join(input_dir, fname)
            if fname.endswith(".fas"):
                data = parse_fasta(fpath)
                if data is not None:
                    inputs[fname] = {"type": "fasta", "data": data}
            elif fname.endswith(".txt"):
                data = parse_txt(fpath)
                if data is not None:
                    inputs[fname] = {"type": "txt", "data": data}
    return inputs

###################### Funções de BLAST e GenBank ######################

def run_blast_and_extract(sequence, hitlist_size=100, min_query_coverage=85, min_identity=90):
    """
    Executa BLAST para uma sequência e retorna:
      - all_ids: IDs dos 100 primeiros hits (sem filtro)
      - top3: lista com até 3 hits filtrados (dicts com genus, query_cov, identity, accession)
    """
    print("🔍 Rodando BLAST...")
    try:
        result_handle = NCBIWWW.qblast(
            program="blastn",
            database="nr",
            sequence=sequence,
            hitlist_size=hitlist_size
        )
        blast_records = NCBIXML.parse(result_handle)
        blast_record = next(blast_records)

        all_ids = []
        filtered_hits = []

        if blast_record.alignments:
            for alignment in blast_record.alignments:
                all_ids.append(alignment.accession)

                hsp = alignment.hsps[0]
                query_cov = (hsp.query_end - hsp.query_start + 1) / len(sequence) * 100
                id_percent = (hsp.identities / hsp.align_length) * 100

                if query_cov >= min_query_coverage and id_percent >= min_identity:
                    title = alignment.title
                    genus = title.split()[1] if " " in title else "NA"
                    filtered_hits.append({
                        "genus": genus,
                        "query_cov": round(query_cov, 2),
                        "identity": round(id_percent, 2),
                        "accession": alignment.accession
                    })

        filtered_hits = sorted(filtered_hits, key=lambda x: x["identity"], reverse=True)
        top3 = filtered_hits[:3]

        return all_ids, top3

    except Exception as e:
        print(f"❌ Erro no BLAST: {e}")
        return [], []

def fetch_genbank_records(ids, api_key, batch_size=200):
    """
    Baixa registros de nucleotídeos em formato GenBank a partir de uma lista de IDs.
    """
    efetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    all_records = ""
    
    for i in range(0, len(ids), batch_size):
        batch_ids = ",".join(ids[i:i + batch_size])
        fetch_params = {
            "db": "nucleotide",
            "id": batch_ids,
            "rettype": "gb",
            "retmode": "text",
            "api_key": api_key
        }
        print(f"📥 Baixando registros {i+1} a {i+len(batch_ids.split(','))}...")
        try:
            response = requests.get(efetch_url, params=fetch_params)
            response.raise_for_status()
            all_records += response.text
            time.sleep(0.2)
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao baixar lote de registros: {e}")
            break
            
    return all_records

def get_gb_folder(genus_folder):
    """
    Retorna o caminho da pasta gb/ dentro da pasta do gênero.
    Cria a pasta se não existir.
    """
    gb_folder = os.path.join(genus_folder, "gb")
    os.makedirs(gb_folder, exist_ok=True)
    return gb_folder


def get_last_gb_date(genus_folder):
    """
    Detecta a data mais recente a partir dos arquivos .gb existentes na pasta gb/.
    Procura por arquivos no formato: Genus_all_genbank_DDMMMYYYY.gb
    Retorna a data no formato YYYY/MM/DD ou None se não encontrar.
    """
    gb_folder = os.path.join(genus_folder, "gb")
    if not os.path.exists(gb_folder):
        return None
    
    # Padrão: Genus_all_genbank_22feb2026.gb ou Genus_blast_top100_hits_22feb2026.gb
    pattern = re.compile(r'_(?:all_genbank|blast_top100_hits)_(\d{1,2}[a-z]{3}\d{4})\.gb$', re.IGNORECASE)
    
    latest_date = None
    for fname in os.listdir(gb_folder):
        match = pattern.search(fname)
        if match:
            date_str = match.group(1)
            try:
                parsed_date = datetime.strptime(date_str, "%d%b%Y")
                if latest_date is None or parsed_date > latest_date:
                    latest_date = parsed_date
            except ValueError:
                continue
    
    if latest_date:
        return latest_date.strftime("%Y/%m/%d")
    return None


def get_last_log_date(genus_folder):
    """
    DEPRECATED: Use get_last_gb_date() instead.
    Mantida para compatibilidade. Lê o arquivo de log do gênero.
    """
    log_path = os.path.join(genus_folder, f"{os.path.basename(genus_folder)}.log")
    if not os.path.exists(log_path):
        return None
    
    with open(log_path, "r") as f:
        lines = f.readlines()
        for line in reversed(lines):
            if line.strip().startswith("input_"):
                parts = line.split("_")
                date_part = parts[1].split(".")[0]
                try:
                    return datetime.strptime(date_part, "%d%b%Y").strftime("%Y/%m/%d")
                except ValueError:
                    continue
    return None

def fetch_genbank_by_genus(genus, api_key, max_length=5000, last_date=None, batch_size=200, force_run=False):
    """
    Busca todas as sequências no GenBank para um gênero, opcionalmente a partir de uma data.
    
    Args:
        genus: Nome do gênero a buscar
        api_key: Chave da API do NCBI
        max_length: Tamanho máximo das sequências (default: 5000)
        last_date: Data no formato YYYY/MM/DD para filtrar releases posteriores
        batch_size: Tamanho do lote para download (default: 200)
        force_run: Se True, ignora last_date e baixa todas as sequências
    """
    search_term = f"{genus}[Organism] AND 1:{max_length}[SLEN] NOT whole genome[Title]"
    
    if force_run:
        print(f"  ⚡ --force-run ativo: baixando TODAS as sequências de {genus}")
    elif last_date:
        # Adiciona o filtro de data: sequências liberadas após a última data
        search_term += f" AND {last_date}[pdat]:9999[pdat]"
        print(f"  📅 Filtro de data: sequências liberadas após {last_date}")

    search_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    search_params = {
        "db": "nucleotide",
        "term": search_term,
        "retmax": 100000,
        "retmode": "json",
        "api_key": api_key
    }
    
    try:
        search_response = requests.get(search_url, params=search_params)
        search_response.raise_for_status()
        id_list = search_response.json()["esearchresult"]["idlist"]
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro na busca por gênero {genus}: {e}")
        return ""
    
    if not id_list:
        print(f"⚠️ Nenhum registro encontrado para {genus}.")
        return ""

    efetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    all_records = ""

    for i in range(0, len(id_list), batch_size):
        batch_ids = ",".join(id_list[i:i + batch_size])
        fetch_params = {
            "db": "nucleotide",
            "id": batch_ids,
            "rettype": "gb",
            "retmode": "text",
            "api_key": api_key
        }
        try:
            response = requests.get(efetch_url, params=fetch_params)
            response.raise_for_status()
            all_records += response.text
            time.sleep(0.2)
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao baixar registros para {genus}: {e}")
            break

    return all_records

###################### Funções de Gerenciamento de Arquivos ######################

def update_genus_log(genus_folder, fname, specimens, top3_hits, output_files):
    """
    Adiciona ou atualiza o log de um gênero específico.
    """
    log_path = os.path.join(genus_folder, f"{os.path.basename(genus_folder)}.log")
    with open(log_path, "a") as f:
        f.write(f"\ninput: {fname} [{', '.join(specimens)}]\n\n")
        f.write("🎯 Top 3 gêneros com maior identidade (query ≥ 85% e ID ≥ 90%):\n")
        f.write("Genus\tQueryCov(%)\tIdentity(%)\n")
        for hit in top3_hits:
            f.write(f"{hit['genus']}\t{hit['query_cov']}\t{hit['identity']}\n")
        for file in output_files:
            f.write(f"\n{os.path.basename(file)} salvo;")

def update_general_log(log_path, log_entries):
    """
    Adiciona novas entradas ao log geral em formato CSV.
    Cria o arquivo com cabeçalho se ele não existir.
    """
    file_exists = os.path.exists(log_path)
    with open(log_path, "a") as f:
        if not file_exists:
            f.write("input;date;specimen;top_3;genus_folder\n")
        for entry in log_entries:
            f.write(f"{entry}\n")

def process_fasta_input(input_fname, input_data, output_dir, force_run=False):
    """
    Processa um arquivo de entrada FASTA, executa BLAST e salva resultados.
    
    Args:
        input_fname: Nome do arquivo de entrada
        input_data: Dicionário com espécimes e sequências
        output_dir: Diretório de saída
        force_run: Se True, ignora datas anteriores e baixa tudo
    """
    date_str = datetime.now().strftime("%d%b%Y").lower()
    
    # Agrupar espécimes por top 3 gêneros
    genus_groups = {}
    general_log_entries = []

    for specimen, seq in input_data.items():
        print(f"  🧬 Espécime: {specimen} (len={len(seq)})")
        all_ids, top3 = run_blast_and_extract(seq)
        
        top_genus = top3[0]["genus"] if top3 else "NA"
        if top_genus not in genus_groups:
            genus_groups[top_genus] = {"specimens": [], "top3": top3, "all_ids": all_ids}
        genus_groups[top_genus]["specimens"].append(specimen)
        
        top3_str = "|".join([hit["genus"] for hit in top3])
        general_log_entries.append(f"{input_fname};{date_str};{specimen};{top3_str};{top_genus}")
    
    # Salvar resultados e logs para cada grupo de gênero
    for genus, group_data in genus_groups.items():
        genus_folder = os.path.join(output_dir, genus)
        os.makedirs(genus_folder, exist_ok=True)
        gb_folder = get_gb_folder(genus_folder)
        
        # Salvar hits do BLAST na pasta gb/
        blast_hits_fname = f"{genus}_blast_top100_hits_{date_str}.gb"
        blast_hits_fpath = os.path.join(gb_folder, blast_hits_fname)
        if group_data["all_ids"]:
            # Usando a nova função para baixar os registros top100
            genbank_data = fetch_genbank_records(group_data["all_ids"][:100], api_key)
            if genbank_data:
                with open(blast_hits_fpath, "w") as f:
                    f.write(genbank_data)
                print(f"  💾 Salvo: {blast_hits_fpath}")
        
        # Detectar última data dos arquivos .gb existentes
        last_date = None if force_run else get_last_gb_date(genus_folder)
        
        # Buscar e salvar todas as sequências do gênero na pasta gb/
        all_genbank_fname = f"{genus}_all_genbank_{date_str}.gb"
        all_genbank_fpath = os.path.join(gb_folder, all_genbank_fname)
        all_genbank_data = fetch_genbank_by_genus(genus, api_key, last_date=last_date, force_run=force_run)
        if all_genbank_data:
            with open(all_genbank_fpath, "w") as f:
                f.write(all_genbank_data)
            print(f"  💾 Salvo: {all_genbank_fpath}")
        else:
            print(f"  ℹ️  Nenhuma sequência nova encontrada para {genus}")
        
        output_files = [blast_hits_fpath, all_genbank_fpath]
        update_genus_log(genus_folder, input_fname, group_data["specimens"], group_data["top3"], output_files)

    return general_log_entries

def process_txt_input(input_fname, input_data, output_dir, force_run=False):
    """
    Processa um arquivo de entrada TXT, busca por gêneros no GenBank e salva resultados.
    
    Args:
        input_fname: Nome do arquivo de entrada
        input_data: Lista de gêneros
        output_dir: Diretório de saída
        force_run: Se True, ignora datas anteriores e baixa tudo
    """
    date_str = datetime.now().strftime("%d%b%Y").lower()
    general_log_entries = []

    for genus in input_data:
        print(f"\n  🔬 Processando gênero: {genus}")
        genus_folder = os.path.join(output_dir, genus)
        os.makedirs(genus_folder, exist_ok=True)
        gb_folder = get_gb_folder(genus_folder)
        
        # Detectar última data dos arquivos .gb existentes
        last_date = None if force_run else get_last_gb_date(genus_folder)
        if last_date and not force_run:
            print(f"  📅 Última atualização detectada: {last_date}")
        
        # Buscar e salvar todas as sequências do gênero na pasta gb/
        all_genbank_fname = f"{genus}_all_genbank_{date_str}.gb"
        all_genbank_fpath = os.path.join(gb_folder, all_genbank_fname)
        all_genbank_data = fetch_genbank_by_genus(genus, api_key, last_date=last_date, force_run=force_run)
        
        if all_genbank_data:
            with open(all_genbank_fpath, "w") as f:
                f.write(all_genbank_data)
            print(f"  💾 Salvo: {all_genbank_fpath}")
            status = "novo" if not last_date else "atualizado"
        else:
            print(f"  ℹ️  Nenhuma sequência nova encontrada para {genus}")
            status = "sem_novidades"

        # Atualizar log de gênero
        log_path = os.path.join(genus_folder, f"{genus}.log")
        with open(log_path, "a") as f:
            f.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]\n")
            f.write(f"input: {input_fname} [busca por gênero]\n")
            f.write(f"force_run: {force_run}\n")
            f.write(f"last_date_filter: {last_date}\n")
            f.write(f"status: {status}\n")
            if all_genbank_data:
                f.write(f"output: {all_genbank_fname}\n")

        general_log_entries.append(f"{input_fname};{date_str};NA;NA;{genus};{status}")
    
    return general_log_entries


def process_txt_input_complex(input_fname, genera_list, output_dir, complex_name, force_run=False):
    """
    Processa múltiplos gêneros como um único complexo de espécies.
    Todos os arquivos .gb são salvos em uma única pasta combinada.
    
    Args:
        input_fname: Nome do arquivo de entrada
        genera_list: Lista de gêneros a combinar
        output_dir: Diretório de saída
        complex_name: Nome da pasta do complexo
        force_run: Se True, ignora datas anteriores e baixa tudo
    
    Returns:
        Lista de entradas para o log geral
    """
    date_str = datetime.now().strftime("%d%b%Y").lower()
    general_log_entries = []
    
    # Cria a pasta do complexo
    complex_folder = os.path.join(output_dir, complex_name)
    os.makedirs(complex_folder, exist_ok=True)
    gb_folder = get_gb_folder(complex_folder)
    
    print(f"\n  🔗 Modo complexo: combinando {len(genera_list)} gêneros em '{complex_name}'")
    print(f"     Gêneros: {', '.join(genera_list)}")
    
    # Detectar última data dos arquivos .gb existentes na pasta do complexo
    last_date = None if force_run else get_last_gb_date(complex_folder)
    if last_date and not force_run:
        print(f"  📅 Última atualização detectada: {last_date}")
    
    all_genera_data = ""
    genera_status = {}
    
    for genus in genera_list:
        print(f"\n  🔬 Buscando sequências de: {genus}")
        
        # Busca sequências do gênero
        genus_data = fetch_genbank_by_genus(genus, api_key, last_date=last_date, force_run=force_run)
        
        if genus_data:
            all_genera_data += genus_data
            genera_status[genus] = "obtido"
            print(f"     ✅ Sequências obtidas para {genus}")
        else:
            genera_status[genus] = "sem_novidades"
            print(f"     ℹ️  Nenhuma sequência nova para {genus}")
    
    # Salva todos os dados em um único arquivo
    if all_genera_data:
        combined_fname = f"{complex_name}_all_genbank_{date_str}.gb"
        combined_fpath = os.path.join(gb_folder, combined_fname)
        with open(combined_fpath, "w") as f:
            f.write(all_genera_data)
        print(f"\n  💾 Arquivo combinado salvo: {combined_fpath}")
        overall_status = "novo" if not last_date else "atualizado"
    else:
        print(f"\n  ℹ️  Nenhuma sequência nova encontrada para o complexo")
        overall_status = "sem_novidades"
    
    # Atualizar log do complexo
    log_path = os.path.join(complex_folder, f"{complex_name}.log")
    with open(log_path, "a") as f:
        f.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]\n")
        f.write(f"input: {input_fname} [modo complexo]\n")
        f.write(f"genera: {', '.join(genera_list)}\n")
        f.write(f"force_run: {force_run}\n")
        f.write(f"last_date_filter: {last_date}\n")
        f.write(f"status: {overall_status}\n")
        for genus, status in genera_status.items():
            f.write(f"  - {genus}: {status}\n")
    
    # Log geral
    genera_str = "|".join(genera_list)
    general_log_entries.append(f"{input_fname};{date_str};COMPLEX;{genera_str};{complex_name};{overall_status}")
    
    return general_log_entries

###################### Main ######################

def process_genbank_inputs(input_dir=None, output_dir=None, force_run=False, genus_filter=None, 
                           sppcomplex_mode=False, complex_name=None):
    """
    Função principal para processar inputs do GenBank.
    Pode ser chamada externamente com caminhos personalizados.
    
    Args:
        input_dir: Diretório com arquivos de input (.fas ou .txt). 
                   Se None, usa 'genbank_in' relativo ao script.
        output_dir: Diretório de saída. Se None, usa 'genbank_out' relativo ao script.
        force_run: Se True, ignora arquivos .gb existentes e baixa tudo do zero.
        genus_filter: Se especificado, processa apenas este gênero (ignora inputs,
                      busca diretamente no GenBank).
        sppcomplex_mode: Se True, combina todos os gêneros em uma única pasta.
        complex_name: Nome da pasta do complexo (requer sppcomplex_mode).
                      Se None e sppcomplex_mode=True, usa <primeiro_gênero>_complex.
    
    Returns:
        - Em modo normal: list de gêneros processados
        - Em modo complex: dict com 'genera' (lista) e 'complex_name' (str)
    """
    # Define caminhos padrão se não fornecidos
    if input_dir is None:
        input_dir = os.path.join(script_dir, "genbank_in")
    if output_dir is None:
        output_dir = os.path.join(script_dir, "genbank_out")
    
    # Garante que os diretórios existem
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    # Se um gênero específico foi fornecido, processa apenas ele (ignora sppcomplex_mode)
    if genus_filter:
        print(f"\n🔬 Processando gênero específico: {genus_filter}")
        log_entries = process_txt_input(
            f"cli_genus_{genus_filter}", 
            [genus_filter], 
            output_dir, 
            force_run=force_run
        )
        general_log_path = os.path.join(output_dir, "genbank_log.log")
        update_general_log(general_log_path, log_entries)
        print("\n🎉 Processamento concluído.")
        return [genus_filter]
    
    all_inputs = get_inputs(input_dir)
    processed_genera = []

    if not all_inputs:
        print("⚠️ Nenhum input encontrado em", input_dir)
        if sppcomplex_mode:
            return {'genera': [], 'complex_name': complex_name}
        return processed_genera
    
    if force_run:
        print("⚡ Modo --force-run ativo: ignorando datas de arquivos .gb existentes")
    
    all_general_log_entries = []
    
    for fname, info in all_inputs.items():
        print(f"\n📂 Processando: {fname} (tipo={info['type']})")
        
        if info["type"] == "fasta":
            # FASTA: executa BLAST - não suporta sppcomplex_mode por enquanto
            if sppcomplex_mode:
                print("⚠️  Modo complexo não suportado para inputs FASTA. Processando normalmente.")
            log_entries = process_fasta_input(fname, info["data"], output_dir, force_run=force_run)
            all_general_log_entries.extend(log_entries)
        
        elif info["type"] == "txt":
            genera_in_file = info["data"]
            
            if sppcomplex_mode and len(genera_in_file) > 1:
                # Modo complexo: combina todos os gêneros do arquivo
                # Define nome do complexo se não especificado
                effective_complex_name = complex_name or f"{genera_in_file[0]}_complex"
                
                log_entries = process_txt_input_complex(
                    fname, genera_in_file, output_dir, 
                    effective_complex_name, force_run=force_run
                )
                all_general_log_entries.extend(log_entries)
                processed_genera.extend(genera_in_file)
                
                # Atualiza complex_name para retorno
                complex_name = effective_complex_name
            else:
                # Modo normal: processa cada gênero separadamente
                if sppcomplex_mode and len(genera_in_file) == 1:
                    print("ℹ️  Apenas 1 gênero no input - modo complexo não aplicável.")
                
                log_entries = process_txt_input(fname, genera_in_file, output_dir, force_run=force_run)
                all_general_log_entries.extend(log_entries)
                processed_genera.extend(genera_in_file)
    
    # Salva o log geral
    general_log_path = os.path.join(output_dir, "genbank_log.log")
    update_general_log(general_log_path, all_general_log_entries)
    print("\n🎉 Processamento concluído. Verifique os logs e arquivos de saída.")
    
    # Retorna informações do complexo se em modo complex
    if sppcomplex_mode and complex_name:
        return {'genera': processed_genera, 'complex_name': complex_name}
    
    return processed_genera


def parse_arguments():
    """
    Configura e parseia argumentos de linha de comando.
    """
    parser = argparse.ArgumentParser(
        description="Busca e baixa sequências do GenBank por gênero ou BLAST.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python obtain_seqs.py                           # Usa diretórios padrão
  python obtain_seqs.py -i ./meu_input -o ./saida # Diretórios customizados
  python obtain_seqs.py --force-run               # Ignora .gb existentes, baixa tudo
  python obtain_seqs.py -f                        # Atalho para --force-run

Estrutura de saída:
  genbank_out/
  └── Genus/
      ├── Genus.log              # Log de processamento do gênero
      └── gb/
          ├── Genus_all_genbank_22feb2026.gb
          └── Genus_blast_top100_hits_22feb2026.gb
        """
    )
    
    parser.add_argument(
        "-i", "--input-dir",
        type=str,
        default=None,
        help="Diretório com arquivos de input (.fas ou .txt). Default: genbank_in/"
    )
    
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        default=None,
        help="Diretório de saída para os arquivos .gb. Default: genbank_out/"
    )
    
    parser.add_argument(
        "-f", "--force-run",
        action="store_true",
        help="Ignora arquivos .gb existentes e baixa todas as sequências do zero."
    )
    
    parser.add_argument(
        "-g", "--genus",
        type=str,
        default=None,
        metavar="GENUS",
        help="Processa apenas o gênero especificado (ex: Fomitiporia). Ignora arquivos de input."
    )
    
    parser.add_argument(
        "-s", "--sppcomplex-mode",
        action="store_true",
        help="Modo complexo: combina todos os gêneros do input em uma única pasta."
    )
    
    parser.add_argument(
        "-c", "--complex-name",
        type=str,
        default=None,
        metavar="NAME",
        help="Nome da pasta do complexo (requer --sppcomplex-mode). Padrão: <primeiro_gênero>_complex"
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    process_genbank_inputs(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        force_run=args.force_run,
        genus_filter=args.genus,
        sppcomplex_mode=args.sppcomplex_mode,
        complex_name=args.complex_name
    )

##### funcionando
##### Tem que consultar os demais se há erros no IF ou na recuperação dos dados do IF.
### veja a anotação no .txt

import pandas as pd
import json
import re
import os

def qualify_vouchers(genus_name, csv_path, basionym_json_path, voucher_dict_path):
    """
    Qualifica as linhas de um arquivo CSV como "material tipo" ou "material de referência".

    Args:
        genus_name (str): O nome do gênero do arquivo (ex: "Tropicoporus").
        csv_path (str): O caminho para o arquivo CSV de entrada.
        basionym_json_path (str): O caminho para o arquivo JSON com informações de basionyms e tipos.
        voucher_dict_path (str): O caminho para o arquivo JSON do dicionário de vouchers.
    """
    try:
        # Carrega os dados
        df_csv = pd.read_csv(csv_path, sep='\t', encoding='utf-8')
        with open(basionym_json_path, 'r', encoding='utf-8') as f:
            basionyms_data = json.load(f)
        with open(voucher_dict_path, 'r', encoding='utf-8') as f:
            voucher_dict = json.load(f)
    except FileNotFoundError as e:
        print(f"Erro: Arquivo não encontrado - {e.filename}")
        return
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar o arquivo JSON: {e}")
        return

    # Passo 1: Qualificar com base na coluna 'type_material'
    # Localiza o índice da coluna 'type_material' e soma 1 para a próxima posição
    if 'type_material' in df_csv.columns:
        idx = df_csv.columns.get_loc('type_material') + 1
        df_csv.insert(idx, 'type_priority', None)
    else:
        # Caso a coluna type_material não exista por algum motivo, cria no final
        df_csv['type_priority'] = None
    
    # Padroniza a string de busca para ser case-insensitive e flexível
# Padroniza as strings de busca para "ex-types" (culturas ou isolados)
    ex_type_patterns = {
        'ex-holotype': r'(culture|isolate)\s+(from|of)\s+(the\s+)?holotype',
        'ex-epitype': r'(culture|isolate)\s+(from|of)\s+(the\s+)?epitype',
        'ex-isotype': r'(culture|isolate)\s+(from|of)\s+(the\s+)?isotype',
        'ex-lectotype': r'(culture|isolate)\s+(from|of)\s+(the\s+)?lectotype',
        'ex-neotype': r'(culture|isolate)\s+(from|of)\s+(the\s+)?neotype',
        'ex-paratype': r'(culture|isolate)\s+(from|of)\s+(the\s+)?paratype',
        'ex-topotype': r'(culture|isolate)\s+(from|of)\s+(the\s+)?topotype',
        'ex-type': r'(culture|isolate)\s+(from|of)\s+(the\s+)?type',
    }

    # Padroniza as strings de busca padrão para materiais do tipo
    type_patterns = {
        'holotype': r'holotype\s+of',
        'epitype': r'epitype\s+of',
        'isotype': r'isotype\s+of',
        'lectotype': r'lectotype\s+of',
        'neotype': r'neotype\s+of',
        'paratype': r'paratype\s+of',
        'topotype': r'topotype\s+of',
        'type': r'type\s+of',
    }

    # Aplica as regras de prioridade
    for row_index, row in df_csv.iterrows():
        type_material_str = str(row.get('type_material', '')).lower()
        if pd.notna(type_material_str) and type_material_str.strip() != 'nan' and type_material_str.strip() != '':
            assigned = False
            
            # 1. Primeiro, tenta identificar se é um "ex-type" (edge case)
            for type_name, pattern in ex_type_patterns.items():
                if re.search(pattern, type_material_str):
                    df_csv.at[row_index, 'type_priority'] = type_name
                    assigned = True
                    break
            
            # 2. Se não for cultura/isolado, aplica a verificação normal
            if not assigned:
                for type_name, pattern in type_patterns.items():
                    if re.search(pattern, type_material_str):
                        df_csv.at[row_index, 'type_priority'] = type_name
                        break

    # Passo 2: Qualificar com base nos dicionários
    # Criar um mapeamento inverso do dicionário de vouchers para facilitar a busca
    reverse_voucher_dict = {}
    for key, values in voucher_dict.items():
        for value in values:
            # Normaliza tanto a chave quanto o valor para uma comparação consistente
            normalized_key = key.replace(' ', '').replace('-', '').replace('/', '').replace('.', '').replace(':', '').upper()
            normalized_value = value.replace(' ', '').replace('-', '').replace('/', '').replace('.', '').replace(':', '').upper()
            reverse_voucher_dict[normalized_value] = normalized_key

 # Iterar sobre o JSON de basionyms
    for record_id, data in basionyms_data.items():
        # Acessa a lista 'types' (ou retorna uma lista vazia se não existir)
        types_list = data.get('types', [])
        
        # Itera sobre cada dicionário dentro da lista 'types'
        for type_entry in types_list:
            type_of_type = type_entry.get('type_of_type')
            type_materials = type_entry.get('material', [])
            
            if type_of_type and type_materials:
                for material in type_materials:
                    # Normaliza o material para comparar com o dicionário de vouchers
                    normalized_material = material.replace(' ', '').replace('-', '').replace('/', '').replace('.', '').replace(':', '').upper()
                    
                    # Procura o material no dicionário de vouchers (chave e valores)
                    
                    # Tenta encontrar a chave padronizada diretamente ou através do mapeamento inverso
                    voucher_to_search = reverse_voucher_dict.get(normalized_material) or normalized_material

                    # Busca no CSV pela coluna 'voucher' que corresponde
                    matching_rows = df_csv['voucher'].str.replace(' ', '').str.replace('-', '').str.replace('/', '').str.replace('.', '').str.replace(':', '').str.upper().eq(voucher_to_search)

                    if matching_rows.any():
                        # Se encontrar, anota o 'type_of_type' na coluna 'type_priority'
                        # Apenas se a coluna ainda não tiver sido preenchida pelo passo 1
                        df_csv.loc[matching_rows, 'type_priority'] = df_csv.loc[matching_rows, 'type_priority'].fillna(type_of_type)
                            
    # Salva o arquivo CSV atualizado
    output_filename = f"{genus_name}_output_dm_qualified.csv"
    output_path = os.path.join(os.path.dirname(csv_path), output_filename)
    df_csv.to_csv(output_path, sep='\t', index=False, encoding='utf-8')
    print(f"Qualificação concluída. Arquivo salvo em: {output_path}")

# --- Instruções para executar no Jupyter Notebook ---
# 1. Defina o nome do seu gênero:
genus_name = 'Fomitiporia'

# 2. Defina os caminhos para os arquivos.
#    Adapte os caminhos abaixo para refletir a localização real dos seus arquivos.
csv_file = f"/home/genivaldo/Documents/Pyroom/jupyter_room/DatasetMaker_working/TaxonQualifier/working/Fomitiporia_based_test/{genus_name}_output_dm.csv"
basionyms_json_file = f"/home/genivaldo/Documents/Pyroom/jupyter_room/DatasetMaker_working/TaxonQualifier/working/Fomitiporia_based_test/{genus_name}_soap.json"
voucher_dict_file = f"/home/genivaldo/Documents/Pyroom/jupyter_room/DatasetMaker_working/TaxonQualifier/working/Fomitiporia_based_test/{genus_name}_voucher_dict.json"

# 3. Chame a função `qualify_vouchers` com os caminhos definidos:
qualify_vouchers(genus_name, csv_file, basionyms_json_file, voucher_dict_file)

"""
Fase 1: Limpeza de Species e Extração de Vouchers

Processa a coluna 'species' para:
1. Extrair vouchers embutidos (ex: "Genus sp. FL01" → voucher="FL01")
2. Limpar a string de species (ex: "Genus sp. FL01" → "Genus sp.")
3. Atualizar voucher_dict se necessário

Regra principal:
- Se voucher original está VAZIO → preencher com voucher extraído
- Se voucher original JÁ EXISTE → apenas limpar species, descartar voucher extraído
"""

import re
import json
import logging
import pandas as pd
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from .phase0_detection import (
    has_voucher_in_species, 
    is_voucher_empty,
    INCOMPLETE_SPECIES_PATTERNS
)

logger = logging.getLogger(__name__)


@dataclass
class CleanupResult:
    """Resultado da limpeza de um registro."""
    original_species: str
    cleaned_species: str
    extracted_voucher: Optional[str]
    voucher_used: bool  # True se o voucher extraído foi usado para preencher
    

@dataclass
class CleanupReport:
    """Relatório da fase de limpeza."""
    total_processed: int = 0
    species_cleaned: int = 0
    vouchers_extracted: int = 0
    vouchers_filled: int = 0  # Preenchidos porque estava vazio
    vouchers_discarded: int = 0  # Descartados porque já existia voucher
    
    def __str__(self) -> str:
        return f"""
═══════════════════════════════════════════════════════
  RELATÓRIO DE LIMPEZA (Fase 1)
═══════════════════════════════════════════════════════
  Total processados:         {self.total_processed}
  Species limpos:            {self.species_cleaned}
  Vouchers extraídos:        {self.vouchers_extracted}
  ├─ Preenchidos:            {self.vouchers_filled}
  └─ Descartados:            {self.vouchers_discarded}
═══════════════════════════════════════════════════════
"""


# Alias para compatibilidade
SpeciesCleanupResult = CleanupReport


def normalize_voucher(voucher: str) -> str:
    """
    Normaliza voucher para comparação.
    Remove espaços, dois-pontos, hífens.
    """
    if not voucher:
        return ""
    # Remove caracteres especiais e converte para lowercase
    normalized = re.sub(r'[\s:\-_]', '', str(voucher).lower())
    return normalized


def process_species_voucher(species: str, voucher: str) -> tuple[str, Optional[str]]:
    """
    Processa a relação species/voucher.
    
    Retorna: (species_limpo, voucher_a_preencher)
    
    voucher_a_preencher só tem valor se:
    - Um voucher foi encontrado na string species
    - E o campo voucher original estava vazio
    """
    has_embedded, species_clean, voucher_found = has_voucher_in_species(species)
    
    if not has_embedded:
        # Nada a fazer
        return species, None
    
    # Voucher original estava vazio → preencher com o extraído
    if is_voucher_empty(voucher):
        return species_clean, voucher_found
    
    # Voucher já existe → apenas limpar species, descartar extraído
    return species_clean, None


def cleanup_species_column(df: pd.DataFrame,
                           species_col: str = 'Species',
                           voucher_col: str = 'voucher',
                           indices_to_process: list = None) -> tuple[pd.DataFrame, CleanupReport]:
    """
    Limpa a coluna species e extrai vouchers embutidos.
    
    Args:
        df: DataFrame a processar
        species_col: Nome da coluna de espécie
        voucher_col: Nome da coluna de voucher  
        indices_to_process: Lista de índices a processar (None = todos)
    
    Returns:
        tuple: (DataFrame modificado, CleanupReport)
    """
    report = CleanupReport()
    df = df.copy()  # Não modificar original
    
    # Determinar índices a processar
    if indices_to_process is None:
        indices = df.index.tolist()
    else:
        indices = indices_to_process
    
    report.total_processed = len(indices)
    logger.info(f"Iniciando limpeza de {report.total_processed} registros...")
    
    for idx in indices:
        species_val = df.at[idx, species_col] if species_col in df.columns else None
        voucher_val = df.at[idx, voucher_col] if voucher_col in df.columns else None
        
        if pd.isna(species_val):
            continue
        
        species_str = str(species_val)
        voucher_str = str(voucher_val) if not pd.isna(voucher_val) else ''
        
        # Processar
        cleaned_species, voucher_to_fill = process_species_voucher(species_str, voucher_str)
        
        # Atualizar DataFrame se houve mudança
        if cleaned_species != species_str:
            df.at[idx, species_col] = cleaned_species
            report.species_cleaned += 1
            
            if voucher_to_fill:
                report.vouchers_extracted += 1
                report.vouchers_filled += 1
                df.at[idx, voucher_col] = voucher_to_fill
                logger.debug(f"[{idx}] Species: '{species_str}' → '{cleaned_species}', "
                           f"Voucher preenchido: '{voucher_to_fill}'")
            else:
                # Voucher foi extraído mas descartado (já existia outro)
                _, _, extracted = has_voucher_in_species(species_str)
                if extracted:
                    report.vouchers_extracted += 1
                    report.vouchers_discarded += 1
                    logger.debug(f"[{idx}] Species: '{species_str}' → '{cleaned_species}', "
                               f"Voucher descartado: '{extracted}' (já existe: '{voucher_str}')")
    
    logger.info(f"Limpeza concluída: {report.species_cleaned} species limpos, "
                f"{report.vouchers_filled} vouchers preenchidos, "
                f"{report.vouchers_discarded} vouchers descartados")
    
    return df, report


def update_voucher_dict(voucher_dict: dict, 
                        new_voucher: str, 
                        existing_key: str = None) -> dict:
    """
    Atualiza o voucher_dict com um novo voucher.
    
    Se existing_key é fornecido, adiciona ao array existente.
    Caso contrário, cria nova entrada.
    
    NOTA: Por decisão de design, na Fase 1 NÃO adicionamos vouchers
    ao dict quando já existe outro voucher diferente.
    Esta função é mantida para uso futuro em outras fases.
    """
    if not new_voucher:
        return voucher_dict
    
    normalized = normalize_voucher(new_voucher)
    
    # Verificar se já existe em algum lugar
    for key, values in voucher_dict.items():
        all_variants = [normalize_voucher(key)] + [normalize_voucher(v) for v in values]
        if normalized in all_variants:
            # Já existe, não precisa adicionar
            return voucher_dict
    
    if existing_key and existing_key in voucher_dict:
        # Adicionar ao array existente
        voucher_dict[existing_key].append(new_voucher)
    else:
        # Nova entrada
        voucher_dict[new_voucher] = []
    
    return voucher_dict


def load_voucher_dict(path: Path) -> dict:
    """Carrega voucher_dict de arquivo JSON."""
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_voucher_dict(voucher_dict: dict, path: Path) -> None:
    """Salva voucher_dict em arquivo JSON."""
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(voucher_dict, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    # Teste básico
    logging.basicConfig(level=logging.DEBUG)
    
    # Dados de teste
    test_data = {
        'Species': [
            'Wrightoporia sp. FL01',           # Cenário A: voucher vazio → preencher
            'Wrightoporia sp. FL01',           # Cenário B: voucher igual → só limpar
            'Wrightoporia sp. FL01',           # Cenário C: voucher diferente → só limpar
            'Fomitiporia sp. DIS 229e',        # Cenário A
            'Fomitiporia australiensis',       # Sem voucher embutido
        ],
        'voucher': ['', 'FL01', 'CBS 123', '', 'BJFC 9123'],
        'geo_loc_name': ['', 'China', 'Brazil', '', 'Australia']
    }
    
    df = pd.DataFrame(test_data)
    print("ANTES:")
    print(df[['Species', 'voucher']])
    
    df_cleaned, report = cleanup_species_column(df)
    
    print("\nDEPOIS:")
    print(df_cleaned[['Species', 'voucher']])
    print(report)

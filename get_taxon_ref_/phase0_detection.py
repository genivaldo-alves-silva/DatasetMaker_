"""
Fase 0: Detecção de Lacunas nos Dados

Realiza scan completo do DataFrame para identificar:
- Vouchers vazios ou 'none'
- Countries vazios
- Species incompletos (Genus sp., *aceae, *ales, *mycetes, *mycota)
- Species com voucher embutido (Genus sp. VOUCHER123)
"""

import re
import logging
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class LacunasReport:
    """Relatório de lacunas encontradas no dataset."""
    total_records: int = 0
    voucher_empty: int = 0
    country_empty: int = 0
    species_incomplete: int = 0
    species_with_voucher: int = 0
    
    # Índices dos registros com lacunas (para processamento posterior)
    voucher_empty_indices: list = field(default_factory=list)
    country_empty_indices: list = field(default_factory=list)
    species_incomplete_indices: list = field(default_factory=list)
    species_with_voucher_indices: list = field(default_factory=list)
    
    def has_gaps(self) -> bool:
        """Verifica se há lacunas a preencher."""
        return (self.voucher_empty > 0 or 
                self.country_empty > 0 or 
                self.species_incomplete > 0)
    
    def needs_cleanup(self) -> bool:
        """Verifica se há limpeza de species a fazer."""
        return self.species_with_voucher > 0
    
    def needs_processing(self) -> bool:
        """Verifica se algum processamento é necessário."""
        return self.has_gaps() or self.needs_cleanup()
    
    def __str__(self) -> str:
        return f"""
═══════════════════════════════════════════════════════
  RELATÓRIO DE LACUNAS
═══════════════════════════════════════════════════════
  Total de registros:        {self.total_records}
  
  VOUCHERS
  └─ Vazios:                 {self.voucher_empty} ({self._pct(self.voucher_empty)}%)
  
  COUNTRY
  └─ Vazios:                 {self.country_empty} ({self._pct(self.country_empty)}%)
  
  SPECIES
  ├─ Incompletos:            {self.species_incomplete} ({self._pct(self.species_incomplete)}%)
  └─ Com voucher embutido:   {self.species_with_voucher}
═══════════════════════════════════════════════════════
  Necessita processamento:   {'SIM' if self.needs_processing() else 'NÃO'}
═══════════════════════════════════════════════════════
"""
    
    def _pct(self, value: int) -> str:
        if self.total_records == 0:
            return "0.0"
        return f"{(value / self.total_records * 100):.1f}"


# Padrões que indicam nome de espécie incompleto
INCOMPLETE_SPECIES_PATTERNS = {
    # Genus sp. (com ou sem coisas após)
    'sp_pattern': re.compile(r'^(\w+)\s+sp\.?\s*(.*)$', re.IGNORECASE),
    # Apenas nível de família (*aceae)
    'family_only': re.compile(r'^\w+aceae$', re.IGNORECASE),
    # Apenas nível de ordem (*ales)
    'order_only': re.compile(r'^\w+ales$', re.IGNORECASE),
    # Apenas nível de classe (*mycetes)
    'class_only': re.compile(r'^\w+mycetes$', re.IGNORECASE),
    # Apenas nível de filo (*mycota)
    'phylum_only': re.compile(r'^\w+mycota$', re.IGNORECASE),
}


def is_voucher_empty(value) -> bool:
    """Verifica se o campo voucher está vazio ou é 'none'."""
    if pd.isna(value):
        return True
    val_str = str(value).strip().lower()
    return val_str == '' or val_str == 'none' or val_str == 'nan'


def is_country_empty(value) -> bool:
    """Verifica se o campo country/geo_loc_name está vazio."""
    if pd.isna(value):
        return True
    val_str = str(value).strip()
    return val_str == '' or val_str.lower() == 'nan'


def is_species_incomplete(species: str) -> bool:
    """
    Verifica se o nome da espécie está incompleto.
    
    Exemplos de incompletos:
    - "Wrightoporia sp."
    - "Wrightoporia sp. FL01"
    - "Polyporaceae"
    - "Polyporales"
    - "Agaricomycetes"
    - "Basidiomycota"
    """
    if pd.isna(species):
        return True
    
    species_str = str(species).strip()
    if not species_str:
        return True
    
    # Verificar cada padrão
    for pattern_name, pattern in INCOMPLETE_SPECIES_PATTERNS.items():
        if pattern.match(species_str):
            return True
    
    # Nome com apenas uma palavra (provavelmente só gênero)
    if len(species_str.split()) == 1:
        return True
    
    return False


def has_voucher_in_species(species: str) -> tuple[bool, Optional[str], Optional[str]]:
    """
    Verifica se a string de species contém um voucher embutido.
    
    Retorna: (tem_voucher, species_limpo, voucher_encontrado)
    
    Exemplos:
    - "Wrightoporia sp. FL01" → (True, "Wrightoporia sp.", "FL01")
    - "Wrightoporia sp. DIS 229e" → (True, "Wrightoporia sp.", "DIS 229e")
    - "Wrightoporia lenta" → (False, None, None)
    """
    if pd.isna(species):
        return False, None, None
    
    species_str = str(species).strip()
    
    # Padrão: "Genus sp. ALGO"
    match = INCOMPLETE_SPECIES_PATTERNS['sp_pattern'].match(species_str)
    if match:
        genus = match.group(1)
        after_sp = match.group(2).strip()
        
        if after_sp:
            # Tem algo após "sp." - provavelmente voucher
            return True, f"{genus} sp.", after_sp
    
    return False, None, None


def detect_lacunas(df: pd.DataFrame, 
                   voucher_col: str = 'voucher',
                   country_col: str = 'geo_loc_name',
                   species_col: str = 'Species') -> LacunasReport:
    """
    Detecta lacunas no DataFrame.
    
    Args:
        df: DataFrame com dados do GenBank
        voucher_col: Nome da coluna de voucher
        country_col: Nome da coluna de país/localidade
        species_col: Nome da coluna de espécie
    
    Returns:
        LacunasReport com estatísticas e índices dos registros com lacunas
    """
    report = LacunasReport()
    report.total_records = len(df)
    
    logger.info(f"Iniciando detecção de lacunas em {report.total_records} registros...")
    
    for idx, row in df.iterrows():
        # Verificar voucher
        voucher_val = row.get(voucher_col)
        if is_voucher_empty(voucher_val):
            report.voucher_empty += 1
            report.voucher_empty_indices.append(idx)
        
        # Verificar country
        country_val = row.get(country_col)
        if is_country_empty(country_val):
            report.country_empty += 1
            report.country_empty_indices.append(idx)
        
        # Verificar species
        species_val = row.get(species_col)
        if is_species_incomplete(species_val):
            report.species_incomplete += 1
            report.species_incomplete_indices.append(idx)
        
        # Verificar se species tem voucher embutido
        has_voucher, _, _ = has_voucher_in_species(species_val)
        if has_voucher:
            report.species_with_voucher += 1
            report.species_with_voucher_indices.append(idx)
    
    logger.info(f"Detecção concluída: {report.voucher_empty} vouchers vazios, "
                f"{report.country_empty} países vazios, "
                f"{report.species_incomplete} species incompletos, "
                f"{report.species_with_voucher} species com voucher embutido")
    
    return report


# Alias para compatibilidade
detect_gaps = detect_lacunas


if __name__ == "__main__":
    # Teste básico
    import sys
    logging.basicConfig(level=logging.INFO)
    
    # Dados de teste
    test_data = {
        'Species': [
            'Wrightoporia sp. FL01',
            'Wrightoporia lenta',
            'Polyporaceae',
            'Fomitiporia sp.',
            'Fomitiporia australiensis'
        ],
        'voucher': ['', 'CBS 123', None, 'BJFC 9123', ''],
        'geo_loc_name': ['', 'China', '', None, 'Brazil']
    }
    
    df = pd.DataFrame(test_data)
    report = detect_lacunas(df)
    print(report)

#!/usr/bin/env python3
"""Script para debug: ver títulos BLAST brutos e como o gênero é extraído."""

import sys
import re
from Bio import SeqIO
from Bio.Blast import NCBIWWW, NCBIXML

def extract_genus_from_blast_title(title):
    """
    Extrai o nome do gênero de um título BLAST de forma robusta.
    """
    # Remove prefixos de identificador (gi|...|gb|...|, ref|...|, etc.)
    cleaned = re.sub(r'^\S+\|.*?\|\s*', '', title)
    
    # Lista de palavras para pular (não são gêneros)
    skip_words = {
        'uncultured', 'synthetic', 'environmental', 'unidentified',
        'unknown', 'unclassified', 'metagenome', 'clone', 'isolate',
        'strain', 'voucher', 'partial', 'complete', 'sequence',
        'gene', 'rna', 'dna', 'sp.', 'spp.', 'internal', 'transcribed',
        'spacer', 'ribosomal', 'subunit', 'large', 'small', 'mitochondrial'
    }
    
    words = cleaned.split()
    
    for word in words:
        clean_word = word.strip('.,;:()[]{}')
        if not clean_word or len(clean_word) < 2:
            continue
        if clean_word.lower() in skip_words:
            continue
        if re.match(r'^[A-Z][a-z]+(?:-[a-z]+)*$', clean_word):
            return clean_word
    
    if 'uncultured' in cleaned.lower():
        for word in words:
            clean_word = word.strip('.,;:()[]{}')
            if clean_word.lower() not in skip_words and len(clean_word) >= 2:
                return f"Uncultured_{clean_word}"
        return "Uncultured"
    
    return "NA"

# Testes unitários com os títulos reais do BLAST
print("="*80)
print("🧪 TESTES UNITÁRIOS - Extração de Gênero")
print("="*80)

test_cases = [
    ("gi|2328574698|gb|OP819291.1| Inocybe sp. isolate M51AR230 internal transcribed spacer", "Inocybe"),
    ("gi|353259799|gb|JN642232.1| Inocybe amazoniensis strain MCA3142 18S small subunit", "Inocybe"),
    ("gi|906398181|gb|KT289970.1| Uncultured fungus clone 67MS_7g internal transcribed spacer", "Uncultured_fungus"),
    ("gi|374723388|gb|JN663843.1| Uncultured Inocybe clone 65ms_3g 18S ribosomal RNA gene", "Inocybe"),
    ("gi|345846039|gb|JN168722.1| Uncultured Inocybe voucher ECM397 internal transcribed spacer", "Inocybe"),
    ("Fomitiporia mediterranea strain CBS 1388", "Fomitiporia"),
]

all_passed = True
for title, expected in test_cases:
    result = extract_genus_from_blast_title(title)
    status = "✅" if result == expected else "❌"
    if result != expected:
        all_passed = False
    print(f"{status} Esperado: '{expected:20s}' | Obtido: '{result:20s}' | Title: {title[:60]}...")

print(f"\n{'='*80}")
if all_passed:
    print("✅ Todos os testes passaram!")
else:
    print("⚠️ Alguns testes falharam.")
print(f"{'='*80}\n")

# Agora roda BLAST real para verificar

# Lê a primeira sequência do arquivo FASTA
fasta_file = "/home/genivaldo/DatasetMaker_/genbank_inout/genbank_in/input_17jun2026.fas"
records = list(SeqIO.parse(fasta_file, "fasta"))

if not records:
    print("Nenhuma sequência encontrada no arquivo FASTA")
    sys.exit(1)

sequence = str(records[0].seq)
print(f"🧬 Espécime: {records[0].id} (len={len(sequence)})")
print(f"🔍 Rodando BLAST...")

try:
    result_handle = NCBIWWW.qblast(
        program="blastn",
        database="nr",
        sequence=sequence,
        hitlist_size=20
    )
    blast_records = NCBIXML.parse(result_handle)
    blast_record = next(blast_records)

    print(f"\n{'='*80}")
    print(f"📋 TÍTULOS BLAST BRUTOS (primeiros 20 hits)")
    print(f"{'='*80}\n")

    if blast_record.alignments:
        for i, alignment in enumerate(blast_record.alignments[:20], 1):
            title = alignment.title
            accession = alignment.accession
            
            # Lógica antiga
            genus_antigo = title.split()[1] if " " in title else "NA"
            # Lógica nova
            genus_novo = extract_genus_from_blast_title(title)
            
            marker = "✅" if genus_novo != genus_antigo else ""
            
            print(f"Hit {i:2d}:")
            print(f"  Accession: {accession}")
            print(f"  Title:     {title}")
            print(f"  Antigo (split()[1]): '{genus_antigo:20s}' | Novo: '{genus_novo:20s}' {marker}")
            print()
    else:
        print("Nenhum alinhamento encontrado.")

except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc()

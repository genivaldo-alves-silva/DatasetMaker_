# 📋 Edge Cases - Extração de Tabelas de PDFs

> **Última atualização:** Março 2026  
> **Objetivo:** Documentar casos especiais de tabelas GenBank em artigos científicos

---

## 🔬 Resumo dos Testes

| Ferramenta | Edge 1 (Transposta) | Edge 2 (Forward Fill) | Edge 3 (Merged) | Tempo |
|------------|---------------------|----------------------|-----------------|-------|
| **PyMuPDF get_text()** | 59 acc ✅ | 435 acc ✅ | 129 acc ✅ | **<0.1s** |
| **pdfplumber** | 1 acc ❌ | 435 acc ✅ | 88 acc ⚠️ | ~1s |
| **pymupdf4llm** | 1 acc ❌ | 436 acc ✅ | N/A | 2-10s |
| **Docling** | 59 acc ✅ | N/A | N/A | **~3 min** |

**Conclusão:** PyMuPDF `get_text()` é a melhor opção para extração rápida de accessions.

---

## 📄 Edge Case 1: Tabela TRANSPOSTA

### Descrição
Tabela com headers na **primeira coluna** (vertical) e dados nas **colunas subsequentes**.
O PDF original tem a tabela **rotacionada 90°**, resultando em texto que parece "espelhado" quando extraído linearmente por algumas ferramentas.

### Características
- Headers: `Genera/Species name`, `Origin`, `Collection reference`, `Substrate`, `Accession #`
- Estrutura: Cada espécie é uma COLUNA, não uma linha
- Problema: pdfplumber e pymupdf4llm extraem texto invertido (`330950YA` ao invés de `AY059033`)

### Arquivos de Teste

**PDF:**
```
get-taxonREF/docling_pdf2md/pdf/5. Phylloporia nouraguensis, an undescribed species on Myrtaceae from French Guiana.pdf
```

**Markdown (convertido por Docling):**
```
get-taxonREF/docling_pdf2md/md/5. Phylloporia nouraguensis, an undescribed species on Myrtaceae from French Guiana.md
get-taxonREF/md/Phylloporia nouraguensis, an undescribed species on Myrtaceae from French Guiana.md
```

### Resultados
- **PyMuPDF:** 59 accessions extraídos corretamente ✅
- **pdfplumber:** 1 accession (falha na detecção de rotação) ❌
- **Docling:** 59 accessions com tabela estruturada ✅ (mas ~3 min)

### Solução Implementada
PyMuPDF lida automaticamente com a rotação de página. O texto é extraído corretamente sem necessidade de transformação manual.

---

## 📄 Edge Case 2: FORWARD FILL (Species → Vouchers)

### Descrição
Tabela onde o nome da espécie aparece **apenas uma vez** seguido de múltiplas linhas de vouchers.
Essas linhas subsequentes têm a célula de species **vazia** e precisam ser preenchidas com propagação (forward fill).

### Características
- Padrão: `Fomitiporia aethiopica` aparece uma vez, seguido de 3-5 linhas só com vouchers
- Headers: `Genus/species names`, `Voucher specimens`, `Locality`, `nLSU`, `ITS`, `tef1-α`, `RPB2`
- Tabela grande: ~300 linhas, múltiplas páginas

### Arquivos de Teste

**PDF:**
```
get-taxonREF/docling_pdf2md/pdf/4. Fomitiporia baccharidis comb. nov., a little known species from high elevation Andean forests and it.pdf
```

**Markdown (convertido por Docling):**
```
get-taxonREF/docling_pdf2md/md/4. Fomitiporia baccharidis comb. nov., a little known species from high elevation Andean forests and it.md
get-taxonREF/md/ex1_Fomitiporia baccharidis comb. nov., a little known species from high elevation Andean forests and it.md
```

### Resultados
- **PyMuPDF:** 435 accessions ✅
- **pdfplumber:** 435 accessions ✅
- **pymupdf4llm:** 436 accessions ✅

### Solução Implementada
Função `forward_fill_species()` em `phase4_pdf_extraction.py`:
1. Detecta linhas onde species começa com voucher pattern (ex: `MUCL 44777`)
2. Propaga o último species válido para essas linhas
3. Extrai voucher da célula original para coluna `_extracted_voucher`

---

## 📄 Edge Case 3: Linhas MESCLADAS (Múltiplas Species/Vouchers)

### Descrição
Tabela onde **múltiplas espécies e vouchers** estão concatenados em uma única célula,
sem separador claro além de padrões de nomenclatura.

### Características
- Padrão: `Hymenochaete epichlora He 525 Hymenochaete floridea He 536` em uma célula
- Necessita split por regex de species binomial
- Comum em tabelas compactas de filogenia

### Arquivos de Teste

**PDF:**
```
get-taxonREF/docling_pdf2md/pdf/1. The Fomitiporia punctata-robusta complex (Basidiomycota, Hymenochaetales) multilocus phylogenetic an.pdf
get-taxonREF/docling_pdf2md/pdf/Hymenochaetaceae (Hymenochaetales) from the Guineo-Congolian phytochorion Phylloporia littoralis sp..pdf
```

**Markdown (convertido por Docling):**
```
get-taxonREF/docling_pdf2md/md/1. The Fomitiporia punctata-robusta complex (Basidiomycota, Hymenochaetales) multilocus phylogenetic an.md
get-taxonREF/md/The Fomitiporia punctata-robusta complex (Basidiomycota, Hymenochaetales) multilocus phylogenetic an.md
```

### Resultados
- **PyMuPDF:** 129 accessions ✅
- **pdfplumber:** 88 accessions ⚠️

### Solução Implementada
Funções `detect_merged_rows()` e `expand_merged_rows()` em `phase4_pdf_extraction.py`:
1. Detecta células com múltiplos binomials (`Genus species Genus species`)
2. Split usando regex: `r'(?=[A-Z][a-z]+\s+[a-z]+(?:\s+[a-z]+)?(?:\s+[A-Z]))'`
3. Expande em múltiplas linhas mantendo outros campos

---

## 🛠️ Ferramentas Testadas

### PyMuPDF (`pymupdf`)
```python
import pymupdf
doc = pymupdf.open(pdf_path)
text = doc[page_num].get_text()
```
- ✅ Lida com rotação automaticamente
- ✅ Velocidade excelente (<0.1s)
- ❌ Não extrai estrutura de tabela (só texto linear)

### pdfplumber
```python
import pdfplumber
with pdfplumber.open(pdf_path) as pdf:
    text = pdf.pages[0].extract_text()
    tables = pdf.pages[0].extract_tables()
```
- ⚠️ Falha em tabelas rotacionadas (texto invertido)
- ✅ Bom para tabelas com bordas
- ⚠️ Fragmenta tabelas sem bordas

### pymupdf4llm
```python
import pymupdf4llm
md_text = pymupdf4llm.to_markdown(pdf_path)
```
- ⚠️ Não gera tabelas markdown para papers científicos
- ✅ Velocidade razoável (2-10s)
- ❌ Falha em PDFs rotacionados

### Docling
```python
from docling.document_converter import DocumentConverter
converter = DocumentConverter()
result = converter.convert(pdf_path)
tables = result.document.tables
```
- ✅ Melhor qualidade de extração de tabelas
- ✅ Lida com rotação corretamente
- ❌ **Muito lento** (~3 min por PDF em CPU)
- ⚠️ Requer GPU para velocidade aceitável

---

## 📁 Estrutura de Diretórios dos Arquivos de Teste

```
get-taxonREF/
├── docling_pdf2md/
│   ├── pdf/                          # PDFs originais
│   │   ├── 5. Phylloporia nouraguensis...pdf     # Edge Case 1
│   │   ├── 4. Fomitiporia baccharidis...pdf      # Edge Case 2
│   │   └── 1. The Fomitiporia punctata...pdf     # Edge Case 3
│   └── md/                           # Markdowns convertidos por Docling
│       ├── 5. Phylloporia nouraguensis...md
│       ├── 4. Fomitiporia baccharidis...md
│       └── 1. The Fomitiporia punctata...md
└── md/                               # Markdowns alternativos
    ├── Phylloporia nouraguensis...md
    ├── ex1_Fomitiporia baccharidis...md
    └── The Fomitiporia punctata...md
```

---

## 🎯 Estratégia de Extração Recomendada

```
CASCATA (implementada em phase4_pdf_extraction_v2.py):

1. [RÁPIDO] PyMuPDF get_text() + accession-anchor parsing (~0.1-0.3s)
   - Extrai texto linear (cada célula em uma linha)
   - Detecta header da tabela GenBank (gene columns)
   - Usa accession codes como âncoras para delimitar rows
   - Forward-fill species via backward row traversal
   - Tempo: <0.3s por PDF

2. [FALLBACK] Heuristic reverse lookup
   - Quando nenhum header é detectado
   - Classifica linhas próximas por tipo (species/voucher/country)
   - Menos preciso mas still functional

3. [CACHE] Se .md existe → parse_md_tables() (phase4_pdf_extraction.py)
   - Reutiliza conversões anteriores do Docling
   - Instantâneo
```

### Lições do artigo Medium (Mark Kramer, Aug 2025)

> "I Tested 12 Best-in-Class PDF Table Extraction Tools, and the Results Were Appalling"

- **Nenhuma ferramenta** out-of-the-box (Docling, Reducto, LLMs) extrai tabelas com 100% precisão
- **pdfplumber** é a melhor fundação library, mas requer código custom significativo
- **PyMuPDF** é extremamente rápido e lida com rotação automaticamente
- **LLMs (Claude, Gemini, o3)** falharam em tabelas complexas com merged cells
- O segredo é **combinar ferramentas com lógica custom** específica para o domínio

Para tabelas GenBank, PyMuPDF `get_text()` é suficiente porque:
- Estrutura repetitiva (species, voucher, locality, accessions)
- Accession codes facilmente identificáveis por regex
- Texto preserva ordem de leitura corretamente

---

## 📝 Código de Uso

```python
from get_taxon_ref_.phase4_pdf_extraction_v2 import find_accession_info

# Buscar species/voucher/country para um accession code
info = find_accession_info("path/to/paper.pdf", "JQ087932", verbose=True)
# → {'species': 'F. cupressicola', 'voucher': 'MUCL 52488', 'country': 'Mexico', ...}

# Extrair todos os registros de uma tabela GenBank
from get_taxon_ref_.phase4_pdf_extraction_v2 import extract_all_rows_from_pdf
rows = extract_all_rows_from_pdf("path/to/paper.pdf")
```

### Testes Validados (Março 2026)

| PDF | Accession | Species | Voucher | Country | Tempo |
|-----|-----------|---------|---------|---------|-------|
| Phylloporia nouraguensis | KC136220 | C. cf. stuckertiana | CORD, Robledo 218 | Argentina | 0.15s |
| Bambusicolous Fomitiporia | JQ087932 | F. cupressicola | MUCL 52488 | Mexico | 0.14s |
| Fomitiporia baccharidis | JX093771 | F. apiahyna | MUCL 53022a,b | French Guiana | 0.08s |
| Hymenochaetales 5 new spp. | JQ279559 | Hymenochaete asetosa | Dai 10756 | China | 0.27s |

---

## 📊 Teste em Massa — Todos os PDFs de `downloads/` (12 Mar 2026)

> **Sandbox de output:** `get_taxon_ref_/sandbox_csv/`  
> **Método:** PyMuPDF get_text() + accession-anchor parsing (phase4_pdf_extraction_v2.py)  
> **Extração de accessions:** pdfplumber regex (ou PyMuPDF para PDFs com tabelas rotacionadas)

| PDF | Accessions | Encontrados | Taxa (%) | Tempo | CSV |
|-----|-----------|-------------|----------|-------|-----|
| Anthracoidea koopmanii (Ustilaginales) | 51 | 51 | **100%** | 2.8s | `anthracoidea_results.csv` |
| Pluteaceae (Agaricales) | 371 | 338 | **91%** | 38.0s | `pluteaceae_results.csv` |
| Sanghuangporus (Basidiomycota) | 130 | 127 | **98%** | 13.4s | `sanghuangporus_results.csv` |
| Amylocorticiales — 2 new genera, 6 new spp. | 113 | 113 | **100%** | 11.6s | `amylocorticiales_results.csv` |
| Bambusicola Fomitiporia (multi-locus) | 331 | 329 | **99%** | 36.3s | `bambusicola_fomitiporia_results.csv` |
| Cabalodontia (Polyporales, Yunnan) | 92 | 91 | **99%** | 6.7s | `cabalodontia_results.csv` |
| Auricularia — Colombia | 174 | 171 | **98%** | 16.2s | `auricularia_results.csv` |
| Steccherinum (neotropical) | 376 | 374 | **99%** | 81.2s | `steccherinum_results.csv` |
| Leucocoprinus beninensis | 64 | 63 | **98%** | 11.0s | `leucocoprinus_results.csv` |
| Hymenochaetales — 5 new spp. (SW China) | 447 | 446 | **100%** | 99.1s | `hymenochaetales_5newspp_results.csv` |
| Fomitiporia baccharidis (neotropical) | 433 | 431 | **100%** | 40.1s | `fomi_results.csv` |
| Phylloporia nouraguensis *(rotated table)* | 57 | 57 | **100%** | 2.8s | `phylloporia_results.csv` |
| **TOTAL** | **2639** | **2591** | **98.2%** | ~359s | |

### Observações

- **Phylloporia:** pdfplumber falha ao extrair accessions (tabela rotacionada 90°). PyMuPDF `extract_text_lines()` foi usado como fonte de accessions.
- **Pluteaceae (91%):** Maior tabela com mais variação de formato. Alguns accessions do corpo do texto (não em tabela) não são localizáveis.
- **3 PDFs com 100% de acerto:** Anthracoidea, Amylocorticiales, Hymenochaetales — tabelas bem estruturadas.
- **Média de confiança `high`** na maioria dos resultados — boa detecção de headers e forward-fill de species.

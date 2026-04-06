# 📋 MDQualifier - Contexto de Implementação

> **Última atualização:** Junho 2026  
> **Status:** Fases 0-4 implementadas (v2 PyMuPDF), Fase 6 implementada, Fase 6.5 implementada, Fase 7 implementada

---

## 🎯 Objetivo

Preencher lacunas de `country`, `voucher` e `species` nos datasets taxonômicos, buscando dados em artigos científicos quando não disponíveis no GenBank.

---

## ✅ Fases Implementadas (0-3)

### Fase 0: Detecção de Lacunas
**Arquivo:** `phase0_detection.py`

Detecta registros com:
- `voucher` vazio ou `== 'none'`
- `country` / `geo_loc_name` vazio
- `species` incompleto (`Genus sp.`, `*aceae`, `*ales`, `*mycetes`, `*mycota`)
- `species` com voucher embutido (`Wrightoporia sp. FL01`)

**Funções principais:**
- `detect_gaps(df)` → `LacunasReport`
- `is_species_incomplete(species)` → bool
- `has_voucher_in_species(species)` → (bool, species_limpo, voucher)

---

### Fase 1: Limpeza de Species
**Arquivo:** `phase1_species_cleanup.py`

**Regra definida (IMPORTANTE):**
```python
# Cenário C tratado como B/D - NÃO adicionar voucher extraído ao voucher_dict se já existe outro voucher
def process_species_voucher(species: str, voucher: str) -> tuple[str, str | None]:
    """
    - Sempre limpar "Genus sp. ALGO" → "Genus sp."
    - voucher_a_preencher só retorna valor se voucher original estava VAZIO
    """
```

| Cenário | species | voucher | species_clean | voucher_a_preencher |
|---------|---------|---------|---------------|---------------------|
| **A** | `Genus sp. FL01` | vazio | `Genus sp.` | `FL01` |
| **B** | `Genus sp. FL01` | `FL01` | `Genus sp.` | `None` |
| **C** | `Genus sp. FL01` | `CBS 123` | `Genus sp.` | `None` (descarta FL01) |

---

### Fase 2: Banco de Artigos Local
**Arquivo:** `phase2_articles_db.py`

Estrutura em dois níveis:
```
articles_db/
├── articles_index.parquet    # Índice para busca rápida
└── articles_data/
    └── <doi_hash>.json       # Dados completos do artigo
```

**Colunas do índice:**
- `doi`, `title`, `year`, `journal`
- `gb_accessions` (lista)
- `species_mentioned`, `vouchers_found`, `countries_found`
- `has_gb_table`, `has_supplementary`, `pdf_downloaded`

**Funções principais:**
- `ArticlesDatabase.get_data_for_gb(gb_code)` → dict ou None
- `ArticlesDatabase.add_article(ArticleRecord)`
- `ArticlesDatabase.add_record_from_supplementary(doi, gb_code, record)`

---

### Fase 3.1: Resolução de DOI
**Arquivo:** `phase3_doi_resolver.py`

**Tratamento da coluna `title` (IMPORTANTE):**

| Cenário | title | Ação |
|---------|-------|------|
| (i) | Vazio | → NCBI ELink direto |
| (ii) | `Direct Submission` | → NCBI ELink direto |
| (iii) | `title1 \| title2` | → Tentar CrossRef com cada um |
| (iv) | `Direct Submission \| title` | → Ignorar DS, tentar com title |
| (v) | `title` | → CrossRef primeiro |

**Cascata de busca:**
1. CrossRef (por título) - se title válido
2. NCBI ELink (GB → PubMed → DOI) - sempre tenta
3. Google Scholar via pop8query (fallback) - pode falhar com CAPTCHA

**Funções principais:**
- `parse_title_column(title)` → list[str] (títulos válidos)
- `get_doi_for_record(gb_accession, title, ...)` → DOIResult

---

### Fase 3.2: Download de PDF
**Arquivo:** `phase3_pdf_downloader.py`

**Cascata de download:**
1. Unpaywall (Open Access)
2. Europe PMC (Open Access)
3. CrossRef (link direto)
4. Sci-Hub (requests)
5. Sci-Hub (Selenium - fallback)
6. URL direta do artigo (Selenium)

**Função principal:**
- `download_article_pdf(doi, output_path, email, ...)` → DownloadResult

---

### Fase 3.3: Material Suplementar
**Arquivo:** `phase3_supplementary.py`

**Publishers configurados:**
- Elsevier/ScienceDirect
- Springer/Nature
- Wiley
- Taylor & Francis
- MDPI
- Pensoft (MycoKeys, PhytoKeys)
- Generic fallback

**Formatos suportados:**
- `.xlsx`, `.xls` → pandas
- `.docx`, `.doc` → python-docx
- `.csv` → pandas
- `.zip` → extrai e processa conteúdo
- `.pdf` → será processado na Fase 4 (Docling)

**Aliases de colunas para tabelas GB:**
```python
COLUMN_ALIASES = {
    'species': ['species', 'taxon', 'organism', 'name', 'taxa'],
    'voucher': ['voucher', 'specimen', 'collection', 'herbarium', 'culture', 'strain'],
    'country': ['country', 'locality', 'location', 'origin', 'geo'],
    'gb_its': ['its', 'its1', 'its2', 'its1-5.8s-its2'],
    'gb_lsu': ['lsu', '28s', 'nrlsu', 'd1/d2'],
    'gb_tef1': ['tef1', 'tef', 'ef1', 'ef-1α'],
    # ... etc
}
```

**Funções principais:**
- `extract_supplementary_links(article_url)` → List[SupplementaryFile]
- `parse_supplementary_for_gb_table(files)` → List[ExtractedRecord]
- `find_record_by_gb_code(records, gb_code)` → ExtractedRecord

---

## ✅ Fase 4: Processamento de PDF — PyMuPDF-first (v2)
**Arquivo primário:** `phase4_pdf_extraction_v2.py`  
**Arquivo legado:** `phase4_pdf_extraction.py` (v1, Docling — mantido como fallback para .md existentes)

### Estratégia: Accession-Anchor Reverse Lookup

Em vez de tentar detectar e parsear tabelas inteiras (como faz pdfplumber/Docling), 
usamos o accession code como âncora:

1. Extrair texto completo do PDF com `PyMuPDF.get_text()`
2. Localizar os blocos de accession codes (linhas consecutivas com accessions/dashes)
3. Detectar headers de tabela (keywords como ITS, LSU, TEF1, etc.)
4. Mapear a posição do accession-alvo no bloco → coluna do gene
5. Olhar para trás (linhas acima) para encontrar species, voucher, country

### Por que PyMuPDF e não Docling?

| Aspecto | PyMuPDF (v2) | Docling (v1) |
|---------|-------------|-------------|
| **Velocidade** | ~0.1-0.3s/PDF | ~3min/PDF (CPU) |
| **Dependências** | Apenas pymupdf | docling + torch + models |
| **Rotação** | Automática | Manual |
| **Tabelas borderless** | Funciona (text-based) | Falha frequente |
| **Precisão (nossos PDFs)** | 4/4 test cases | Requer pós-processamento |

### Lições do artigo Medium (Mark Kramer, 2025):
- Nenhuma ferramenta (LLMs, Docling, Reducto) extrai tabelas com 100% de precisão
- pdfplumber é a melhor fundação, mas requer código custom significativo
- PyMuPDF é extremamente rápido e lida com rotação automaticamente
- O segredo é combinar ferramentas com lógica custom específica para o domínio

### Funções principais:

**Busca focada (um accession):**
- `lookup_accession_in_pdf(pdf_path, target_accession)` → `AccessionLookupResult`
- `find_accession_info(pdf_path, accession_code, verbose)` → dict (conveniência)

**Extração em massa (todos os registros):**
- `extract_all_rows_from_pdf(pdf_path)` → list[dict]

**Helpers internos:**
- `extract_text_lines(pdf_path)` → list[str] (PyMuPDF)
- `find_table_headers(lines)` → list[TableHeader]
- `_expand_accession_block(lines, target_idx)` → bloco de accessions
- `_extract_row_from_context(...)` → mapeia fields
- `_find_forward_fill_species(lines, row_start, header)` → backward traversal
- `_is_valid_species_candidate(s)` → blocklist de palavras comuns
- `_clean_species_name(species)` → remove autores, normaliza cf./aff.

### Dataclass:
```python
@dataclass
class AccessionLookupResult:
    accession: str          # O accession buscado
    species: str            # "F. cupressicola", "C. cf. stuckertiana"
    voucher: str            # "MUCL 52488", "CORD, Robledo 218"
    country: str            # "Mexico", "Argentina" 
    gene_region: str        # "ITS", "nLSU", etc.
    other_accessions: dict  # gene_region -> accession (do mesmo row)
    raw_row_lines: list     # Linhas originais do PDF
    confidence: str         # "high", "medium", "low"
    method: str             # "pymupdf", "pdfplumber", "markdown"
```

### Edge Cases Resolvidos:
1. **Tabelas borderless** (sem grid): pdfplumber retorna 0 tabelas → PyMuPDF funciona via texto
2. **Forward-fill de species**: Backward traversal por linhas acima, pulando blocos de accession
3. **Abbreviated genus**: `F. cupressicola`, `C. cf. stuckertiana` → SPECIES_RE com separador obrigatório
4. **Dados em mesma linha**: Múltiplos accessions em sequência → bloco expandido + mapeamento posicional
5. **Table headers compostos**: "GenBank accession numbers" como super-header → SUPER_HEADER_RE
6. **Numeração romana**: "Table I." → TABLE_TITLE_RE com `[IVXivx]+`

### Testes validados (Junho 2026):

| Case | PDF | Accession | Species encontrada | Voucher | Country | Tempo |
|------|-----|-----------|-------------------|---------|---------|-------|
| 1 | Robledo2015 | KC136220 | C. cf. stuckertiana | CORD, Robledo 218 | Argentina | 0.15s |
| 2 | BambusicolousFomitiporia | JQ087932 | F. cupressicola | MUCL 52488 | Mexico | 0.14s |
| 3 | Phylloporia_nouraguensis | JX093771 | F. apiahyna | MUCL 53022a,b | French Guiana | 0.08s |
| 4 | HymenochaetalesSWChina | JQ279559 | Hymenochaete asetosa | Dai 10756 | China | 0.27s |

### Integração no Pipeline:
- **md_qualifier.py** chama `lookup_accession_in_pdf()` automaticamente após download do PDF (Fase 3.2)
- Se encontrado, salva extração completa no `articles_db` via `extract_all_rows_from_pdf()`
- **phase3_supplementary.py** usa `parse_pdf_gb_table()` para PDFs suplementares (antes deferido para Docling)

---

## 🚧 Fases Pendentes

### Fase 5: Validação de Dados Extraídos
**Objetivo:** Validar country e voucher antes de salvar.

**Plano:**
- Country: verificar com pycountry + fuzzy match
- Voucher: verificar formato com regex de padrões conhecidos

**Regex de voucher válido:**
```python
VOUCHER_PATTERNS = [
    r'^[A-Z]{2,6}[-:\s]?\d+',    # "BJFC 9123", "CBS:12345"
    r'^\d+',                      # "12086"
    r'^[A-Za-z]+\s+\d+',         # "Dai 12086"
    r'^[A-Z]{1,3}\s*\d{4,}',     # "VNM00075562"
]
```

**Prioridade de fontes (em caso de conflito):**
```python
PRIORITY_SOURCE = {
    'country': ['article_table', 'genbank', 'gbif'],
    'voucher': ['genbank', 'article_table'],
    'species': ['article_taxonomy', 'article_table', 'genbank'],
}
```

---

### Fase 6: Fallback GBIF/iDigBio para Country
**Objetivo:** Buscar país quando todas as outras fontes falharam.

**Status:** ✅ Implementada em `phase6_gbif_fallback.py`

**APIs:**
- GBIF: `https://api.gbif.org/v1/occurrence/search?catalogNumber=VOUCHER`
- iDigBio: similar

**Plano:**
- Usar todos os vouchers do `voucher_dict` como query
- Extrair `country` do resultado

---

### Fase 7: Consolidação de Linhas
**Objetivo:** Reorganizar DataFrame quando novos vouchers são encontrados.

**Status:** ✅ Implementada em `phase7_consolidation.py`
### Fase 6.5: Fallback de Country por PDF Narrativo (pré-Fase 7)
**Objetivo:** Entre as fases 6 e 7, preencher apenas countries ainda vazios com parser narrativo de PDF.

**Status:** ✅ Implementada em `phase6_5_pdf_country.py`

**Regras principais:**
- Executa após `phase6_gbif_fallback` e antes de `phase7_consolidation`.
- Não sobrescreve country já preenchido.
- Usa PDFs já associados no banco local de artigos.
- Gera auditoria pré-consolidação por par `(voucher, gb_accession) -> country`.
- Quando voucher da linha está vazio, usa o accession para localizar contexto narrativo no PDF e inferir apenas `country`.
- A fase 6.5 não recupera/preenche voucher por accession-context (evita aumento de falso positivo).
- Quando regex não encontra match, pode usar fallback LLM (OpenRouter) de forma opcional/controlada.

**Artefatos de auditoria:**
- CSV em `get_taxon_ref_/logs/phase6_5_pairs/` com colunas:
    `row_index`, `doi`, `pdf_path`, `voucher`, `gb_accession`,
    `country_extracted`, `confidence`, `method`, `status`, `note`.


**Lógica:**
1. Para cada novo voucher encontrado, verificar se pertence a algum array do `voucher_dict`
2. Se pertence, mover dados para a linha correspondente
3. Merge de sequências por gene

---

## 📁 Estrutura de Arquivos

```
get_taxon_ref_/
├── __init__.py
├── md_qualifier.py               # Orquestrador principal
├── phase0_detection.py           # ✅ Implementado
├── phase1_species_cleanup.py     # ✅ Implementado
├── phase2_articles_db.py         # ✅ Implementado
├── phase3_doi_resolver.py        # ✅ Implementado
├── phase3_pdf_downloader.py      # ✅ Implementado
├── phase3_supplementary.py       # ✅ Implementado (agora processa PDFs suplementares)
├── phase4_pdf_extraction.py      # ✅ v1 (Docling/Markdown — fallback para .md existentes)
├── phase4_pdf_extraction_v2.py   # ✅ v2 (PyMuPDF-first — PRIMÁRIO, <0.3s/PDF)
├── EDGE_CASES_PDF_EXTRACTION.md  # Documentação de edge cases e decisões
├── IMPLEMENTATION_CONTEXT.md     # Este arquivo
├── phase5_validation.py          # 🚧 Pendente
├── phase6_gbif_fallback.py       # 🚧 Pendente
├── phase7_consolidation.py       # 🚧 Pendente
├── articles_db/
│   ├── articles_index.parquet
│   └── articles_data/
├── downloads/
└── logs/
```

---

## 🔗 Dependências de Código Existente

### De `get-taxonREF/porp8/`
- `PorP8_authors.py`: `execute_search()` para Google Scholar
- `porp8/downloader.py`: funções de download (Unpaywall, Sci-Hub, Selenium)
- `pop8query`: executável para buscas

### De `get-taxonREF/docling_pdf2md/`
- `pdf2md.py`: conversão PDF → Markdown com Docling

### De `TaxonQualifier/`
- `country_detector.py`: detecção de país (pycountry + geopy)

---

## ⚙️ Configurações Necessárias

```python
# Variáveis de ambiente
NCBI_API_KEY = os.getenv("NCBI_API_KEY")
UNPAYWALL_EMAIL = os.getenv("UNPAYWALL_EMAIL")

# Paths (opcionais)
pop8_path = Path("get-taxonREF/porp8/pop8query")
chrome_binary = Path("get-taxonREF/porp8/chrome/chrome")
chromedriver = Path("get-taxonREF/porp8/chrome-linux64/chromedriver")
```

---

## 🧪 Testes Realizados

### Fases 0-1 (Março 2026)
```python
# Input
test_data = {
    'Species': ['Wrightoporia sp. FL01', 'Wrightoporia lenta', 'Polyporaceae'],
    'voucher': ['', 'CBS 123', None],
    'geo_loc_name': ['', 'China', '']
}

# Output após Fase 1
# Species[0]: 'Wrightoporia sp. FL01' → 'Wrightoporia sp.'
# voucher[0]: '' → 'FL01' (extraído)
```

### NCBI ELink (testado em terminal)
```bash
# PV389820 → PubMed 40606284 → DOI 10.3897/mycokeys.118.154175
# Artigo: "Three new species of Fomitiporia..."
```

---

## 📝 Notas Importantes

1. **Google Scholar bloqueado:** pop8query retorna erro 522 (CAPTCHA). Usar NCBI ELink como rota principal.

2. **Sci-Hub:** Funciona com requests, mas Selenium é mais robusto para sites que bloqueiam.

3. **Docling:** Requer GPU para melhor performance, mas funciona em CPU (mais lento).

4. **Rate limits sugeridos:**
   - CrossRef: 50 req/min
   - GBIF: 100 req/min
   - Google Scholar: 10 req/min (conservador)
   - Sci-Hub: 5 req/min

---

## 🔄 Próximos Passos

1. ~~Implementar `phase4_pdf_extraction.py`~~ → ✅ Implementado como `phase4_pdf_extraction_v2.py` (PyMuPDF-first)
2. Implementar `phase5_validation.py` (pycountry + regex)
   - Recebe `AccessionLookupResult` ou `EnrichmentResult`
   - Valida country com pycountry + fuzzy match + `TaxonQualifier/country_detector.py`
   - Valida formato de voucher com regex
3. ~~Implementar `phase6_gbif_fallback.py`~~ → ✅ Implementado
4. ~~Implementar `phase7_consolidation.py`~~ → ✅ Implementado
5. Integrar ao `main.py` como flag `--enrich` (automático após `--qualify`)
6. Testar com dados reais (Diacanthodes, Fomitiporia, Wrightoporia)

## Input do usuário que iniciou tudo

Abaixo porei as várias ideias e implementações que estou pensando. Me ajude a entender se existem lacunas que eu possa preencher para um agente seguir, me faça perguntas se necessário, me retorne um plano estruturado em .md.

-------------
Outra possível atualização/completion baseadas nos códigos GB, são os nomes das espécies (coluna 'species'). Algumas foram depositadas como Genus sp. e nunca foram atualizadas.

Isso também pode ser recuperado das tabelas de GB dos artigos, como também se necessário de dentro dos artigos nas seções de Taxonomia.

Isto também deve entrar na rota de implementação.

Tenho que decidir quando as buscas em artigos serão conduzidas. Creio que algo como abaixo deve ser seguido.

Até agora temos que buscar informações de outros lugares (não existentes nos .gb e IndexFungorum) para os itens:
country, voucher, species (**quando 'Genus sp. ou family/order/class/filo name' (com 1 word ou genus/family/order/class/filo sp.), etc).

**'family':'*aceae', 'order':'*ales', 'class':'*mycetes', 'filo':'*mycota'.
***Exemplos de edge case para 'sp.' -> 'edge_cases_in_sp':'Wrightoporia sp. FL01', 'Wrightoporia sp. B1a0905EM2CC429', 'Wrightoporia sp. KUC20110922-37', 'Wrightoporia sp. DIS 229e'. Aqui só teria que verificar se a string após o sp. está no voucher_dict e eliminar para manter apenas '* sp.'. 

1. Creio que podemos primeiro manter o fluxo com gb_handle.py, mas, todo o restante abaixo podemos construir um novo script (./TaxonQualifier/md_qualifier.py).
   -Com base em 'genus_output_dm.csv' ou 'genus_output_dm.parquet', podemos começar as tentativas de preencher as lacunas de vouchers vazios. Para isso, (i) implementar a verificação se nas strings da coluna 'species' há algum voucher como nos exemplos acima***. Há raros casos além de quando está em 'sp.', mas também é possível ter 'Genus epiteto voucher'. Se encontrado algum voucher, verificar se não é o mesmo da coluna 'voucher' ou baseado no que está na coluna voucher, consultar os values em 'voucher_dict' Se a coluna voucher estiver vazia, preencha com este voucher que foi encontrado na string de 'species'; se vucher não estiver vazio, veja se este voucher encontrado na srting 'species' já está no array correspondente em 'voucher_dict'; se não está, inclua. Feito tudo isso pode limpar a string 'species' e deixar 'Genus sp.' ou 'Genus epiteto' ou como for só que sem o voucher.
   -Depois desse fluxo (gb_handle normal + início da tentativa de preencher vouchers vazios), teremos:
      --vouchers vazios ou == 'none';
      --'country' vazio e 'species' como 'Genus sp.'**;
2. Continuando com './TaxonQualifier/md_qualifier.py', como a lógica do processo de qualificação é baseada no voucher, poderemos partir para a busca de voucher. 
   -Tentando preencher 'voucher' vazio (ou == 'none') e/ou 'Genus sp.'** (mas se genus** forem distintos, manter ambos pipe) e/ou 'country', buscar o código GB existente em artigos primeiro com 'title', se existente, e depois buscando no 'google' com o código GB e recuperando o título do artigo.
      --Aqui podemos discutir a ordem de recuperação e parseamento. Mas acredito que podemos fazer: CrossRef → busca DOI pelo título -> Unpaywall/EuropePMC → tenta PDF OA -> (se não tiver OA) Sci-Hub (fallback) → PDF pago -> Docling → PDF → Markdown -> Regex/LLM → extrai localidade e/ou nome da espécie e/ou voucher do texto/tabela GB. Podemos dar preferência às tabelas.
      --Será necessário fazer uma busca a material suplementar caso não encontre a tabela de GB dentro do artigo. No material suplementar pode ser encontrado .docx, .xlsx, .pdf, etc.
      --Podemos refinar isso aqui, pois tenho já algumas coisas prontas (comento no fim). Algo importante, que deveremos discutir também em outras partes do projeto, é manter um banco de dados, que inclusive pode ser consultado aqui (neste workflow que tenta preencher lacunas) antes de tudo (antes de ir buscar na internet), para ver se já não tem o artigo (e melhor o .md ou dicionário como sugerido na sequência) em questão. Inclusive, talvez seja interessante pensar em metadados que facilitem o parseamento nesse acesso do banco de dados, por exemplo, manter um .json ou .parquet ou .csv com o nome do artigo (como a key?) e em values, colocar os códigos GB, vouchers, nome das espécies e country (e o path do .mb existente?). Ou será que seria melhor fazermos um .json ou .parquet ou .csv das tabelas GB de cada artigo assim que ele é encontrado pela primeira vez? Assim, nas próximas, o fluxo já busca dentro dos dicionários do bando de dados.
     --Mesmo vindo até aqui ainda é possível que nem tudo se resolva e se tivermos countryless, ainda temos uma última chance como abaixo.
3. Se 'country' persistir vazio, poderemos seguir com GBIF/iDigBio (speciesLink e/ou Mycoportal já estão no GBIF?) → busca voucher direto. E aqui para usar os vouchers, deve-se consultar o 'voucher_dict' e usar todos os values como query de busca para tentar encontrar o country.

Depois de toda essa busca e complementações. Talvez precise de reorganizações das linhas dos 'genus_ouput_dm.*. Ou seja, com base nos vouchers (!!! e aqui podemos nos concentrar nos novos vouchers), precisamos implementar uma lógica de verificação a cada novo voucher, se ele faz parte de algum voucher array (voucher_dict.json) e assim se juntar a mesma linha conforme o gene.

Pipeline existente, que está funcionando parcialmente para 'CrossRef → busca DOI pelo título -> Unpaywall/EuropePMC → tenta PDF OA -> (se não tiver OA) Sci-Hub (fallback) → PDF pago -> Docling → PDF → Markdown -> Regex/LLM' e posterior recuperação dos dados dos artigos:
!!!!! a partir daqui tenho que preencher, mas você pode sugerir também
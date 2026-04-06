# 📚 DatasetMaker - Documentação de Contexto para IA

> **Última atualização:** Fevereiro 2026  
> **Status:** Em desenvolvimento ativo

---

## 🎯 Visão Geral do Projeto

**DatasetMaker** é um pipeline automatizado para construção de datasets taxonômicos de fungos, integrando dados do **GenBank/NCBI** com qualificação via **Index Fungorum**. O projeto visa automatizar a obtenção, processamento e qualificação de sequências genéticas para estudos filogenéticos.

### Objetivo Principal
Criar datasets de sequências de DNA de fungos com metadados qualificados (vouchers, países, tipos nomenclaturais) para análises filogenéticas, com foco em:
- Redução de trabalho manual na curadoria de dados
- Padronização de vouchers e localidades geográficas
- Identificação automática de material tipo (holótipos, lectótipos, etc.)
- Suporte a complexos de espécies (múltiplos gêneros como dataset único)

---

## 📁 Estrutura do Projeto

```
DatasetMaker_/
├── main.py                      # Entrada principal do pipeline
├── .env                         # Variáveis de ambiente (NCBI_API_KEY)
├── requirements_jupyter.txt     # Dependências Python
├── voucherless_countryless.txt  # Documentação de estratégias futuras
│
├── genbank_inout/               # Módulo de interação com GenBank
│   ├── obtain_seqs.py           # Obtenção de sequências (BLAST + E-utilities)
│   ├── gb_handle.py             # Processamento de arquivos .gb
│   ├── gendict.json             # Dicionário de genes/marcadores (~1700 entradas)
│   ├── genbank_in/              # Arquivos de entrada (gêneros a buscar)
│   │   └── input_*.txt/.fas     # Arquivos de input
│   └── genbank_out/             # Saídas processadas
│       ├── genbank_log.log      # Log geral de processamento
│       └── <Genus>/             # Pasta por gênero
│           ├── gb/              # 📁 Arquivos .gb (GenBank format)
│           │   └── Genus_all_genbank_DDmmmYYYY.gb
│           ├── Genus.log
│           ├── Genus_output_dm.csv
│           ├── Genus_output_dm.xlsx
│           ├── Genus_output_dm.parquet
│           ├── Genus_SpecimensList.csv/.parquet
│           ├── Genus_no_duplicates.parquet
│           ├── Genus_processed.parquet
│           ├── Genus_voucher_dict.json
│           └── Genus_genes_ignorados.log
│
├── TaxonQualifier/              # Qualificação taxonômica via Index Fungorum
│   ├── IFget_types_soap.py      # API SOAP do Index Fungorum
│   ├── country_detector.py      # Detecção de países (pycountry + geopy)
│   ├── countries_json.json      # Mapeamento de variações de nomes de países
│   ├── cache_local_para_pais.json # Cache de geocoding
│   └── types_dict/              # Cache de tipos nomenclaturais
│
├── get-taxonREF/                # Old one - Módulo de obtenção de PDFs/referências
│   ├── scihub.py                # Integração Sci-Hub
│   ├── docling_pdf2md/          # Conversão PDF → Markdown
│   ├── porp8/                   # Serviço de busca acadêmica
│   └── md/                      # Artigos convertidos para Markdown
│
├── get_taxon_ref/               # Atual conforme ~/IMPLEMENTATION_CONTEXT.md - 🚧 EM CONSTRUÇÃO
│
├── type_qualifier/              # 🚧 EM CONSTRUÇÃO 
│   └── (limpar dados sobre tipos vindos do genbank; atualizar vouchers que são tipo baseado nos dados taxonômicos vindos dos types_dict; na ausência de material tipo, usar país (enriquecido na etapa anterior) para designar material de referência ancorado na localidade do tipo)
│
├── dereplicate/                 # 🚧 EM CONSTRUÇÃO
│   └── (dereplicação de sequências com CD-HIT)
│
└── phylogeny/                   # 🚧 EM CONSTRUÇÃO
    └── (pipeline filogenético futuro)
```

---

## 🔄 Pipeline Principal (3 Etapas)

### Etapa 1: Obtenção de Sequências (`--obtain`)
**Arquivo:** [genbank_inout/obtain_seqs.py](genbank_inout/obtain_seqs.py)

- **Input:** Arquivo `input_*.txt` com lista de gêneros OU arquivo `input_*.fas` com sequências FASTA
- **Processo:** 
  - Se `.txt`: busca direta no GenBank via E-utilities por gênero
  - Se `.fas`: executa BLAST para identificar sequências similares
  - Detecção automática de última data de download (evita re-downloads)
  - Suporte a modo complexo (`-s`) para combinar múltiplos gêneros
- **Output:** Arquivos `.gb` (GenBank format) em `genbank_out/<Genus>/gb/`
  - Formato: `Genus_all_genbank_DDmmmYYYY.gb` (ex: `Fomitiporia_all_genbank_22feb2026.gb`)

```bash
python main.py --obtain
python main.py -o
python main.py --obtain -f             # Força download completo (ignora .gb existentes)
python main.py --obtain Fomitiporia    # Apenas um gênero específico
python main.py -o -s                   # Modo complexo de espécies
python main.py -o -s -c MeuComplexo    # Modo complexo com nome customizado
```

### Etapa 2: Processamento GenBank (`--gb_handle`)
**Arquivo:** [genbank_inout/gb_handle.py](genbank_inout/gb_handle.py)

- **Input:** Arquivos `.gb` em `genbank_out/<Genus>/gb/`
- **Processo:**
  - Extração de anotações: Species, GBn (accession), Description, bp, Title, Sequence
  - Extração de qualifiers: strain, specimen_voucher, isolate, culture_collection, bio_material, geo_loc_name, host, type_material
  - Classificação de genes usando `gendict.json` + fallback regex (TEF1, RPB1, RPB2, TUB, ATP6)
  - Consolidação de vouchers (normalização e agrupamento)
  - Geração de `voucher_dict.json` por gênero
- **Output:** Múltiplos arquivos na pasta do gênero:
  - `Genus_output_dm.csv/.xlsx/.parquet` - Dataset completo
  - `Genus_SpecimensList.csv/.parquet` - Lista de espécimes
  - `Genus_no_duplicates.parquet` - Sem duplicatas
  - `Genus_processed.parquet` - Processado para qualificação
  - `Genus_voucher_dict.json` - Dicionário de vouchers

```bash
python main.py --gb_handle
python main.py -g
python main.py -g Fomitiporia  # Processar apenas um gênero
python main.py -g -T 4         # Usar 4 threads
```

### Etapa 3: Qualificação Taxonômica (`--qualify`)
**Arquivo:** [TaxonQualifier/IFget_types_soap.py](TaxonQualifier/IFget_types_soap.py)

- **Input:** Arquivos `.parquet` gerados na Etapa 2
- **Processo:**
  - Consulta API SOAP do Index Fungorum por espécie
  - Extração de tipos nomenclaturais (Holotype, Epitype, Lectotype, etc.)
  - Verificação híbrida HTML para capturar tipos adicionais
  - Detecção de país via `country_detector.py` (pycountry + geopy + cache)
- **Output:** Arquivos qualificados com coluna `is_type` e informações de tipos

```bash
python main.py --qualify
python main.py -q
python main.py -q Lentinus  # Qualificar apenas um gênero
```

---

## 🧬 Marcadores Genéticos Suportados

O arquivo [gendict.json](genbank_inout/gendict.json) contém ~1700 variações de descrições para os seguintes marcadores:

| Marcador | Descrição |
|----------|-----------|
| **ITS** | Internal Transcribed Spacer (ITS1-5.8S-ITS2) |
| **nrLSU** | Nuclear ribosomal Large Subunit (28S) |
| **nrSSU** | Nuclear ribosomal Small Subunit (18S) |
| **mtSSU** | Mitochondrial Small Subunit (12S/16S) |
| **TEF1** | Translation Elongation Factor 1-alpha |
| **RPB1** | RNA Polymerase II largest subunit |
| **RPB2** | RNA Polymerase II second largest subunit |
| **TUB** | Beta-tubulin |
| **ATP6** | ATP synthase subunit 6 |
| **COI** | Cytochrome c oxidase subunit I |

---

## 🌍 Detecção de País

**Arquivo:** [TaxonQualifier/country_detector.py](TaxonQualifier/country_detector.py)

Sistema em cascata para detectar país a partir de strings de localidade:

1. **Lookup direto** → `countries_json.json` (variações manuais)
2. **pycountry** → Nomes oficiais ISO
3. **geopy/Nominatim** → Geocoding reverso (com cache persistente)
4. **Cache local** → `cache_local_para_pais.json`

---

## ⚙️ Opções de Linha de Comando

```bash
python main.py [GENUS] [OPTIONS]

Argumentos posicionais:
  GENUS                   Gênero específico para processar (todas as etapas)

Etapas:
  -o, --obtain            Etapa 1: Obter sequências do GenBank
  -g, --gb_handle [GENUS] Etapa 2: Processar arquivos GenBank
  -q, --qualify [GENUS]   Etapa 3: Qualificação taxonômica

Performance:
  -T, --threads N         Número de threads (padrão: 2x CPU cores)
  --no-parallel           Modo sequencial (debug)

Controle de execução:
  -f, --force-run         Ignora .gb existentes e baixa/processa do zero

Modo Complexo de Espécies:
  -s, --sppcomplex-mode   Combina múltiplos gêneros em dataset único
  -c, --complex-name NAME Nome customizado para pasta do complexo

Exemplos:
  python main.py                         # Todas as etapas, todos os gêneros
  python main.py Fomitiporia             # Todas as etapas para Fomitiporia
  python main.py --obtain --gb_handle    # Apenas etapas 1 e 2
  python main.py -g -T 4                 # Etapa 2 com 4 threads
  python main.py -o -f                   # Download forçado (ignora existentes)
  python main.py -s                      # Modo complexo: combina gêneros do input
  python main.py -s -c Wrightoporia_clade # Complexo com nome customizado
```

---

## 📦 Dependências Principais

- **biopython** - Manipulação de sequências e arquivos GenBank
- **pandas** - Processamento de dados tabulares
- **zeep** - Cliente SOAP para Index Fungorum
- **pycountry** - Nomes de países ISO
- **geopy** - Geocoding
- **openpyxl** - Exportação Excel
- **requests** - Requisições HTTP

---

## 🗂️ Formato de Entrada

### Arquivo de gêneros (`input_*.txt`)
```
Fomitiporia
Lentinus
Agaricus
```

### Arquivo FASTA (`input_*.fas`)
```fasta
>specimen_001
ATCGATCGATCG...
>specimen_002
GCTAGCTAGCTA...
```

---

## 📊 Formato de Saída

Os arquivos de saída (`.csv`, `.xlsx`, `.parquet`) estão localizados em `genbank_out/<Genus>/` e contêm as seguintes colunas:

| Coluna | Descrição |
|--------|-----------|
| `Species` | Nome da espécie |
| `GBn` | Accession number do GenBank |
| `Description` | Descrição do registro |
| `bp` | Tamanho da sequência |
| `Title` | Título da publicação |
| `voucher_dict` | Dicionário consolidado de vouchers |
| `geo_loc_name` | Localidade geográfica |
| `host` | Organismo hospedeiro |
| `type_material` | Informação de material tipo |
| `is_type` | Flag booleana de material tipo |
| `ITS`, `TEF1`, `RPB1`... | Sequências por marcador |

### Arquivos por Gênero
```
genbank_out/<Genus>/
├── gb/                              # Arquivos GenBank brutos
│   └── Genus_all_genbank_DDmmmYYYY.gb
├── Genus.log                        # Log de processamento
├── Genus_output_dm.csv/.xlsx/.parquet  # Dataset principal
├── Genus_SpecimensList.csv/.parquet    # Lista de espécimes
├── Genus_no_duplicates.parquet         # Sem duplicatas
├── Genus_processed.parquet             # Para qualificação
├── Genus_voucher_dict.json             # Mapeamento de vouchers
└── Genus_genes_ignorados.log           # Genes não classificados
```

---

## 🚧 Módulos em Construção

### `/dereplicate`
**Objetivo:** Dereplicação de sequências usando CD-HIT para reduzir redundância em datasets.

**Funcionalidades planejadas:**
- Clusterização por similaridade (95-99%)
- Priorização de espécimes com múltiplos marcadores
- Preferência por material tipo e localidade tipo
- Manutenção mínima de 2 representantes por espécie

**Arquivo principal:** [dereplicate/derep_its_multi_reps.py](dereplicate/derep_its_multi_reps.py)

### `/phylogeny`
**Objetivo:** Pipeline automatizado para análise filogenética.

**Funcionalidades planejadas:**
- Alinhamento de sequências
- Seleção de modelo evolutivo
- Construção de árvores (ML, Bayesiana)
- Visualização de árvores

---

## 🔮 Roadmap / TODOs

Documentado em [voucherless_countryless.txt](voucherless_countryless.txt):

1. **Recuperação de vouchers ausentes:**
   - Busca em tabelas de artigos via DOI/CrossRef
   - Pipeline: CrossRef → Unpaywall → Sci-Hub → Docling → Regex/LLM

2. **Atualização de nomes de espécies:**
   - Atualizar registros depositados como "Genus sp."
   - Verificar se texto após "sp." é voucher (ex: "Wrightoporia sp. FL01")

3. **Busca em bases de dados de espécimes:**
   - GBIF, iDigBio, SpeciesLink, MycoPortal
   - Usar vouchers para recuperar país de coleta

4. **Banco de dados de artigos processados:**
   - Cache de artigos já parseados
   - Metadados: DOI, códigos GB, vouchers, espécies, países

---

## 🔑 Configurações Necessárias

### Variáveis de Ambiente (.env)
O arquivo `.env` na raiz do projeto deve conter:
```env
NCBI_API_KEY=sua_api_key_aqui
```

Obtenha sua API Key em: https://www.ncbi.nlm.nih.gov/account/settings/

### Outras API Keys (opcionais)
- **BHL:** Salvar em `get-taxonREF/bhl-api-key.txt`

### Index Fungorum
- Acesso via WSDL: `https://www.indexfungorum.org/ixfwebservice/fungus.asmx?WSDL`
- Não requer API key

---

## 💡 Dicas para Desenvolvimento

1. **Debug:** Use `--no-parallel` para execução sequencial e logs mais claros
2. **Testes parciais:** Use argumentos posicionais para testar com um único gênero
3. **Cache:** O sistema de cache de países em `cache_local_para_pais.json` é persistente
4. **Parquet:** O sistema usa Parquet internamente quando `USE_PARQUET = True` para melhor performance
5. **Force run:** Use `-f` para forçar reprocessamento ignorando arquivos existentes

---

## 📝 Notas para Contexto de IA

### Use em testes e pra valer o ambiente abaixo
- ~/jupyter/bin/activate

### Estrutura de pastas importante:
- Arquivos `.gb` ficam em `genbank_out/<Genus>/gb/` (NÃO na raiz do gênero)
- Nome dos arquivos `.gb` inclui data: `Genus_all_genbank_DDmmmYYYY.gb`
- O sistema detecta automaticamente a última data de download para evitar re-downloads

### Variável de ambiente
- Configure e organize credenciais e dados sensíveis em './.env' e use os.getenv()
- Variáveis de ambiente são carregadas via `python-dotenv`

### Ao modificar código:
- O arquivo `gendict.json` é crítico para classificação de genes - modificar somente quando claramente explícito
- Vouchers são normalizados removendo espaços, dois-pontos, hífens e texto entre parênteses
- O país é extraído de `geo_loc_name` (campo preferido) OU `country` (fallback)

### Ao adicionar novos genes:
1. Adicionar entradas em `gendict.json`
2. Se necessário, criar regex fallback em `FALLBACK_REGEX` no `gb_handle.py`

### Ao debugar:
- Logs são salvos em `genbank_out/<Genus>/<Genus>.log`
- Log geral em `genbank_out/genbank_log.log`
- Usar `grep_search` para encontrar padrões específicos nos arquivos de saída

### Modo Complexo de Espécies:
- Use `-s` quando quiser combinar múltiplos gêneros em um único dataset
- Útil para clados onde gêneros estão misturados ou em revisão taxonômica
- Nome padrão: `<primeiro_gênero>_complex`, customizável com `-c`

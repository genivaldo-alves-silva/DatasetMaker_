# DatasetMaker

DatasetMaker is a fungal taxonomy dataset pipeline that automates sequence retrieval, GenBank parsing, taxonomic qualification, and literature-based enrichment.

The project combines data from GenBank/NCBI with Index Fungorum metadata to produce cleaner, analysis-ready datasets for phylogenetic workflows.

## What this project does

- Retrieves sequences from GenBank (text list of genera or FASTA-driven flow)
- Parses `.gb` files and standardizes metadata fields
- Qualifies nomenclatural type information via Index Fungorum
- Enriches records with article-based validation in `get_taxon_ref_`
- Supports species-complex mode to process multiple genera as a single dataset

## Main project structure

```
DatasetMaker_/
|- main.py
|- .env
|- requirements_jupyter.txt
|- CONTEXT_README.md
|- genbank_inout/
|  |- obtain_seqs.py
|  |- gb_handle.py
|  |- gendict.json
|  |- genbank_in/
|  |- genbank_out/
|- TaxonQualifier/
|  |- IFget_types_soap.py
|  |- country_detector.py
|  |- countries_json.json
|  |- cache_local_para_pais.json
|- get_taxon_ref_/
|  |- md_qualifier.py
|  |- phase0_detection.py ... phase7_consolidation.py
|  |- run_all_pdfs.py
|  |- test_*.py
|- dereplicate/
|- type_qualifier/
|- phylogeny/
```

## Prerequisites

- Linux (recommended, current development environment)
- Python 3.10+ (3.11 recommended)
- Git

## Environment setup

1. Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements_jupyter.txt
```

3. Create `.env` in project root:

```env
NCBI_API_KEY=your_ncbi_api_key_here
```

Get your NCBI key from:
`https://www.ncbi.nlm.nih.gov/account/settings/`

Optional:
- BHL key file: `get-taxonREF/bhl-api-key.txt`

## Running the pipeline

General form:

```bash
python main.py [GENUS] [OPTIONS]
```

### Stage 1: Obtain sequences (`--obtain`)

```bash
python main.py --obtain
python main.py --obtain Fomitiporia
python main.py --obtain -f
```

### Stage 2: Handle GenBank files (`--gb_handle`)

```bash
python main.py --gb_handle
python main.py --gb_handle Fomitiporia
python main.py -g -T 4
```

### Stage 3: Taxonomic qualification (`--qualify`)

```bash
python main.py --qualify
python main.py --qualify Lentinus
```

### Stage 4: Article-based enrichment (`--enrich`)

```bash
python main.py --enrich
python main.py --enrich Fomitiporia
python main.py --enrich --review-diff
```

### Species complex mode

Use this mode when multiple genera should be processed as one combined dataset:

```bash
python main.py -s
python main.py -s -c Wrightoporia_clade
python main.py -o -s -c MeuComplexo
```

### Full runs and mixed runs

```bash
python main.py
python main.py Fomitiporia
python main.py --obtain --gb_handle
python main.py --no-parallel
```

## Inputs and outputs

### Inputs
- Genus or sequence inputs in `genbank_inout/genbank_in/`
- Typical accepted patterns in codebase: `input_*.txt` and `input_*.fas`

### Core outputs
- `genbank_inout/genbank_out/<Genus>/gb/*.gb`
- `genbank_inout/genbank_out/<Genus>/*_output_dm.csv|xlsx|parquet`
- `genbank_inout/genbank_out/<Genus>/*_SpecimensList.*`
- `genbank_inout/genbank_out/<Genus>/*_voucher_dict.json`
- Enrichment artifacts under `get_taxon_ref_/` (logs, downloads, sandbox outputs)

## Running tests

Current tests are concentrated in `get_taxon_ref_`:

```bash
pytest -q get_taxon_ref_/test_phase2_articles_db.py
pytest -q get_taxon_ref_/test_pdf_extraction.py
pytest -q get_taxon_ref_/test_phase5_validation.py
pytest -q get_taxon_ref_/test_phase6_5_helper.py
pytest -q get_taxon_ref_/test_phase6_5_pdf_country.py
pytest -q get_taxon_ref_/test_phase6_phase7.py
```

Run all listed tests:

```bash
pytest -q get_taxon_ref_/test_*.py
```

## Troubleshooting

- Use `--no-parallel` for easier debugging.
- Use `-f` to force fresh runs when cached files block expected updates.
- Check genus logs in `genbank_inout/genbank_out/<Genus>/<Genus>.log`.
- Check global processing log in `genbank_inout/genbank_out/genbank_log.log`.
- If country parsing behaves unexpectedly, inspect `TaxonQualifier/cache_local_para_pais.json`.

## Notes

- This repository also contains work-in-progress modules (`dereplicate`, `type_qualifier`, `phylogeny`).
- For deeper implementation context, see `CONTEXT_README.md` and `get_taxon_ref_/IMPLEMENTATION_CONTEXT.md`.

#!/usr/bin/env python3
"""
Processa todos os PDFs de uma pasta, extraindo accession codes e gerando CSVs.

Uso:
  python -m get_taxon_ref_.run_all_pdfs                          # pasta padrão (downloads/)
  python -m get_taxon_ref_.run_all_pdfs /caminho/para/pdfs       # pasta custom
  python -m get_taxon_ref_.run_all_pdfs --outdir /caminho/saida  # diretório de saída custom

Fluxo por PDF:
  1. Extrai accession codes via pdfplumber (regex)
  2. Se pdfplumber não encontrar nenhum, tenta PyMuPDF (lida com tabelas rotacionadas)
  3. Chama find_accession_info() para cada accession → species, voucher, country
  4. Salva CSV no diretório de saída
"""

import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from get_taxon_ref_.phase4_pdf_extraction_v2 import extract_text_lines
from get_taxon_ref_.test_pdf_extraction import test_multi_csv

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

DEFAULT_PDF_DIR = Path(__file__).resolve().parent.parent / "get-taxonREF" / "downloads"
DEFAULT_OUT_DIR = Path(__file__).resolve().parent / "sandbox_csv"

ACCESSION_RE = re.compile(r'\b(?:[A-Z]\d{5}|[A-Z]{2}\d{6,8})(?:[a-z])?\b')
ACCESSION_LINE_RE = re.compile(r'^(?:[A-Z]\d{5}|[A-Z]{2}\d{6,8})(?:[a-z])?$')


def extract_accessions_pdfplumber(pdf_path: str) -> list[str]:
    """Extrai accessions via pdfplumber (funciona para a maioria dos PDFs)."""
    if not HAS_PDFPLUMBER:
        return []
    accessions = set()
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    accessions.update(ACCESSION_RE.findall(text))
    except Exception as e:
        print(f"    ⚠ pdfplumber falhou: {e}")
        return []
    return sorted(a for a in accessions if not a.startswith('MB'))


def extract_accessions_pymupdf(pdf_path: str) -> list[str]:
    """Extrai accessions via PyMuPDF (fallback para tabelas rotacionadas)."""
    lines = extract_text_lines(pdf_path)
    accessions = set()
    for line in lines:
        stripped = line.strip()
        if ACCESSION_LINE_RE.match(stripped):
            accessions.add(stripped)
        else:
            accessions.update(ACCESSION_RE.findall(stripped))
    return sorted(a for a in accessions if not a.startswith('MB'))


def process_all_pdfs(pdf_dir: Path, out_dir: Path):
    """Processa todos os PDFs do diretório."""
    pdfs = sorted(pdf_dir.glob("*.pdf"))
    if not pdfs:
        print(f"Nenhum PDF encontrado em {pdf_dir}")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*78}")
    print(f"  Processando {len(pdfs)} PDFs de: {pdf_dir.name}/")
    print(f"  Saída em: {out_dir}")
    print(f"{'='*78}\n")

    summary = []
    t0_all = time.time()

    for i, pdf_path in enumerate(pdfs, 1):
        name = pdf_path.stem
        short_name = name[:65] + "..." if len(name) > 65 else name
        csv_name = re.sub(r'[^\w\-.]', '_', name[:60]).strip('_') + "_results.csv"
        csv_path = out_dir / csv_name

        print(f"\n{'─'*78}")
        print(f"  [{i}/{len(pdfs)}] {short_name}")
        print(f"{'─'*78}")

        # 1. Tentar pdfplumber
        accessions = extract_accessions_pdfplumber(str(pdf_path))
        source = "pdfplumber"

        # 2. Fallback: PyMuPDF (tabelas rotacionadas, etc.)
        if not accessions:
            print("    pdfplumber: 0 accessions → tentando PyMuPDF...")
            accessions = extract_accessions_pymupdf(str(pdf_path))
            source = "pymupdf"

        if not accessions:
            print(f"    ❌ Nenhum accession encontrado (pdfplumber + PyMuPDF)")
            summary.append((short_name, 0, 0, 0, "—"))
            continue

        print(f"    {len(accessions)} accessions via {source}\n")

        t0 = time.time()
        test_multi_csv(str(pdf_path), accessions, csv_path=str(csv_path))
        elapsed = time.time() - t0

        # Contar encontrados no CSV gerado
        lines_csv = csv_path.read_text().strip().splitlines()
        total = len(accessions)
        found = sum(1 for l in lines_csv[1:] if ',not_found' not in l)
        summary.append((short_name, total, found, elapsed, csv_name))

    elapsed_all = time.time() - t0_all

    # Sumário final
    print(f"\n\n{'='*78}")
    print(f"  SUMÁRIO — {len(pdfs)} PDFs processados em {elapsed_all:.1f}s")
    print(f"{'='*78}\n")
    print(f"  {'PDF':<50} {'Total':>6} {'Found':>6} {'%':>6}  {'Tempo':>7}")
    print(f"  {'─'*50} {'─'*6} {'─'*6} {'─'*6}  {'─'*7}")

    grand_total = 0
    grand_found = 0
    for name, total, found, elapsed, csv_name in summary:
        pct = f"{found/total*100:.0f}%" if total > 0 else "—"
        tm = f"{elapsed:.1f}s" if elapsed > 0 else "—"
        print(f"  {name:<50} {total:>6} {found:>6} {pct:>6}  {tm:>7}")
        grand_total += total
        grand_found += found

    pct_all = f"{grand_found/grand_total*100:.1f}%" if grand_total > 0 else "—"
    print(f"  {'─'*50} {'─'*6} {'─'*6} {'─'*6}  {'─'*7}")
    print(f"  {'TOTAL':<50} {grand_total:>6} {grand_found:>6} {pct_all:>6}  {elapsed_all:>6.1f}s")
    print(f"\n  CSVs em: {out_dir.resolve()}\n")


def main():
    args = sys.argv[1:]
    pdf_dir = DEFAULT_PDF_DIR
    out_dir = DEFAULT_OUT_DIR

    i = 0
    while i < len(args):
        if args[i] == "--outdir" and i + 1 < len(args):
            out_dir = Path(args[i + 1])
            i += 2
        elif not args[i].startswith("-"):
            pdf_dir = Path(args[i])
            i += 1
        else:
            i += 1

    if not pdf_dir.is_dir():
        print(f"Diretório não encontrado: {pdf_dir}")
        sys.exit(1)

    process_all_pdfs(pdf_dir, out_dir)


if __name__ == "__main__":
    main()

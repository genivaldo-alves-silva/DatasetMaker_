#!/usr/bin/env python3
"""
Script para testar extração de accession codes de PDFs.

Uso:
  # Modo interativo (pergunta PDF e accessions, gera CSV):
  python -m get_taxon_ref_.test_pdf_extraction

  # Modo direto (1 accession):
  python -m get_taxon_ref_.test_pdf_extraction <pdf_path> <accession>

  # Múltiplos accessions → tabela + CSV:
  python -m get_taxon_ref_.test_pdf_extraction <pdf_path> ACC1,ACC2,ACC3
  python -m get_taxon_ref_.test_pdf_extraction <pdf_path> ACC1 ACC2 ACC3

  # Listar todos os registros de um PDF:
  python -m get_taxon_ref_.test_pdf_extraction <pdf_path> --all

  # MODO AUTO: extrai accessions do PDF via regex e processa automaticamente:
  python -m get_taxon_ref_.test_pdf_extraction <pdf_path> --auto
  python -m get_taxon_ref_.test_pdf_extraction <pdf_path> --auto output.csv

  # Rodar suíte de testes de regressão (com valores esperados):
  python -m get_taxon_ref_.test_pdf_extraction --batch

  # Rodar apenas os testes de um PDF específico:
  python -m get_taxon_ref_.test_pdf_extraction --batch Phylloporia

  # Rodar apenas um test-case por nome:
  python -m get_taxon_ref_.test_pdf_extraction --batch --case JX093771_fomitiporia
"""

import csv
import io
import sys
import time
import json
import glob
from pathlib import Path
from typing import Optional

# Garantir que o módulo é importável
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from get_taxon_ref_.phase4_pdf_extraction_v2 import (
    find_accession_info,
    extract_all_rows_from_pdf,
    extract_text_lines,
    find_table_headers,
)

import re
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# Diretório padrão de PDFs
DEFAULT_PDF_DIR = Path(__file__).resolve().parent.parent / "get-taxonREF" / "downloads"


# ============================================================================
# Suíte de testes de regressão
# ============================================================================
# Cada entrada: {
#   "name":      identificador único do teste (para --case),
#   "pdf":       nome parcial do PDF (busca em DEFAULT_PDF_DIR),
#   "accession": código GenBank,
#   "expect": {  campos esperados — só verifica os que estiverem presentes
#       "species": "...",
#       "voucher": "...",
#       "country": "...",
#   }
# }
# Para adicionar novos testes, basta acrescentar entradas aqui.
# ============================================================================

BATCH_TESTS: list[dict] = [
    # --- Phylloporia nouraguensis (tabela com coluna Substrate) ---
    {
        "name": "AF311019_phylloporia",
        "pdf": "Phylloporia_nouraguensis",
        "accession": "AF311019",
        "expect": {"species": "I. rheades", "country": "Germany"},
    },
    {
        "name": "KC136225_phylloporia",
        "pdf": "Phylloporia_nouraguensis",
        "accession": "KC136225",
        "expect": {"species": "Phylloporia sp", "country": "Argentina"},
    },
    {
        "name": "AY411825_phylloporia",
        "pdf": "Phylloporia_nouraguensis",
        "accession": "AY411825",
        "expect": {"species": "F. robiniae", "country": "USA"},
    },
    {
        "name": "HM635671_phylloporia",
        "pdf": "Phylloporia_nouraguensis",
        "accession": "HM635671",
        "expect": {"species": "P. minutispora", "country": "Congo"},
    },
    {
        "name": "KC136220_phylloporia",
        "pdf": "Phylloporia_nouraguensis",
        "accession": "KC136220",
        "expect": {"country": "Argentina"},
    },
    {
        "name": "JF712922_phylloporia",
        "pdf": "Phylloporia_nouraguensis",
        "accession": "JF712922",
        "expect": {"species": "P. crataegi", "country": "China"},
    },

    # --- Fomitiporia baccharidis (tabela sem Substrate, forward-fill) ---
    {
        "name": "JX093771_fomitiporia",
        "pdf": "Fomitiporia baccharidis",
        "accession": "JX093771",
        "expect": {"species": "F. apiahyna", "country": "French Guiana"},
    },
    {
        "name": "JQ087932_fomitiporia",
        "pdf": "Fomitiporia baccharidis",
        "accession": "JQ087932",
        "expect": {"species": "F. cupressicola", "country": "Mexico"},
    },
    {
        "name": "AY618204_fomitiporia",
        "pdf": "Fomitiporia baccharidis",
        "accession": "AY618204",
        "expect": {"species": "F. aethiopica", "country": "Ethiopia"},
    },

    # --- Hymenochaetales (5 new species from SW China) ---
    {
        "name": "JQ279559_hymenochaetales",
        "pdf": "Hymenochaetales",
        "accession": "JQ279559",
        "expect": {"species": "Hymenochaete asetosa", "country": "China", "voucher": "Dai 10756"},
    },
]


def _resolve_pdf(name_fragment: str) -> Optional[Path]:
    """Encontra PDF no diretório padrão por nome parcial (case-insensitive)."""
    fragment_lower = name_fragment.lower()
    for p in DEFAULT_PDF_DIR.glob("*.pdf"):
        if fragment_lower in p.name.lower():
            return p
    return None


def _check_field(actual: str, expected: str) -> bool:
    """Compara campo extraído com esperado (case-insensitive, substring ok)."""
    if not expected:
        return True
    if not actual:
        return False
    return expected.lower() in actual.lower()


def run_batch_tests(
    pdf_filter: Optional[str] = None,
    case_filter: Optional[str] = None,
    verbose: bool = False,
) -> tuple[int, int, int]:
    """
    Executa a suíte de testes de regressão.
    
    Args:
        pdf_filter:  se informado, roda só testes cujo 'pdf' contém esse texto
        case_filter: se informado, roda só o teste com esse 'name'
        verbose:     mostra detalhes de cada teste (mesmo os que passam)
    
    Returns:
        (total, passed, failed)
    """
    tests = BATCH_TESTS
    
    if case_filter:
        tests = [t for t in tests if t["name"] == case_filter]
        if not tests:
            # Tenta busca parcial
            tests = [t for t in BATCH_TESTS if case_filter.lower() in t["name"].lower()]
    
    if pdf_filter and not case_filter:
        tests = [t for t in tests if pdf_filter.lower() in t["pdf"].lower()]
    
    if not tests:
        print("  Nenhum teste encontrado com os filtros informados.")
        return 0, 0, 0

    # Agrupa por PDF para reutilizar o texto extraído
    from collections import OrderedDict
    by_pdf: OrderedDict[str, list[dict]] = OrderedDict()
    for t in tests:
        by_pdf.setdefault(t["pdf"], []).append(t)

    total = len(tests)
    passed = 0
    failed = 0
    failures: list[dict] = []

    print(f"\n{'='*74}")
    print(f"  BATCH TEST — {total} caso(s) em {len(by_pdf)} PDF(s)")
    print(f"{'='*74}")

    t0_all = time.time()

    for pdf_frag, cases in by_pdf.items():
        pdf_path = _resolve_pdf(pdf_frag)
        if not pdf_path:
            for c in cases:
                failed += 1
                failures.append({
                    "name": c["name"],
                    "accession": c["accession"],
                    "error": f"PDF não encontrado: '{pdf_frag}'",
                })
            continue

        pdf_name = pdf_path.name
        if len(pdf_name) > 65:
            pdf_name = pdf_name[:62] + "..."
        print(f"\n  📄 {pdf_name}")
        print(f"  {'─'*70}")

        for c in cases:
            acc = c["accession"]
            expect = c.get("expect", {})
            name = c["name"]

            t0 = time.time()
            info = find_accession_info(str(pdf_path), acc)
            elapsed = time.time() - t0

            # Verificar cada campo esperado
            field_results = {}
            all_ok = True

            if info["confidence"] == "not_found":
                all_ok = False
                field_results["_found"] = False
            else:
                field_results["_found"] = True
                for field_name, expected_val in expect.items():
                    actual_val = info.get(field_name, "") or ""
                    ok = _check_field(actual_val, expected_val)
                    field_results[field_name] = {
                        "ok": ok,
                        "actual": actual_val,
                        "expected": expected_val,
                    }
                    if not ok:
                        all_ok = False

            if all_ok:
                passed += 1
                mark = "✅"
                species_show = info.get("species", "") or "—"
                print(f"    {mark} {acc}  {species_show:<28} ({elapsed:.2f}s)  [{name}]")
                if verbose:
                    for fn, fv in field_results.items():
                        if fn.startswith("_"):
                            continue
                        print(f"         {fn}: {fv['actual']!r}")
            else:
                failed += 1
                mark = "❌"
                print(f"    {mark} {acc}  FALHOU  ({elapsed:.2f}s)  [{name}]")

                fail_info = {"name": name, "accession": acc, "details": []}

                if not field_results.get("_found", True):
                    print(f"         Não encontrado no PDF")
                    fail_info["details"].append("Não encontrado no PDF")
                else:
                    for fn, fv in field_results.items():
                        if fn.startswith("_"):
                            continue
                        if not fv["ok"]:
                            print(f"         {fn}: obtido={fv['actual']!r}  esperado={fv['expected']!r}")
                            fail_info["details"].append(
                                f"{fn}: obtido={fv['actual']!r} esperado={fv['expected']!r}"
                            )
                        elif verbose:
                            print(f"         {fn}: {fv['actual']!r} ✓")

                failures.append(fail_info)

    elapsed_all = time.time() - t0_all

    # Sumário
    print(f"\n{'='*74}")
    if failed == 0:
        print(f"  🎉 TODOS PASSARAM: {passed}/{total} testes OK  ({elapsed_all:.2f}s)")
    else:
        print(f"  ⚠  RESULTADO: {passed} OK, {failed} FALHA(S) de {total}  ({elapsed_all:.2f}s)")
        print(f"\n  Falhas:")
        for f in failures:
            details = "; ".join(f.get("details", [f.get("error", "?")]))
            print(f"    • {f['accession']} [{f['name']}]: {details}")
    print(f"{'='*74}\n")

    return total, passed, failed


# ============================================================================
# Funções existentes (test_single, test_all_rows, interactive)
# ============================================================================

def list_pdfs(directory: Path = DEFAULT_PDF_DIR) -> list[Path]:
    """Lista PDFs disponíveis no diretório."""
    return sorted(directory.glob("*.pdf"))


def test_single(pdf_path: str, accession: str):
    """Testa um accession code em um PDF."""
    print(f"\n{'='*70}")
    print(f"  PDF:       {Path(pdf_path).name}")
    print(f"  Accession: {accession}")
    print(f"{'='*70}")

    t0 = time.time()
    info = find_accession_info(pdf_path, accession)
    elapsed = time.time() - t0

    if info["confidence"] == "not_found":
        print(f"\n  ❌ Accession {accession} NÃO encontrado no PDF")
        print(f"     Tempo: {elapsed:.2f}s")
        # Dica: verificar se o accession existe no texto bruto
        lines = extract_text_lines(pdf_path)
        matches = [l for l in lines if accession in l]
        if matches:
            print(f"     ⚠ Porém, '{accession}' aparece no texto bruto ({len(matches)}x):")
            for m in matches[:3]:
                print(f"       → {m[:120]}")
        else:
            print(f"     O código '{accession}' não aparece em nenhuma linha do PDF.")
    else:
        print(f"\n  ✅ ENCONTRADO (confidence: {info['confidence']})")
        print(f"     Species:  {info['species'] or '—'}")
        print(f"     Voucher:  {info['voucher'] or '—'}")
        print(f"     Country:  {info['country'] or '—'}")
        print(f"     Gene:     {info['gene_region'] or '—'}")
        if info.get("other_accessions"):
            print(f"     Outros:   {info['other_accessions']}")
        print(f"     Tempo:    {elapsed:.2f}s")
        print(f"     Método:   {info.get('method', '?')}")


def test_all_rows(pdf_path: str):
    """Extrai e mostra todos os registros de um PDF."""
    print(f"\n{'='*70}")
    print(f"  PDF: {Path(pdf_path).name}")
    print(f"  Modo: extração completa (--all)")
    print(f"{'='*70}")

    t0 = time.time()
    rows = extract_all_rows_from_pdf(pdf_path)
    elapsed = time.time() - t0

    if not rows:
        print(f"\n  ❌ Nenhum registro extraído.")
        # Diagnóstico
        lines = extract_text_lines(pdf_path)
        headers = find_table_headers(lines)
        print(f"     Linhas de texto: {len(lines)}")
        print(f"     Headers detectados: {len(headers)}")
        for h in headers:
            print(f"       gene_cols={h.gene_cols}, n_meta={h.n_meta_cols}")
        return

    print(f"\n  {len(rows)} registros extraídos em {elapsed:.2f}s\n")

    # Determinar colunas presentes
    meta_keys = {"species", "voucher", "country"}
    gene_keys = set()
    for r in rows:
        for k in r:
            if not k.startswith("_") and k not in meta_keys and k not in {"reference", "other_meta"}:
                gene_keys.add(k)
    gene_keys = sorted(gene_keys)

    # Header
    header = f"  {'#':>3}  {'Species':<30} {'Voucher':<22} {'Country':<18}"
    for g in gene_keys:
        header += f" {g:<12}"
    print(header)
    print("  " + "─" * (len(header) - 2))

    for i, r in enumerate(rows, 1):
        line = f"  {i:>3}  {(r.get('species','') or '—'):<30} {(r.get('voucher','') or '—'):<22} {(r.get('country','') or '—'):<18}"
        for g in gene_keys:
            line += f" {(r.get(g,'') or '—'):<12}"
        ff = " [ff]" if r.get("_forward_filled") else ""
        print(line + ff)

    print(f"\n  [ff] = species preenchido por forward-fill")
    print(f"  Total: {len(rows)} registros, {len(gene_keys)} genes ({', '.join(gene_keys)})")


def _parse_accession_list(raw: str) -> list[str]:
    """Aceita accessions separados por vírgula, espaço ou ambos."""
    # Substitui vírgulas por espaço, depois faz split
    return [a.strip() for a in raw.replace(",", " ").split() if a.strip()]


def extract_accessions_from_pdf(pdf_path: str) -> list[str]:
    """
    Extrai accession codes GenBank de um PDF usando regex.
    
    Args:
        pdf_path: caminho do PDF
    
    Returns:
        Lista ordenada de accession codes únicos encontrados.
    """
    if not HAS_PDFPLUMBER:
        raise ImportError("pdfplumber não instalado. Execute: pip install pdfplumber")
    
    # Padrão GenBank típico: 1 letra + 5 dígitos, ou 2 letras + 6-8 dígitos.
    # Alguns PDFs trazem sufixo de nota sobrescrita (ex.: MF977778a).
    pattern = re.compile(r'\b(?:[A-Z][0-9]{5}|[A-Z]{2}[0-9]{6,8})(?:[a-z])?\b')
    accessions = set()
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                accessions.update(pattern.findall(text))
    
    # Filtrar falsos positivos conhecidos
    # MB = MycoBank numbers, não GenBank
    accessions = {a for a in accessions if not a.startswith('MB')}
    
    return sorted(accessions)


def auto_extract_and_test(pdf_path: str, csv_path: Optional[str] = None):
    """
    Modo automático: extrai accessions do PDF e processa.
    
    Args:
        pdf_path: caminho do PDF
        csv_path: nome do arquivo CSV de saída (opcional)
    """
    print(f"\n{'='*70}")
    print(f"  MODO AUTO: extraindo accession codes do PDF...")
    print(f"{'='*70}")
    
    t0 = time.time()
    accessions = extract_accessions_from_pdf(pdf_path)
    elapsed = time.time() - t0
    
    print(f"\n  Encontrados {len(accessions)} accession codes únicos ({elapsed:.2f}s)")
    
    if not accessions:
        print("  ❌ Nenhum accession code encontrado no PDF.")
        return
    
    # Se não informou csv_path, gera nome baseado no PDF
    if csv_path is None:
        csv_path = Path(pdf_path).stem[:40] + "_results.csv"
    
    test_multi_csv(pdf_path, accessions, csv_path=csv_path)


def test_multi_csv(
    pdf_path: str,
    accessions: list[str],
    csv_path: Optional[str] = None,
):
    """
    Testa múltiplos accessions e gera saída tabular + CSV.

    Args:
        pdf_path:   caminho do PDF
        accessions: lista de códigos GenBank
        csv_path:   se informado, salva CSV nesse arquivo; senão pergunta.
    """
    pdf_name = Path(pdf_path).name
    if len(pdf_name) > 70:
        pdf_name = pdf_name[:67] + "..."

    print(f"\n{'='*78}")
    print(f"  PDF: {pdf_name}")
    print(f"  Accessions: {len(accessions)}")
    print(f"{'='*78}\n")

    rows: list[dict] = []
    t0 = time.time()

    for acc in accessions:
        info = find_accession_info(pdf_path, acc)
        rows.append(info)
        mark = "✅" if info["confidence"] != "not_found" else "❌"
        sp = info.get("species") or "—"
        print(f"  {mark} {acc:<14} {sp:<32} {(info.get('country') or '—'):<18} {info.get('confidence','')}")

    elapsed = time.time() - t0
    found = sum(1 for r in rows if r["confidence"] != "not_found")
    print(f"\n  {found}/{len(rows)} encontrados em {elapsed:.2f}s")

    # ---------- Gerar CSV ----------
    fieldnames = ["accession", "species", "voucher", "country", "gene_region", "confidence"]

    # Verificar se há outros campos (other_accessions, method)
    extra_keys = set()
    for r in rows:
        extra_keys.update(k for k in r if k not in fieldnames and not k.startswith("_"))
    fieldnames.extend(sorted(extra_keys))

    # Gerar conteúdo CSV em memória
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    csv_content = buf.getvalue()

    # Mostrar tabela no terminal
    print(f"\n  Prévia CSV ({len(rows)} linhas):")
    print(f"  {'─'*74}")
    for line in csv_content.strip().splitlines()[:22]:  # Até 20 linhas de dados + header
        print(f"    {line}")
    if len(rows) > 20:
        print(f"    ... ({len(rows) - 20} linhas omitidas)")
    print()

    # Salvar
    if csv_path is None:
        default_name = Path(pdf_path).stem[:40] + "_results.csv"
        try:
            csv_path = input(f"  Salvar CSV como [{default_name}]: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n  Cancelado.")
            return
        if not csv_path:
            csv_path = default_name

    out = Path(csv_path)
    out.write_text(csv_content, encoding="utf-8")
    print(f"  💾 CSV salvo em: {out.resolve()}")
    print()


def interactive_mode():
    """Modo interativo: lista PDFs e pede accession."""
    pdfs = list_pdfs()

    if not pdfs:
        print(f"Nenhum PDF encontrado em {DEFAULT_PDF_DIR}")
        print("Informe o caminho completo do PDF como argumento.")
        return

    print(f"\n📂 PDFs disponíveis em {DEFAULT_PDF_DIR.name}/:\n")
    for i, p in enumerate(pdfs, 1):
        name = p.name
        if len(name) > 80:
            name = name[:77] + "..."
        print(f"  [{i:>2}] {name}")

    print(f"\n  [ 0] Informar outro caminho")
    print()

    try:
        choice = input("Escolha o PDF (número): ").strip()
        if not choice:
            return

        idx = int(choice)
        if idx == 0:
            pdf_path = input("Caminho do PDF: ").strip()
        elif 1 <= idx <= len(pdfs):
            pdf_path = str(pdfs[idx - 1])
        else:
            print("Opção inválida.")
            return

        if not Path(pdf_path).exists():
            print(f"Arquivo não encontrado: {pdf_path}")
            return

        print()
        print("  Informe accession code(s) separados por vírgula ou espaço,")
        print("  ou 'all' para extrair todos os registros do PDF.")
        acc_input = input("\n  Accession(s): ").strip()

        if not acc_input:
            return

        if acc_input.lower() == "all":
            test_all_rows(pdf_path)
            return

        accessions = _parse_accession_list(acc_input)

        if len(accessions) == 1:
            test_single(pdf_path, accessions[0])
        else:
            test_multi_csv(pdf_path, accessions)

    except (KeyboardInterrupt, EOFError):
        print("\nSaindo.")


def main():
    args = sys.argv[1:]

    if not args:
        interactive_mode()
        return

    # --batch mode
    if args[0] == "--batch":
        pdf_filter = None
        case_filter = None
        verbose = False
        rest = args[1:]

        while rest:
            if rest[0] == "--case" and len(rest) > 1:
                case_filter = rest[1]
                rest = rest[2:]
            elif rest[0] == "-v" or rest[0] == "--verbose":
                verbose = True
                rest = rest[1:]
            elif not rest[0].startswith("-"):
                pdf_filter = rest[0]
                rest = rest[1:]
            else:
                rest = rest[1:]

        total, passed, failed = run_batch_tests(pdf_filter, case_filter, verbose)
        sys.exit(0 if failed == 0 else 1)

    pdf_path = args[0]

    if not Path(pdf_path).exists():
        print(f"Arquivo não encontrado: {pdf_path}")
        sys.exit(1)

    # --auto mode: extrai accessions automaticamente do PDF
    if len(args) > 1 and args[1] == "--auto":
        csv_path = args[2] if len(args) > 2 else None
        auto_extract_and_test(pdf_path, csv_path)
        return

    if len(args) == 1 or args[1] == "--all":
        test_all_rows(pdf_path)
    else:
        accessions = []
        for acc in args[1:]:
            if acc.startswith("-"):
                continue
            accessions.extend(_parse_accession_list(acc))

        if len(accessions) == 1:
            test_single(pdf_path, accessions[0])
        elif accessions:
            test_multi_csv(pdf_path, accessions)


if __name__ == "__main__":
    main()

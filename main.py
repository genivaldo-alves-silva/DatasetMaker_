'''
DatasetMaker - Pipeline de Processamento Taxonômico

Uso:
    python main.py                         # Executa todas as etapas
    python main.py Fomitiporia             # Executa todas as etapas para Fomitiporia
    python main.py --obtain                # Etapa 1: Obter sequências do GenBank
    python main.py --obtain Fomitiporia    # Etapa 1 apenas para Fomitiporia
    python main.py --obtain -f Fomitiporia # Etapa 1, força download completo
    python main.py --obtain --force-run    # Ignora .gb existentes, baixa tudo
    python main.py --gb_handle             # Etapa 2: Processar arquivos GenBank
    python main.py --gb_handle Fomitiporia # Etapa 2 apenas para Fomitiporia
    python main.py --qualify               # Etapa 3: Qualificação taxonômica
    python main.py --qualify Lentinus      # Etapa 3 apenas para Lentinus
    python main.py --enrich                # Etapa 4: Enriquecimento por artigos (Fases 0-5)
    python main.py --enrich Fomitiporia    # Etapa 4 apenas para Fomitiporia
    python main.py --obtain --gb_handle    # Executa etapas 1 e 2
    python main.py -T 8                    # Usa 8 threads para processamento paralelo
    python main.py -g -T 4                 # Processa GenBank com 4 threads
    python main.py --no-parallel           # Modo sequencial (útil para debug)
    
    # Modo complexo de espécies (múltiplos gêneros como dataset único):
    python main.py -s                      # Combina gêneros do input_*.txt em pasta <primeiro>_complex/
    python main.py -s -c Wrightoporia_clade # Define nome do complexo manualmente
    python main.py --sppcomplex-mode --complex-name MeuComplexo  # Forma longa

TODO:
- Se os diferentes gêneros da lista de .txt devem ser processados juntos
 ✅ FEITO: --sppcomplex-mode
Se o usuário souber, poderá ser informado:
    - % da clusterização; - outgroup; -região do DNA mais informativa;
- como está quando tiver tanto .txt quanto .fas?
- pensar em como organizar e disponibilizar os produtos intermediários
- de gêneros especiosos, como Agaricus, pensar no futuro em disponibilizar clados 
específicos para trabalharem. Certamente será após uma grande análise.
- Nas filogenias, será útil utilizar o tamanho dos ramos excessivos para saber se tem 
algum espécime com sequências muito ruins. Ou seria possível filtrar isso na dereplicação? Talvez sim, principalmente quando tiver somente uma das regiões de DNA. Pensando que será utilizado para gerar a filogenia single.
- Pensar no Plano pago para usar APIs como do firecrawn, ou zyte, etc;
- Construir um banco de dados com os gêneros maiores. Assim o processo será mais rápido;
'''

import argparse
import asyncio
import sys
import os
from pathlib import Path

# Adiciona o diretório raiz ao path para imports corretos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

# Caminhos centralizados do projeto
GENBANK_IN_DIR = os.path.join(BASE_DIR, "genbank_inout", "genbank_in")
GENBANK_OUT_DIR = os.path.join(BASE_DIR, "genbank_inout", "genbank_out")

from genbank_inout.obtain_seqs import process_genbank_inputs
from genbank_inout.gb_handle import gb_handle
from TaxonQualifier.IFget_types_soap import process_genus, process_all_genuses
from get_taxon_ref_.md_qualifier import qualify_genus_data
from get_taxon_ref_.quick_review_diff import generate_review_csvs


def parse_args():
    """Configura e processa argumentos de linha de comando."""
    parser = argparse.ArgumentParser(
        description="DatasetMaker - Pipeline de Processamento Taxonômico",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python main.py                         # Executa todas as etapas
  python main.py Fomitiporia             # Todas as etapas para Fomitiporia
  python main.py --obtain                # Apenas obtém sequências do GenBank
  python main.py --obtain -f             # Obtém sequências, ignorando .gb existentes
  python main.py --obtain -f Fomitiporia # Força download completo de Fomitiporia
  python main.py --gb_handle             # Processa arquivos .gb de todos os gêneros
  python main.py --gb_handle Fomitiporia # Processa apenas Fomitiporia
  python main.py --qualify               # Qualificação taxonômica
  python main.py --qualify Lentinus      # Qualifica apenas Lentinus
    python main.py --enrich                # Enriquecimento via artigos (Fases 0-5)
    python main.py --enrich Fomitiporia    # Enriquece apenas Fomitiporia
  python main.py --obtain --gb_handle    # Executa etapas 1 e 2
  python main.py -T 8                    # Usa 8 threads para processamento
  python main.py -g -T 4                 # Processa GenBank com 4 threads
  python main.py --no-parallel           # Executa sequencialmente (debug)
  
  # Modo complexo de espécies (múltiplos gêneros como dataset único):
  python main.py -s                      # Combina gêneros do input em <primeiro>_complex/
  python main.py -s -c Wrightoporia_clade # Nome customizado para o complexo
        """
    )
    
    # Flags para etapas específicas
    parser.add_argument(
        "--obtain", "-o",
        action="store_true",
        help="Etapa 1: Obter sequências do GenBank (genbank_in → genbank_out/*.gb)"
    )
    parser.add_argument(
        "--gb_handle", "-g",
        nargs="?",
        const=True,
        default=False,
        metavar="GENUS",
        help="Etapa 2: Processar arquivos GenBank (.csv, .xlsx, .parquet). Opcionalmente especifique um gênero."
    )
    parser.add_argument(
        "--qualify", "-q",
        nargs="?",
        const=True,
        default=False,
        metavar="GENUS",
        help="Etapa 3: Qualificação taxonômica via Index Fungorum. Opcionalmente especifique um gênero."
    )
    parser.add_argument(
        "--enrich", "-e",
        nargs="?",
        const=True,
        default=False,
        metavar="GENUS",
        help="Etapa 4: Enriquecimento via artigos (Fases 0-5). Opcionalmente especifique um gênero."
    )
    parser.add_argument(
        "--review-diff",
        action="store_true",
        help="Após --enrich, gera CSVs de revisão rápida (modified_enriched/original/removed)."
    )
    
    # Opções de performance
    parser.add_argument(
        "--threads", "-T",
        type=int,
        default=None,
        metavar="N",
        help="Número de threads para processamento paralelo (padrão: 2x núcleos da CPU)"
    )
    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="Desativa processamento paralelo (executa sequencialmente)"
    )
    
    # Opções para obtain_seqs
    parser.add_argument(
        "--force-run", "-f",
        action="store_true",
        help="Ignora arquivos .gb existentes e baixa todas as sequências do zero (usado com --obtain)"
    )
    
    # Opções para complexos de espécies (múltiplos gêneros como um único dataset)
    parser.add_argument(
        "--sppcomplex-mode", "-s",
        action="store_true",
        help="Modo complexo de espécies: trata todos os gêneros do input como um único dataset combinado"
    )
    parser.add_argument(
        "--complex-name", "-c",
        type=str,
        default=None,
        metavar="NAME",
        help="Nome da pasta de saída para o complexo (requer --sppcomplex-mode). Padrão: <primeiro_gênero>_complex"
    )
    
    # Argumento posicional opcional para gênero (executa todas as etapas para esse gênero)
    parser.add_argument(
        "genus",
        nargs="?",
        default=None,
        help="Gênero para processar em todas as etapas (ex: Fomitiporia, Lentinus)"
    )
    
    return parser.parse_args()


def run_obtain(force_run=False, genus_filter=None, sppcomplex_mode=False, complex_name=None):
    """Etapa 1: Obtenção de sequências do GenBank.
    
    Args:
        force_run: Se True, ignora .gb existentes e baixa tudo do zero
        genus_filter: Gênero específico para processar (ou None para todos)
        sppcomplex_mode: Se True, combina todos os gêneros em uma única pasta
        complex_name: Nome da pasta do complexo (requer sppcomplex_mode)
    """
    print("\n🚀 [Etapa 1] Obtendo sequências do GenBank...")
    if force_run:
        print("   ⚡ Modo --force-run ativo: ignorando arquivos .gb existentes")
    if genus_filter:
        print(f"   📌 Filtro de gênero: {genus_filter}")
    if sppcomplex_mode:
        print(f"   🔗 Modo complexo de espécies: combinando gêneros em '{complex_name or '<auto>'}")
    
    processed_genera = process_genbank_inputs(
        force_run=force_run,
        genus_filter=genus_filter,
        sppcomplex_mode=sppcomplex_mode,
        complex_name=complex_name
    )
    print("✅ Obtenção de sequências concluída.")
    return processed_genera


def run_gb_handle(genus=None, parallel=True, max_workers=None, force_run=False, sppcomplex_mode=False, complex_name=None):
    """Etapa 2: Processamento dos arquivos GenBank.
    
    Args:
        genus: Nome do gênero específico para processar (ou None para todos)
        parallel: Se True, processa gêneros em paralelo
        max_workers: Número de threads (None = auto)
        force_run: Se True, força reprocessamento mesmo sem arquivos novos
        sppcomplex_mode: Se True, processa pasta do complexo como dataset único
        complex_name: Nome da pasta do complexo (requer sppcomplex_mode)
    """
    if sppcomplex_mode and complex_name:
        print(f"\n🚀 [Etapa 2] Processando complexo '{complex_name}' como dataset único...")
    elif genus:
        print(f"\n🚀 [Etapa 2] Processando arquivos GenBank para gênero: {genus}...")
    else:
        print("\n🚀 [Etapa 2] Processando arquivos GenBank para gerar datasets...")
    
    if parallel and max_workers:
        print(f"   ⚡ Usando {max_workers} threads para processamento paralelo")
    elif not parallel:
        print("   🔄 Modo sequencial (sem paralelização)")
    
    if force_run:
        print("   ⚡ --force-run: forçando reprocessamento")
    
    gb_handle(
        input_folder=GENBANK_OUT_DIR, 
        output_folder=GENBANK_OUT_DIR, 
        genus_filter=complex_name if sppcomplex_mode else genus,
        parallel=parallel,
        max_workers=max_workers,
        force_run=force_run,
        sppcomplex_mode=sppcomplex_mode
    )
    print("✅ Processamento de datasets concluído.")


def run_qualify(genus=None, processed_genera=None):
    """Etapa 3: Qualificação taxonômica via Index Fungorum."""
    print("\n🚀 [Etapa 3] Iniciando a qualificação taxonômica...")
    
    if genus:
        print(f"   Processando gênero específico: {genus}")
        asyncio.run(process_genus(genus))
    elif processed_genera:
        print(f"   Processando gêneros detectados: {', '.join(processed_genera)}")
        asyncio.run(process_all_genuses(processed_genera))
    else:
        print("   Detectando gêneros automaticamente...")
        asyncio.run(process_all_genuses())
    
    print("✅ Qualificação taxonômica concluída.")


def run_enrich(genus=None, processed_genera=None, review_diff=False, **kwargs):
    """Etapa 4: Enriquecimento via artigos (Fases 0-5 do get_taxon_ref_)."""
    print("\n🚀 [Etapa 4] Iniciando enriquecimento por artigos...")

    targets = []
    if genus:
        targets = [genus]
    elif processed_genera:
        targets = [g for g in processed_genera if isinstance(g, str)]
    else:
        out_root = Path(GENBANK_OUT_DIR)
        if out_root.exists():
            targets = sorted([p.name for p in out_root.iterdir() if p.is_dir()])

    if not targets:
        print("   ⚠️ Nenhum gênero detectado para enriquecimento.")
        return

    print(f"   📚 Gêneros alvo: {', '.join(targets)}")

    enriched_count = 0
    skipped_count = 0

    for g in targets:
        genus_dir = Path(GENBANK_OUT_DIR) / g
        if not genus_dir.exists():
            skipped_count += 1
            continue

        parquet_candidates = [
            genus_dir / f"{g}_output_dm.parquet",
            genus_dir / f"{g}_processed.parquet",
        ]
        input_parquet = next((p for p in parquet_candidates if p.exists()), None)

        if not input_parquet:
            print(f"   ⏭️  {g}: parquet não encontrado, pulando")
            skipped_count += 1
            continue

        voucher_dict_path = genus_dir / f"{g}_voucher_dict.json"
        output_parquet = genus_dir / f"{input_parquet.stem}_enriched.parquet"

        print(f"   🔎 {g}: enriquecendo {input_parquet.name}")
        qualify_genus_data(
            parquet_path=input_parquet,
            output_path=output_parquet,
            voucher_dict_path=voucher_dict_path if voucher_dict_path.exists() else None,
            **kwargs,
        )

        if review_diff:
            review_dir = genus_dir / "review"
            summary = generate_review_csvs(
                original_path=input_parquet,
                enriched_path=output_parquet,
                out_dir=review_dir,
                prefix=g,
            )
            if summary.get("has_important_diffs"):
                print(f"   🧾 {g}: review CSVs gerados em {review_dir}")
                print(f"      - modified_enriched: {Path(summary['modified_enriched_csv']).name}")
                print(f"      - modified_original: {Path(summary['modified_original_csv']).name}")
                print(f"      - removed: {Path(summary['removed_csv']).name}")
            else:
                print(f"   🧾 {g}: sem diferenças importantes (apenas auditoria ou sem mudanças)")

        enriched_count += 1
        print(f"   ✅ {g}: salvo em {output_parquet.name}")

    print(f"✅ Enriquecimento concluído. Processados: {enriched_count} | Pulados: {skipped_count}")


def main():
    """
    Função principal que orquestra os processos de obtenção de sequências,
    qualificação taxonômica e enriquecimento por artigos.
    
    Fluxo de execução:
    1. Obtém sequências do GenBank baseado nos inputs (.txt ou .fas)
       - Input: genbank_in/ (arquivos .txt ou .fas)
       - Output: genbank_out/{genus}/ (arquivos .gb)
    2. Processa os arquivos GenBank e gera datasets (.csv, .xlsx, .parquet)
       - Input: genbank_out/{genus}/ (arquivos .gb)
       - Output: genbank_out/{genus}/ (arquivos .csv, .xlsx, .parquet)
    3. Qualifica taxonomicamente via Index Fungorum (tipos, basiônimos, etc.)
    4. Enriquece lacunas com dados de artigos (Fases 0-5 do get_taxon_ref_)
    """
    args = parse_args()
    
    print("=" * 60)
    print("  DatasetMaker - Pipeline de Processamento Taxonômico")
    print("=" * 60)
    
    # Se nenhuma etapa específica foi selecionada, executa todas
    run_all = not (args.obtain or args.gb_handle or args.qualify or args.enrich)
    
    # Gênero global (argumento posicional) - usado quando nenhuma flag específica tem gênero
    global_genus = args.genus
    
    # Modo complexo de espécies
    sppcomplex_mode = args.sppcomplex_mode
    complex_name = args.complex_name
    
    # Validação: --complex-name requer --sppcomplex-mode
    if complex_name and not sppcomplex_mode:
        print("⚠️  Aviso: --complex-name ignorado (requer --sppcomplex-mode)")
        complex_name = None
    
    if global_genus:
        print(f"\n📌 Processando gênero: {global_genus}")
    
    if sppcomplex_mode:
        print(f"\n🔗 Modo complexo de espécies ativo")
        if complex_name:
            print(f"   Nome do complexo: {complex_name}")
    
    processed_genera = None
    
    # Etapa 1: Obtenção de sequências
    if run_all or args.obtain:
        processed_genera = run_obtain(
            force_run=args.force_run,
            genus_filter=global_genus,
            sppcomplex_mode=sppcomplex_mode,
            complex_name=complex_name
        )
        # Se em modo complex, atualiza complex_name com o valor retornado (caso não tenha sido especificado)
        if sppcomplex_mode and processed_genera and isinstance(processed_genera, dict):
            complex_name = processed_genera.get('complex_name', complex_name)
            processed_genera = processed_genera.get('genera', [])
    
    # Etapa 2: Processamento GenBank
    if run_all or args.gb_handle:
        # Prioridade: flag específica > argumento posicional
        genus_gb = args.gb_handle if isinstance(args.gb_handle, str) else global_genus
        run_gb_handle(
            genus=genus_gb,
            parallel=not args.no_parallel,
            max_workers=args.threads,
            force_run=args.force_run,
            sppcomplex_mode=sppcomplex_mode,
            complex_name=complex_name
        )
    
    # Etapa 3: Qualificação taxonômica
    if run_all or args.qualify:
        # Prioridade: flag específica > argumento posicional
        genus_qual = args.qualify if isinstance(args.qualify, str) else global_genus
        # Em modo complex, usa o complex_name para qualificação
        if sppcomplex_mode and complex_name:
            run_qualify(genus=complex_name, processed_genera=processed_genera)
        else:
            run_qualify(genus=genus_qual, processed_genera=processed_genera)

    # Etapa 4: Enriquecimento via artigos
    if run_all or args.enrich:
        genus_enrich = args.enrich if isinstance(args.enrich, str) else global_genus
        if sppcomplex_mode and complex_name:
            run_enrich(genus=complex_name, processed_genera=processed_genera, review_diff=args.review_diff)
        else:
            run_enrich(genus=genus_enrich, processed_genera=processed_genera, review_diff=args.review_diff)
    
    print("\n" + "=" * 60)
    print("  🎉 Pipeline concluído com sucesso!")
    print("=" * 60)


if __name__ == "__main__":
    main()

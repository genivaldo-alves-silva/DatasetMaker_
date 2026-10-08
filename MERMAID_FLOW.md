# DatasetMaker Flow Map

Documento de referência do fluxo completo do DatasetMaker para humanos e agentes de IA.

Escopo:
- Entrada e roteamento principal em `main.py`
- Etapa 1: obtenção de sequências
- Etapa 2: processamento GenBank
- Etapa 3: qualificação taxonômica
- Etapa 4: enriquecimento por artigos
- Lacunas, falhas conhecidas e roadmap futuro

## Fluxo Principal

```mermaid
flowchart TD
    A[Entrada do usuário / CLI] --> B[main.py]

    B --> C{Flags selecionadas?}
    C -->|nenhuma| D[Executar pipeline completo]
    C -->|--obtain| E[Etapa 1: obter sequências]
    C -->|--gb_handle| F[Etapa 2: processar GenBank]
    C -->|--qualify| G[Etapa 3: qualificação taxonômica]
    C -->|--enrich| H[Etapa 4: enriquecimento por artigos]

    D --> E --> F --> G --> H

    subgraph S1[Etapa 1 - Obtenção]
        E1[genbank_inout/obtain_seqs.py]
        E2[Input .txt com gêneros]
        E3[Input .fas com sequências]
        E4[Saída: .gb em genbank_out/<Genus>/gb/]
        E1 --> E2
        E1 --> E3
        E1 --> E4
    end

    subgraph S2[Etapa 2 - GenBank]
        F1[genbank_inout/gb_handle.py]
        F2[Extrair e padronizar metadados]
        F3[Gerar csv/xlsx/parquet]
        F4[Gerar voucher_dict.json]
        F1 --> F2 --> F3 --> F4
    end

    subgraph S3[Etapa 3 - Qualificação]
        G1[TaxonQualifier/IFget_types_soap.py]
        G2[Index Fungorum SOAP]
        G3[TaxonQualifier/country_detector.py]
        G4[Marcar tipos nomenclaturais]
        G1 --> G2 --> G4
        G1 --> G3 --> G4
    end

    subgraph S4[Etapa 4 - Enriquecimento]
        H1[get_taxon_ref_/md_qualifier.py]
        H2[Fase 0: detectar lacunas]
        H3[Fase 1: limpar species]
        H4[Fases 2-5: recuperar / validar em artigos]
        H5[Fase 6: fallback GBIF / iDigBio]
        H6[Fase 6.5: fallback narrativo de country em PDF]
        H7[Fase 7: consolidar linhas por voucher_dict]
        H1 --> H2 --> H3 --> H4 --> H5 --> H6 --> H7
    end

    H4 --> DB[(articles_db)]
    H4 --> DOI[CrossRef / NCBI ELink / pop8query]
    H4 --> PDF[Unpaywall / EuropePMC / CrossRef / Sci-Hub]
    H4 --> SUP[Suplementares: xlsx / docx / csv / zip / pdf]
    H4 --> EXT[PyMuPDF-first PDF extraction]

    DB --> H4
    DOI --> PDF
    PDF --> SUP
    PDF --> EXT
    EXT --> H5
    EXT --> H6
    SUP --> H4

    H5 --> GBIF[GBIF / iDigBio]
    H6 --> PROSE[Parser narrativo de PDF]
    H6 --> LLM[LLM opcional / controlado]
    PROSE --> H7
    LLM --> H7

    H7 --> OUT[DataFrame enriquecido / parquet final]

    subgraph GAPS[Lacunas e problemas vigentes]
        G0[Species ainda como Genus sp.]
        G1G[Voucher embutido em species]
        G2G[Country vazio / ambiguo]
        G3G[Falsos positivos de accession/specimen]
        G4G[Extração PDF com edge cases]
        G5G[Linhas que precisam consolidacao]
    end

    H2 -. expõe .-> G0
    H2 -. expõe .-> G1G
    H2 -. expõe .-> G2G
    H4 -. pode gerar .-> G3G
    H4 -. pode gerar .-> G4G
    H7 -. corrige .-> G5G

    subgraph ROADMAP[Roadmap futuro]
        R1[dereplicate: ramo opcional / futuro]
        R2[type_qualifier: fluxo ativo a integrar]
        R3[phylogeny: futuro explícito]
    end

    OUT -. alimenta .-> R1
    OUT -. alimenta .-> R2
    OUT -. alimenta .-> R3

    classDef current fill:#e8f4ff,stroke:#2b6cb0,color:#1a365d;
    classDef future fill:#fef3c7,stroke:#b45309,color:#7c2d12;
    classDef issue fill:#fee2e2,stroke:#b91c1c,color:#7f1d1d;
    classDef cache fill:#dcfce7,stroke:#15803d,color:#14532d;

    class A,B,C,D,E,F,G,H,E1,E2,E3,E4,F1,F2,F3,F4,G1,G2,G3,G4,H1,H2,H3,H4,H5,H6,H7,DB,DOI,PDF,SUP,EXT,GBIF,PROSE,LLM,OUT current;
    class R1,R2,R3 future;
    class G0,G1G,G2G,G3G,G4G,G5G issue;
```

## Leitura Rápida

- `main.py` é o roteador principal do pipeline.
- `md_qualifier.py` concentra a etapa de enriquecimento e suas fases 0-7.
- `articles_db` atua como cache/local store para evitar redownload e reprocessamento.
- `dereplicate` fica fora do caminho principal atual, mas entra como ramo futuro opcional.
- `type_qualifier` já existe como fluxo útil e deve ser integrado ao pipeline principal.
- `phylogeny` ainda está vazio e deve ser tratado como futuro explícito.

## Problemas Conhecidos

- `species` ainda pode chegar como `Genus sp.` ou com voucher embutido.
- `country` pode continuar vazio mesmo após os fallbacks.
- Há falsos positivos de accession/specimen que precisam de atenção.
- A extração de PDF depende do tipo de artigo e ainda tem edge cases.
- A consolidação por `voucher_dict` pode alterar a estrutura das linhas.

## Próximos Passos

1. Ligar este documento ao `README.md` principal.
2. Refinar os nomes dos blocos se algum módulo novo entrar no pipeline.
3. Usar este fluxo como referência para novas implementações e agentes.
# Nascidos vivos (SINASC) de mães residentes no município do Rio de Janeiro

Data lake com os registros individuais do Sistema de Informações sobre Nascidos Vivos (SINASC) cuja mãe residia no município do Rio de Janeiro (IBGE 330455), de 2019 em diante. A fonte é o CSV anual do OpenDATASUS. O desenho segue o do data lake do SIM (`mortalidade_mrj`): download direto, filtro do município, Parquet e atualização por GitHub Actions. A diferença é que aqui os dados ficam no próprio Git, porque somam cerca de 22 MB.

## O que tem aqui

```
sinasc_datalake.py                      pipeline (download, filtro, tratamento, séries)
requirements.txt
dados/
  bruto/sinasc_mrj_bruto_AAAA.parquet     colunas originais do SINASC, tudo como texto
  pronto/sinasc_mrj_pronto_AAAA.parquet   datas, números, rótulos e variáveis derivadas (58 colunas)
  manifesto.csv                           um registro por ano: arquivo de origem, data de modificação, linhas, período
  qualidade_por_ano.csv                   campos sem informação e valores atípicos, por ano
  nascidos_vivos_mensal.csv               série mensal
  nascidos_vivos_semanal.csv              série por semana epidemiológica
dicionario/variaveis_pronto.csv         descrição de cada variável do arquivo pronto
referencias/municipios_rj.csv           códigos IBGE e nomes dos 92 municípios do estado
tests/test_sinasc.py                    testes sem rede
.github/workflows/atualizar_sinasc.yml  atualização semanal
```

## Recorte

Entram os nascidos vivos com `CODMUNRES` igual a 330455, qualquer que seja o local do parto. Em 2019 a 2026 são 495.589 registros, dos quais 471.475 nasceram no próprio município e 24.114 (4,9%) em outro. A coluna `nascido_no_municipio` separa os dois grupos. Para analisar os partos ocorridos no município, inclusive de mães de fora, seria preciso outro recorte (`CODMUNNASC`), que este repositório não faz.

O ano do arquivo coincide com o ano de nascimento em todos os registros carregados.

## Ler os dados

Python:

```python
import pandas as pd
nv = pd.read_parquet("dados/pronto")          # todos os anos
```

R:

```r
nv <- arrow::open_dataset("dados/pronto") |> dplyr::collect()
```

DuckDB (para o bruto, `union_by_name` é necessário, porque a coluna `OPORT_DN` só existe de 2023 em diante):

```sql
SELECT * FROM read_parquet('dados/bruto/*.parquet', union_by_name = true);
```

Ler a pasta `dados/bruto` de uma vez com `pd.read_parquet` descarta `OPORT_DN`. Para mantê-la, leia os arquivos um a um e concatene.

## Séries

`nascidos_vivos_mensal.csv`: `ano`, `mes`, `ano_mes`, `nascidos_vivos`, `partos_cesareos`, `prematuros`, `baixo_peso`, `mes_completo`.

`nascidos_vivos_semanal.csv`: `ano_epi`, `sem_epi`, `periodo_se`, `inicio_semana_epi`, `nascidos_vivos`, `partos_cesareos`, `prematuros`, `baixo_peso`, `semana_completa`.

As colunas de contagem (`partos_cesareos`, `prematuros`, `baixo_peso`) somam os registros verdadeiros e tratam a informação ausente como falso. Para proporções, use o arquivo pronto e exclua os ignorados do denominador.

A semana epidemiológica segue o critério MMWR: domingo a sábado, e a SE 1 é a que contém 4 de janeiro. A SE 1 de 2020 começa em 29/12/2019, a SE 1 de 2026 em 04/01/2026, e 2025 tem 53 semanas. `mes_completo` e `semana_completa` indicam só se todos os dias do período estão dentro do intervalo coberto pelos dados (de 01/01/2019 até a última data de nascimento). A SE 1 de 2019 começa em 30/12/2018 e por isso sai como incompleta. Nenhuma das duas colunas mede o atraso de registro descrito abaixo.

## Dados recentes são parciais

O SINASC recebe registros com atraso, e o último arquivo é uma fotografia. Na carga de 26/10/2026, os nascimentos de agosto de 2026 iam só até o dia 25 e somavam 1.430, e julho tinha 3.951, contra 5.086 e 4.957 em julho e agosto de 2025. Os meses mais recentes podem ganhar registros nas próximas cargas. Nos nascimentos de 2026 de janeiro a abril, a mediana entre a data de nascimento e a data de cadastro (`DTCADASTRO`, no arquivo bruto) ficou entre 20 e 31 dias, e de 37% a 51% dos registros foram cadastrados mais de 30 dias depois do parto.

Fevereiro de 2026 também está baixo: 3.245 nascimentos, contra 4.627 em fevereiro de 2025. A queda vai de 9 a 25/02: nos 13 dias úteis desse intervalo a contagem diária fica entre 92 e 123, contra 130 a 177 nos dias úteis de janeiro, do início de fevereiro e de março. Ela aparece em vários estabelecimentos grandes. O atraso mediano de cadastro desses nascimentos foi de 31 dias, contra 20 a 28 dias em janeiro, março e abril. Não investiguei a causa. Uma queda de cerca de 30% em duas semanas e meia sugere falha de registro ou de envio, e só a SMS pode confirmar. Quem usar fevereiro de 2026 como denominador deve conferir com ela.

## Tratamento

- Colunas em maiúsculas (`contador` virou `CONTADOR` nos arquivos de 2023 em diante) e valores originais no arquivo bruto.
- No arquivo pronto, 99 vira NA em idade da mãe, Apgar, semanas de gestação, consultas de pré-natal, mês de início do pré-natal, idade do pai e contagens de filhos e gestações.
- Valores atípicos permanecem. Existem registros com peso abaixo de 500 g (de 48 a 154 por ano) e mães com 50 anos ou mais (de 8 a 34 por ano). A contagem fica em `qualidade_por_ano.csv`.
- Prematuro significa menos de 37 semanas. Usa `SEMAGESTAC`; sem ela, usa a faixa de `GESTACAO`. Fica NA se as duas faltam.
- `contador` é único dentro de cada ano e repete entre anos. Não há identificador único geral.
- Códigos de municípios têm 6 dígitos, como no arquivo de origem. O nome do município de nascimento só existe para o estado do Rio de Janeiro.

Rótulos só foram aplicados aos campos cujo código está confirmado. Ficam com o código original, sem rótulo, `TPMETESTIM`, `TPNASCASSI`, `TPFUNCRESP`, `TPDOCRESP`, `KOTELCHUCK`, `PARIDADE` e `TPROBSON`. Os rótulos aplicados também devem ser conferidos com o dicionário oficial do SINASC antes de uma publicação, porque não consegui acessar esse documento ao montar o repositório. Se um ano trouxer um código fora das tabelas, ele é mantido como veio e listado na coluna `codigos_sem_rotulo` de `qualidade_por_ano.csv`.

Colunas administrativas (lote, versão do sistema, datas de cadastro e de recebimento, entre outras) ficam só no arquivo bruto.

## Atualizar

O workflow `Atualizar SINASC (MRJ)` roda toda segunda-feira às 08:00 UTC. Ele consulta o cabeçalho do arquivo de cada ano (data de modificação e tamanho) e só baixa e reprocessa o ano que mudou. Sem mudança, termina em segundos e não faz commit. Os anos que mudam são gravados em `dados/` e enviados ao repositório pelo próprio workflow. Em 26/10/2026 a origem mostrava 2019 a 2024 modificados em 08/05/2026, 2025 em 31/08/2026 e 2026 em 02/10/2026.

Primeira execução no GitHub: Actions, "Atualizar SINASC (MRJ)", Run workflow, com o campo `anos` vazio. O repositório já vem com os dados de 2019 a 2026.

Localmente:

```bash
pip install -r requirements.txt
python sinasc_datalake.py                    # 2019 até o ano corrente, só o que mudou
python sinasc_datalake.py --anos 2025-2026
python sinasc_datalake.py --forcar           # reprocessa mesmo sem mudança
python sinasc_datalake.py --apagar-zip       # apaga o zip depois de processar
python sinasc_datalake.py --so-series        # refaz séries a partir de dados/pronto
python -m pytest -q tests
```

Cada ano exige baixar um zip de cerca de 100 MB e ler um CSV de 2,4 a 2,8 milhões de linhas, o que levou cerca de 20 segundos de processamento por ano no ambiente de teste. Cada reprocessamento do ano corrente regrava seus dois Parquet (de 1,5 a 3,4 MB juntos, conforme o número de meses) e aumenta o histórico do Git nessa medida.

## Carga de 26/10/2026

| Ano | Linhas no Brasil | Residentes no MRJ | Última data de nascimento |
|---|---|---|---|
| 2019 | 2.849.146 | 76.803 | 31/12/2019 |
| 2020 | 2.730.145 | 73.135 | 31/12/2020 |
| 2021 | 2.677.101 | 68.874 | 31/12/2021 |
| 2022 | 2.561.922 | 64.978 | 31/12/2022 |
| 2023 | 2.537.576 | 62.850 | 31/12/2023 |
| 2024 | 2.389.325 | 57.457 | 31/12/2024 |
| 2025 | 2.456.688 | 58.726 | 31/12/2025 |
| 2026 | 1.504.579 | 32.766 | 25/08/2026 |

Verificações feitas nessa carga: as séries mensal e semanal somam os 495.589 registros; a semana epidemiológica de cada data foi conferida contra uma segunda implementação; `baixo_peso`, `cesariana`, `prematuro` e `nascido_no_municipio` batem com os campos originais; reprocessar um ano sem mudança na origem gera arquivos idênticos byte a byte.

## Fonte

Ministério da Saúde, OpenDATASUS, conjunto SINASC. Endereço dos arquivos: `https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_AAAA_csv.zip`. Os arquivos não trazem nome nem documento, mas combinam data e hora do nascimento, estabelecimento e características da mãe. O uso deve respeitar a LGPD e os termos do OpenDATASUS.

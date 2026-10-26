#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nascidos vivos (SINASC) de mães residentes no município do Rio de Janeiro (IBGE 330455).

Fonte: CSV anual do OpenDATASUS (Brasil inteiro). O script baixa cada ano, lê o arquivo em
blocos, guarda só as linhas com CODMUNRES = 330455 e grava duas camadas em Parquet:

    dados/bruto/sinasc_mrj_bruto_AAAA.parquet    colunas originais, tudo como texto
    dados/pronto/sinasc_mrj_pronto_AAAA.parquet  datas, números, rótulos e variáveis derivadas

Também grava:

    dados/manifesto.csv                 um registro por ano (arquivo de origem, tamanho, data de modificação,
                                        linhas no Brasil e no MRJ, período coberto)
    dados/qualidade_por_ano.csv         percentual de campos sem informação e valores atípicos, por ano
    dados/nascidos_vivos_mensal.csv     série mensal
    dados/nascidos_vivos_semanal.csv    série por semana epidemiológica (domingo a sábado)

Um ano só é baixado de novo quando o arquivo remoto mudou (Last-Modified ou Content-Length).

Uso
---
    python sinasc_datalake.py                       # 2019 até o ano corrente, só o que mudou
    python sinasc_datalake.py --anos 2024-2026      # intervalo
    python sinasc_datalake.py --anos 2019,2025      # lista
    python sinasc_datalake.py --forcar              # reprocessa mesmo sem mudança na origem
    python sinasc_datalake.py --apagar-zip          # apaga o zip depois de processar
    python sinasc_datalake.py --so-series           # só refaz manifesto e séries a partir do que já está em dados/

Dependências: pandas, pyarrow, numpy, requests
"""
from __future__ import annotations

import argparse
import codecs
import datetime as dt
import email.utils
import logging
import re
import sys
import time
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# --------------------------------------------------------------------------------------
# Configuração
# --------------------------------------------------------------------------------------
COD_MRJ = "330455"  # município do Rio de Janeiro, código IBGE de 6 dígitos
ANO_INICIAL = 2019
URL_TPL = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SINASC/csv/SINASC_{ano}_csv.zip"
LINHAS_POR_BLOCO = 200_000

RAIZ = Path(__file__).resolve().parent
PASTA_DADOS = RAIZ / "dados"
PASTA_BRUTO = PASTA_DADOS / "bruto"
PASTA_PRONTO = PASTA_DADOS / "pronto"
ARQ_MANIFESTO = PASTA_DADOS / "manifesto.csv"
ARQ_QUALIDADE = PASTA_DADOS / "qualidade_por_ano.csv"
ARQ_MENSAL = PASTA_DADOS / "nascidos_vivos_mensal.csv"
ARQ_SEMANAL = PASTA_DADOS / "nascidos_vivos_semanal.csv"
ARQ_MUNICIPIOS = RAIZ / "referencias" / "municipios_rj.csv"

log = logging.getLogger("sinasc")

# --------------------------------------------------------------------------------------
# Rótulos
# Só entram domínios cujos códigos estão confirmados. Campos sem rótulo aqui (TPMETESTIM,
# TPNASCASSI, TPFUNCRESP, TPDOCRESP, KOTELCHUCK, PARIDADE, TPROBSON) ficam com o código original.
# Um código que não está na tabela é mantido como veio e aparece no relatório de qualidade.
# --------------------------------------------------------------------------------------
ESC_AGR = {
    "00": "Sem escolaridade", "01": "Fundamental I incompleto",
    "02": "Fundamental I completo", "03": "Fundamental II incompleto",
    "04": "Fundamental II completo", "05": "Médio incompleto",
    "06": "Médio completo", "07": "Superior incompleto",
    "08": "Superior completo", "09": "Ignorado",
    "10": "Fundamental I incompleto ou inespecífico",
    "11": "Fundamental II incompleto ou inespecífico",
    "12": "Médio incompleto ou inespecífico",
}
SIM_NAO = {"1": "Sim", "2": "Não", "9": "Ignorado"}
SIM_NAO_NA = {"1": "Sim", "2": "Não", "3": "Não se aplica", "9": "Ignorado"}

LABELS: dict[str, dict[str, str]] = {
    "LOCNASC": {
        "1": "Hospital", "2": "Outros estabelecimentos de saúde", "3": "Domicílio",
        "4": "Outros", "5": "Aldeia indígena", "9": "Ignorado",
    },
    "ESTCIVMAE": {
        "1": "Solteira", "2": "Casada", "3": "Viúva",
        "4": "Separada judicialmente/divorciada", "5": "União estável", "9": "Ignorado",
    },
    "ESCMAE": {
        "1": "Nenhuma", "2": "1 a 3 anos", "3": "4 a 7 anos",
        "4": "8 a 11 anos", "5": "12 anos e mais", "9": "Ignorado",
    },
    "ESCMAE2010": {
        "0": "Sem escolaridade", "1": "Fundamental I", "2": "Fundamental II", "3": "Médio",
        "4": "Superior incompleto", "5": "Superior completo", "9": "Ignorado",
    },
    "ESCMAEAGR1": ESC_AGR,
    "GESTACAO": {
        "1": "Menos de 22 semanas", "2": "22 a 27 semanas", "3": "28 a 31 semanas",
        "4": "32 a 36 semanas", "5": "37 a 41 semanas", "6": "42 semanas e mais", "9": "Ignorado",
    },
    "GRAVIDEZ": {"1": "Única", "2": "Dupla", "3": "Tripla e mais", "9": "Ignorada"},
    "PARTO": {"1": "Vaginal", "2": "Cesáreo", "9": "Ignorado"},
    "CONSULTAS": {
        "1": "Nenhuma", "2": "1 a 3", "3": "4 a 6", "4": "7 e mais", "9": "Ignorado",
    },
    "SEXO": {
        "1": "Masculino", "2": "Feminino", "0": "Ignorado", "9": "Ignorado",
        "M": "Masculino", "F": "Feminino", "I": "Ignorado",
    },
    "RACACOR": {"1": "Branca", "2": "Preta", "3": "Amarela", "4": "Parda", "5": "Indígena"},
    "RACACORMAE": {"1": "Branca", "2": "Preta", "3": "Amarela", "4": "Parda", "5": "Indígena"},
    "IDANOMAL": SIM_NAO,
    "TPAPRESENT": {"1": "Cefálico", "2": "Pélvica ou podálica", "3": "Transversa", "9": "Ignorado"},
    "STTRABPART": SIM_NAO_NA,
    "STCESPARTO": SIM_NAO_NA,
}

# coluna de origem -> coluna no arquivo pronto
COLUNAS_ROTULADAS = {
    "LOCNASC": "local_nascimento",
    "ESTCIVMAE": "estado_civil_mae",
    "ESCMAE": "escolaridade_mae_anos",
    "ESCMAE2010": "escolaridade_mae_2010",
    "ESCMAEAGR1": "escolaridade_mae_agregada",
    "RACACORMAE": "raca_cor_mae",
    "SEXO": "sexo",
    "RACACOR": "raca_cor",
    "GESTACAO": "faixa_gestacao",
    "GRAVIDEZ": "tipo_gravidez",
    "PARTO": "tipo_parto",
    "CONSULTAS": "faixa_consultas_prenatal",
    "IDANOMAL": "anomalia_congenita",
    "TPAPRESENT": "apresentacao_fetal",
    "STTRABPART": "trabalho_parto_induzido",
    "STCESPARTO": "cesarea_antes_trabalho_parto",
}
# campos mantidos como código, sem rótulo (domínio não confirmado)
COLUNAS_CODIGO = {
    "TPMETESTIM": "tpmetestim",
    "TPNASCASSI": "tpnascassi",
    "TPFUNCRESP": "tpfuncresp",
    "TPDOCRESP": "tpdocresp",
    "KOTELCHUCK": "kotelchuck",
    "PARIDADE": "paridade",
    "TPROBSON": "tprobson",
}
# campos numéricos em que 99 significa ignorado
NUMERICOS_99 = {
    "QTDFILVIVO": "qtd_filhos_vivos",
    "QTDFILMORT": "qtd_filhos_mortos",
    "QTDGESTANT": "qtd_gestacoes_anteriores",
    "QTDPARTNOR": "qtd_partos_vaginais",
    "QTDPARTCES": "qtd_partos_cesareos",
    "CONSPRENAT": "n_consultas_prenatal",
    "MESPRENAT": "mes_inicio_prenatal",
    "IDADEPAI": "idade_pai",
}

FAIXAS_IDADE_MAE = ([0, 15, 20, 25, 30, 35, 40, 45, 200],
                    ["10 a 14", "15 a 19", "20 a 24", "25 a 29", "30 a 34", "35 a 39", "40 a 44", "45 ou mais"])
FAIXAS_PESO = ([0, 1000, 1500, 2500, 3000, 4000, 100000],
               ["Menos de 1.000 g", "1.000 a 1.499 g", "1.500 a 2.499 g",
                "2.500 a 2.999 g", "3.000 a 3.999 g", "4.000 g ou mais"])

# colunas do arquivo pronto, na ordem de saída (o dicionário em dicionario/ segue esta lista)
COLUNAS_PRONTO = [
    "contador", "ano_arquivo",
    "data_nascimento", "ano_nascimento", "mes_nascimento", "ano_mes",
    "ano_epi", "sem_epi", "periodo_se", "inicio_semana_epi", "hora_nascimento",
    "codigo_municipio_residencia", "codigo_municipio_nascimento", "nascido_no_municipio",
    "municipio_nascimento", "local_nascimento", "codigo_estabelecimento",
    "idade_mae", "faixa_etaria_mae", "estado_civil_mae", "escolaridade_mae_anos",
    "escolaridade_mae_2010", "escolaridade_mae_agregada", "raca_cor_mae",
    "qtd_filhos_vivos", "qtd_filhos_mortos", "qtd_gestacoes_anteriores",
    "qtd_partos_vaginais", "qtd_partos_cesareos", "idade_pai",
    "sexo", "raca_cor", "peso_g", "faixa_peso", "baixo_peso", "apgar1", "apgar5",
    "faixa_gestacao", "semanas_gestacao", "prematuro", "tipo_gravidez", "tipo_parto", "cesariana",
    "faixa_consultas_prenatal", "n_consultas_prenatal", "mes_inicio_prenatal",
    "anomalia_congenita", "codigo_anomalia", "apresentacao_fetal",
    "trabalho_parto_induzido", "cesarea_antes_trabalho_parto",
    "tpmetestim", "tpnascassi", "tpfuncresp", "tpdocresp", "kotelchuck", "paridade", "tprobson",
]


# --------------------------------------------------------------------------------------
# Semana epidemiológica (MMWR): domingo a sábado; a SE 1 é a que contém 4 de janeiro
# --------------------------------------------------------------------------------------
def semana_epidemiologica(datas: pd.Series) -> pd.DataFrame:
    """Devolve ano_epi, sem_epi e inicio_semana_epi (o domingo) para cada data."""
    d = pd.to_datetime(datas)
    domingo = d.dt.normalize() - pd.to_timedelta((d.dt.dayofweek + 1) % 7, unit="D")
    ano_epi = (domingo + pd.Timedelta(days=3)).dt.year  # o 4º dia da semana decide o ano
    jan4 = pd.to_datetime(ano_epi.astype("Int64").astype("string") + "-01-04")
    domingo_se1 = jan4 - pd.to_timedelta((jan4.dt.dayofweek + 1) % 7, unit="D")
    sem = ((domingo - domingo_se1).dt.days // 7) + 1
    return pd.DataFrame({
        "ano_epi": ano_epi.astype("Int64"),
        "sem_epi": sem.astype("Int64"),
        "inicio_semana_epi": domingo,
    })


# --------------------------------------------------------------------------------------
# Origem: consulta ao arquivo remoto, download e leitura
# --------------------------------------------------------------------------------------
def _sessao() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "sinasc-mrj-datalake (github.com/FEHALTENBURG1)"
    return s


def consultar_origem(ano: int, sessao: requests.Session, tentativas: int = 4) -> dict | None:
    """HEAD no arquivo do ano. None se o arquivo não existe (S3 responde 403 ou 404)."""
    url = URL_TPL.format(ano=ano)
    for i in range(1, tentativas + 1):
        try:
            r = sessao.head(url, timeout=60, allow_redirects=True)
        except requests.RequestException as e:
            log.warning("HEAD %s falhou (%s), tentativa %d/%d", ano, e, i, tentativas)
            time.sleep(3 * i)
            continue
        if r.status_code in (403, 404):
            return None
        if r.status_code == 200:
            return {
                "url": url,
                "last_modified": _normalizar_data_http(r.headers.get("Last-Modified", "")),
                "content_length": int(r.headers.get("Content-Length", 0) or 0),
            }
        log.warning("HEAD %s devolveu %s, tentativa %d/%d", ano, r.status_code, i, tentativas)
        time.sleep(3 * i)
    raise RuntimeError(f"Não consegui consultar {url}")


def _normalizar_data_http(txt: str) -> str:
    if not txt:
        return ""
    try:
        return email.utils.parsedate_to_datetime(txt).astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError):
        return txt


def baixar_zip(origem: dict, destino: Path, sessao: requests.Session, tentativas: int = 4) -> Path:
    """Baixa para um .part e confere o tamanho antes de renomear."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = destino.with_suffix(destino.suffix + ".part")
    for i in range(1, tentativas + 1):
        try:
            with sessao.get(origem["url"], stream=True, timeout=(30, 120)) as r:
                r.raise_for_status()
                with open(parcial, "wb") as f:
                    for bloco in r.iter_content(chunk_size=1 << 20):
                        f.write(bloco)
            tam = parcial.stat().st_size
            if origem["content_length"] and tam != origem["content_length"]:
                raise IOError(f"tamanho {tam} difere do esperado {origem['content_length']}")
            parcial.replace(destino)
            return destino
        except (requests.RequestException, IOError) as e:
            log.warning("download de %s falhou (%s), tentativa %d/%d", destino.name, e, i, tentativas)
            time.sleep(5 * i)
    raise RuntimeError(f"Não consegui baixar {origem['url']}")


def _detectar_encoding(amostra: bytes) -> str:
    if amostra.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    try:
        codecs.getincrementaldecoder("utf-8")().decode(amostra, final=False)
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def _detectar_sep(primeira_linha: str) -> str:
    return ";" if primeira_linha.count(";") >= primeira_linha.count(",") else ","


def ler_mrj(caminho_zip: Path, linhas_por_bloco: int = LINHAS_POR_BLOCO) -> tuple[pd.DataFrame, int]:
    """Lê o CSV dentro do zip em blocos e devolve (linhas do MRJ, total de linhas do arquivo).

    Os nomes das colunas viram maiúsculos (o cabeçalho tem 'contador' em 2023+ e 'CONTADOR' antes).
    Todos os valores ficam como texto.
    """
    with zipfile.ZipFile(caminho_zip) as z:
        membros = [m for m in z.namelist() if m.lower().endswith(".csv")]
        if not membros:
            raise RuntimeError(f"{caminho_zip.name} não contém CSV")
        nome = membros[0]
        with z.open(nome) as f:
            amostra = f.read(1 << 16)
        enc = _detectar_encoding(amostra)
        primeira = amostra.decode(enc, errors="ignore").splitlines()[0]
        sep = _detectar_sep(primeira)
        log.info("  %s: encoding=%s sep=%r", nome, enc, sep)

        partes, total = [], 0
        with z.open(nome) as f:
            leitor = pd.read_csv(f, sep=sep, dtype=str, keep_default_na=False, na_filter=False,
                                 encoding=enc, chunksize=linhas_por_bloco)
            for bloco in leitor:
                bloco.columns = [c.replace("﻿", "").strip().upper() for c in bloco.columns]
                if "CODMUNRES" not in bloco.columns:
                    raise RuntimeError(f"{nome}: coluna CODMUNRES ausente ({list(bloco.columns)[:8]}...)")
                total += len(bloco)
                # compara os 6 primeiros dígitos, para aceitar também o código com dígito verificador
                mrj = bloco["CODMUNRES"].str.strip().str[:6] == COD_MRJ
                if mrj.any():
                    partes.append(bloco.loc[mrj])
    if not partes:
        return pd.DataFrame(), total
    return pd.concat(partes, ignore_index=True), total


# --------------------------------------------------------------------------------------
# Camada pronto
# --------------------------------------------------------------------------------------
def _txt(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip()


def _num(s: pd.Series, ignorado: int | None = None) -> pd.Series:
    """Texto -> Int64. Vazio, não numérico e o código de ignorado viram NA."""
    t = _txt(s).replace("", pd.NA)
    n = pd.to_numeric(t, errors="coerce")
    if ignorado is not None:
        n = n.where(n != ignorado)
    return n.round().astype("Int64")


def _rotular(s: pd.Series, dic: dict[str, str]) -> pd.Series:
    t = _txt(s)
    r = t.map(dic).astype("string")
    sem_rotulo = r.isna() & t.notna() & (t != "")
    r = r.mask(sem_rotulo, t)
    return r.mask(t.isna() | (t == ""), pd.NA)


def _carregar_municipios() -> dict[str, str]:
    if not ARQ_MUNICIPIOS.exists():
        return {}
    m = pd.read_csv(ARQ_MUNICIPIOS, dtype=str)
    return dict(zip(m["codigo"].str[:6], m["nome"]))


def preparar(bruto: pd.DataFrame, ano_arquivo: int, municipios: dict[str, str] | None = None) -> pd.DataFrame:
    """Camada pronto a partir das colunas originais (todas como texto)."""
    if municipios is None:
        municipios = _carregar_municipios()
    b = bruto

    def col(nome: str) -> pd.Series:
        # colunas que não existem em todos os anos (ex.: OPORT_DN) vêm vazias
        return b[nome] if nome in b.columns else pd.Series([""] * len(b), index=b.index, dtype="string")

    p = pd.DataFrame(index=b.index)
    p["contador"] = _txt(col("CONTADOR"))
    p["ano_arquivo"] = np.int16(ano_arquivo)

    data = pd.to_datetime(_txt(col("DTNASC")), format="%d%m%Y", errors="coerce")
    p["data_nascimento"] = data.dt.date
    p["ano_nascimento"] = data.dt.year.astype("Int16")
    p["mes_nascimento"] = data.dt.month.astype("Int8")
    p["ano_mes"] = data.dt.strftime("%Y-%m").astype("string")
    se = semana_epidemiologica(data.where(data.notna()).dropna()).reindex(b.index)
    p["ano_epi"] = se["ano_epi"].astype("Int16")
    p["sem_epi"] = se["sem_epi"].astype("Int8")
    p["periodo_se"] = (se["ano_epi"].astype("string") + "-SE" + se["sem_epi"].astype("string").str.zfill(2))
    p["inicio_semana_epi"] = se["inicio_semana_epi"].dt.date

    hhmm = _txt(col("HORANASC")).replace("", pd.NA).str.zfill(4)  # vazio fica NA (não vira meia-noite)
    hora = pd.to_numeric(hhmm.str[:2], errors="coerce")
    p["hora_nascimento"] = hora.where(hora.between(0, 23)).astype("Int8")

    cod_res = _txt(col("CODMUNRES")).str[:6]
    cod_nasc = _txt(col("CODMUNNASC")).str[:6].replace("", pd.NA)
    p["codigo_municipio_residencia"] = cod_res
    p["codigo_municipio_nascimento"] = cod_nasc
    p["nascido_no_municipio"] = (cod_nasc == COD_MRJ).where(cod_nasc.notna()).astype("boolean")
    p["municipio_nascimento"] = cod_nasc.map(municipios).astype("string")
    p["codigo_estabelecimento"] = _txt(col("CODESTAB")).replace("", pd.NA)

    idade = _num(col("IDADEMAE"), ignorado=99)
    p["idade_mae"] = idade.astype("Int16")
    p["faixa_etaria_mae"] = pd.cut(idade.astype("float"), bins=FAIXAS_IDADE_MAE[0],
                                   labels=FAIXAS_IDADE_MAE[1], right=False).astype("string")

    for origem, destino in COLUNAS_ROTULADAS.items():
        p[destino] = _rotular(col(origem), LABELS[origem])
    for origem, destino in NUMERICOS_99.items():
        p[destino] = _num(col(origem), ignorado=99).astype("Int16")

    peso = _num(col("PESO"))
    p["peso_g"] = peso.astype("Int16")
    p["faixa_peso"] = pd.cut(peso.astype("float"), bins=FAIXAS_PESO[0], labels=FAIXAS_PESO[1],
                             right=False).astype("string")
    p["baixo_peso"] = (peso < 2500).astype("boolean")
    p["apgar1"] = _num(col("APGAR1"), ignorado=99).astype("Int8")
    p["apgar5"] = _num(col("APGAR5"), ignorado=99).astype("Int8")

    sem = _num(col("SEMAGESTAC"), ignorado=99)
    p["semanas_gestacao"] = sem.astype("Int8")
    # prematuro: menos de 37 semanas completas. Usa SEMAGESTAC; sem ela, a faixa de GESTACAO (códigos 1 a 4 = menos de 37)
    faixa = _num(col("GESTACAO"))
    por_faixa = faixa.where(faixa.isin([1, 2, 3, 4, 5, 6]))
    prem = (sem < 37).astype("boolean")
    prem_faixa = (por_faixa <= 4).astype("boolean")
    p["prematuro"] = prem.where(sem.notna(), prem_faixa.where(por_faixa.notna())).astype("boolean")

    p["cesariana"] = (_txt(col("PARTO")).map({"1": False, "2": True})).astype("boolean")
    p["codigo_anomalia"] = _txt(col("CODANOMAL")).replace("", pd.NA)
    for origem, destino in COLUNAS_CODIGO.items():
        p[destino] = _txt(col(origem)).replace("", pd.NA)

    return p[COLUNAS_PRONTO]


# --------------------------------------------------------------------------------------
# Qualidade, séries e manifesto
# --------------------------------------------------------------------------------------
def resumo_qualidade(pronto: pd.DataFrame, bruto: pd.DataFrame, ano: int) -> dict:
    n = len(pronto)
    pct = lambda s: round(100 * float(s.isna().sum()) / n, 2) if n else np.nan
    sem_rotulo = {}
    for origem, destino in COLUNAS_ROTULADAS.items():
        valores = _txt(bruto[origem]).dropna() if origem in bruto.columns else pd.Series(dtype="string")
        fora = sorted(set(v for v in valores.unique() if v != "" and v not in LABELS[origem]))
        if fora:
            sem_rotulo[origem] = fora
    return {
        "ano_arquivo": ano,
        "nascidos_vivos": n,
        "contador_duplicado": int(pronto["contador"].duplicated().sum()),
        "data_invalida": int(pronto["data_nascimento"].isna().sum()),
        "data_fora_do_ano_do_arquivo": int((pronto["ano_nascimento"] != ano).sum()),
        "pct_sem_peso": pct(pronto["peso_g"]),
        "pct_sem_apgar5": pct(pronto["apgar5"]),
        "pct_sem_semanas_gestacao": pct(pronto["semanas_gestacao"]),
        "pct_sem_prematuro": pct(pronto["prematuro"]),
        "pct_sem_n_consultas_prenatal": pct(pronto["n_consultas_prenatal"]),
        "pct_sem_idade_mae": pct(pronto["idade_mae"]),
        "pct_sem_raca_cor": pct(pronto["raca_cor"]),
        "pct_escolaridade_mae_ignorada": round(
            100 * float((pronto["escolaridade_mae_2010"].isna() | (pronto["escolaridade_mae_2010"] == "Ignorado")).sum()) / n, 2) if n else np.nan,
        "peso_menor_500g": int((pronto["peso_g"] < 500).sum()),
        "idade_mae_menor_12": int((pronto["idade_mae"] < 12).sum()),
        "idade_mae_50_ou_mais": int((pronto["idade_mae"] >= 50).sum()),
        "codigos_sem_rotulo": ";".join(f"{k}={'|'.join(v)}" for k, v in sem_rotulo.items()),
    }


def _agregar(pronto: pd.DataFrame, chaves: list[str]) -> pd.DataFrame:
    base = pronto[chaves].copy()
    base["nascidos_vivos"] = 1
    for nome, origem in (("partos_cesareos", "cesariana"), ("prematuros", "prematuro"), ("baixo_peso", "baixo_peso")):
        base[nome] = pronto[origem].fillna(False).astype(int)
    return base.groupby(chaves, sort=True).sum().reset_index()


def gerar_series(pronto: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Séries mensal e semanal, a partir da data de nascimento."""
    validos = pronto[pronto["data_nascimento"].notna()].copy()
    mensal = _agregar(validos, ["ano_nascimento", "mes_nascimento"])
    mensal.insert(2, "ano_mes", mensal["ano_nascimento"].astype(str) + "-" + mensal["mes_nascimento"].astype(str).str.zfill(2))
    mensal = mensal.rename(columns={"ano_nascimento": "ano", "mes_nascimento": "mes"})
    # mês completo: o primeiro e o último dia do mês estão dentro do período coberto pelos dados
    dmin = pd.Timestamp(min(validos["data_nascimento"]))
    dmax = pd.Timestamp(max(validos["data_nascimento"]))
    primeiro = pd.to_datetime(mensal["ano_mes"] + "-01")
    mensal["mes_completo"] = (primeiro >= dmin) & (primeiro + pd.offsets.MonthEnd(0) <= dmax)

    semanal = _agregar(validos, ["ano_epi", "sem_epi", "inicio_semana_epi"])
    semanal.insert(2, "periodo_se", semanal["ano_epi"].astype(str) + "-SE" + semanal["sem_epi"].astype(str).str.zfill(2))
    # semana completa: os 7 dias estão dentro do período coberto pelos dados
    ini = pd.to_datetime(semanal["inicio_semana_epi"])
    semanal["semana_completa"] = (ini >= dmin) & (ini + pd.Timedelta(days=6) <= dmax)
    semanal["inicio_semana_epi"] = ini.dt.strftime("%Y-%m-%d")
    return mensal, semanal


def ler_manifesto() -> pd.DataFrame:
    if ARQ_MANIFESTO.exists():
        return pd.read_csv(ARQ_MANIFESTO, dtype=str, keep_default_na=False)
    return pd.DataFrame(columns=["ano", "url", "last_modified", "content_length", "linhas_brasil",
                                 "linhas_mrj", "data_min", "data_max", "processado_em"])


def atualizar_manifesto(man: pd.DataFrame, reg: dict) -> pd.DataFrame:
    man = man[man["ano"] != str(reg["ano"])]
    man = pd.concat([man, pd.DataFrame([{k: str(v) for k, v in reg.items()}])], ignore_index=True)
    return man.sort_values("ano").reset_index(drop=True)


def gravar_parquet(df: pd.DataFrame, caminho: Path) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(caminho, engine="pyarrow", compression="zstd", index=False)


def _csv(df: pd.DataFrame, caminho: Path) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(caminho, index=False, encoding="utf-8", lineterminator="\n")


def refazer_saidas_agregadas() -> None:
    """Lê todos os parquets pronto e refaz séries e relatório de qualidade."""
    arquivos = sorted(PASTA_PRONTO.glob("sinasc_mrj_pronto_*.parquet"))
    if not arquivos:
        log.warning("Nenhum arquivo pronto em %s", PASTA_PRONTO)
        return
    pronto = pd.concat([pd.read_parquet(a) for a in arquivos], ignore_index=True)
    mensal, semanal = gerar_series(pronto)
    _csv(mensal, ARQ_MENSAL)
    _csv(semanal, ARQ_SEMANAL)
    log.info("Séries: %d meses, %d semanas epidemiológicas, %d nascidos vivos", len(mensal), len(semanal), len(pronto))


# --------------------------------------------------------------------------------------
# Execução
# --------------------------------------------------------------------------------------
def interpretar_anos(txt: str | None) -> list[int]:
    if not txt:
        return list(range(ANO_INICIAL, dt.date.today().year + 1))
    anos: set[int] = set()
    for parte in txt.split(","):
        parte = parte.strip()
        m = re.fullmatch(r"(\d{4})(?:-(\d{4}))?", parte)
        if not m:
            raise SystemExit(f"Formato de anos inválido: {parte!r}. Use 2019-2026 ou 2019,2025.")
        ini = int(m.group(1))
        fim = int(m.group(2) or ini)
        if fim < ini:
            raise SystemExit(f"Intervalo invertido: {parte}")
        anos.update(range(ini, fim + 1))
    fora = [a for a in anos if a < ANO_INICIAL]
    if fora:
        raise SystemExit(f"Este repositório começa em {ANO_INICIAL}; anos fora do escopo: {sorted(fora)}")
    return sorted(anos)


def processar_ano(ano: int, origem: dict, pasta_zip: Path, sessao: requests.Session,
                  apagar_zip: bool, linhas_por_bloco: int) -> dict:
    zip_path = pasta_zip / f"SINASC_{ano}_csv.zip"
    if zip_path.exists() and origem["content_length"] and zip_path.stat().st_size == origem["content_length"]:
        log.info("%d: usando o zip já baixado (%s)", ano, zip_path)
    else:
        log.info("%d: baixando %.0f MB", ano, origem["content_length"] / 1e6)
        baixar_zip(origem, zip_path, sessao)

    try:
        bruto, total = ler_mrj(zip_path, linhas_por_bloco)
    finally:
        if apagar_zip and zip_path.exists():
            zip_path.unlink()
    if bruto.empty:
        raise RuntimeError(f"{ano}: nenhuma linha com CODMUNRES={COD_MRJ} em {total} linhas; confira o arquivo")

    pronto = preparar(bruto, ano)
    q = resumo_qualidade(pronto, bruto, ano)
    log.info("%d: %d nascidos vivos de residentes no MRJ (de %d no arquivo)", ano, len(bruto), total)
    for chave in ("contador_duplicado", "data_invalida", "data_fora_do_ano_do_arquivo"):
        if q[chave]:
            log.warning("%d: %s = %d", ano, chave, q[chave])
    if q["codigos_sem_rotulo"]:
        log.warning("%d: códigos sem rótulo: %s", ano, q["codigos_sem_rotulo"])

    gravar_parquet(bruto, PASTA_BRUTO / f"sinasc_mrj_bruto_{ano}.parquet")
    gravar_parquet(pronto, PASTA_PRONTO / f"sinasc_mrj_pronto_{ano}.parquet")

    datas = pd.to_datetime(pronto["data_nascimento"])
    reg = {
        "ano": ano, "url": origem["url"], "last_modified": origem["last_modified"],
        "content_length": origem["content_length"], "linhas_brasil": total, "linhas_mrj": len(bruto),
        "data_min": datas.min().strftime("%Y-%m-%d"), "data_max": datas.max().strftime("%Y-%m-%d"),
        "processado_em": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return {"manifesto": reg, "qualidade": q}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Nascidos vivos (SINASC) de residentes no município do Rio de Janeiro")
    p.add_argument("--anos", help="ex.: 2019-2026 ou 2019,2025 (padrão: 2019 até o ano corrente)")
    p.add_argument("--forcar", action="store_true", help="reprocessa mesmo que a origem não tenha mudado")
    p.add_argument("--apagar-zip", action="store_true", help="apaga o zip depois de processar")
    p.add_argument("--pasta-zip", default=str(RAIZ / ".cache_zip"), help="onde guardar os zips baixados")
    p.add_argument("--so-series", action="store_true", help="só refaz séries e qualidade a partir de dados/pronto")
    p.add_argument("--linhas-por-bloco", type=int, default=LINHAS_POR_BLOCO)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

    if args.so_series:
        refazer_saidas_agregadas()
        return 0

    anos = interpretar_anos(args.anos)
    sessao = _sessao()
    manifesto = ler_manifesto()
    qualidade = pd.read_csv(ARQ_QUALIDADE, dtype=str, keep_default_na=False) if ARQ_QUALIDADE.exists() else pd.DataFrame()
    falhas, processados = [], []

    for ano in anos:
        try:
            origem = consultar_origem(ano, sessao)
        except RuntimeError as e:
            log.error("%d: %s", ano, e)
            falhas.append(ano)
            continue
        if origem is None:
            log.info("%d: arquivo ainda não publicado, pulando", ano)
            continue

        atual = manifesto[manifesto["ano"] == str(ano)]
        existe = (PASTA_PRONTO / f"sinasc_mrj_pronto_{ano}.parquet").exists() and \
                 (PASTA_BRUTO / f"sinasc_mrj_bruto_{ano}.parquet").exists()
        inalterado = (not atual.empty and existe
                      and atual.iloc[0]["last_modified"] == origem["last_modified"]
                      and atual.iloc[0]["content_length"] == str(origem["content_length"]))
        if inalterado and not args.forcar:
            log.info("%d: sem mudança na origem (%s), mantido", ano, origem["last_modified"])
            continue

        try:
            res = processar_ano(ano, origem, Path(args.pasta_zip), sessao, args.apagar_zip, args.linhas_por_bloco)
        except Exception as e:  # noqa: BLE001 - um ano com problema não deve impedir os outros
            log.error("%d: %s", ano, e)
            falhas.append(ano)
            continue
        manifesto = atualizar_manifesto(manifesto, res["manifesto"])
        linha_q = pd.DataFrame([{k: str(v) for k, v in res["qualidade"].items()}])
        if not qualidade.empty:
            qualidade = qualidade[qualidade["ano_arquivo"] != str(ano)]
        qualidade = pd.concat([qualidade, linha_q], ignore_index=True).sort_values("ano_arquivo")
        # grava a cada ano, para não perder o que já foi processado se um ano posterior falhar
        _csv(manifesto, ARQ_MANIFESTO)
        _csv(qualidade, ARQ_QUALIDADE)
        processados.append(ano)

    refazer_saidas_agregadas()
    log.info("Processados: %s | Falhas: %s", processados or "nenhum", falhas or "nenhuma")
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())

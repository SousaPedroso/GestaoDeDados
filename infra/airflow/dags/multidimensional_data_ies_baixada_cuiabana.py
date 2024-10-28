"""
Pipeline orquestrado pelo airflow que faz a carga dos dados das
IES, trazendo somente a baixada cuiabana. É feita a separação
em múltiplas dimensões de interesse, pegando somente indicadores
de interesse para a análise.
"""
import os

import pandas as pd

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator

from dotenv import load_dotenv

DAG_ID = "censo_ensino_superior_ies_baixada_cuiabana"
TAGS = ["INEP", "IES", "Censo Ensino Superior", "Baixada Cuiabana"]

@dag(
    dag_id=DAG_ID,
    description="""Pipeline de dados de IES para IES da baixada cuiabana,
        com extração e carga dos dados coletados, movendo os dados para o datawarehouse
        no schema inep""",
    schedule_interval="@yearly",
    catchup=False,
    default_args={
        "owner": "sousapedroso",
        "start_date": "2024-10-20",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    tags=TAGS,
    max_active_runs=1,
)
def multidimensional_data_ies_baixada_cuiabana():
    """
    Pipeline de dados de IES para IES da baixada cuiabana, com extração e carga dos dados coletados,
    movendo os dados para o datawarehouse no schema inep
    """
    @task()
    def extract_baixada_cuiabana_data():
        """
        Extração dos dados das IES da baixada cuiabana
        tanto para cursos quanto para as IES
        """

        dados_cursos_ies_serie_historica = pd.DataFrame()
        dados_cadastrais_ies_serie_historica = pd.DataFrame()

        microdados_path = "/opt/airflow/data"
        for microdados_por_ano in os.listdir(microdados_path):
            microdados_do_ano = os.listdir(
                os.path.join(microdados_path, microdados_por_ano)
            )
            microdados_do_ano = [
                microdado.split(".CSV")[0] for microdado in microdados_do_ano if microdado.endswith(".CSV")
            ]

            colunas_usadas_cursos = [
                "CO_CURSO", "CO_REGIAO", "CO_UF", "CO_MUNICIPIO", "CO_IES", 
                "TP_GRAU_ACADEMICO", "TP_MODALIDADE_ENSINO", "TP_NIVEL_ACADEMICO", 
                "CO_CINE_ROTULO", "NO_CINE_ROTULO", "CO_CINE_AREA_GERAL", 
                "CO_CINE_AREA_ESPECIFICA", "CO_CINE_AREA_DETALHADA", "NU_ANO_CENSO",
                "NO_CINE_AREA_GERAL", "NO_CINE_AREA_ESPECIFICA", "NO_CINE_AREA_DETALHADA",
                "DESCRICAO_GRAU", "DESCRICAO_MODALIDADE", "DESCRICAO_NIVEL",
                "QT_MAT", "QT_MAT_FEM", "QT_MAT_MASC", "QT_MAT_DIURNO", "QT_MAT_NOTURNO",
                "QT_MAT_0_17", "QT_MAT_18_24", "QT_MAT_25_29", "QT_MAT_30_34", "QT_MAT_35_39",
                "QT_MAT_40_49", "QT_MAT_50_59", "QT_MAT_60_MAIS", "QT_MAT_BRANCA", "QT_MAT_PRETA",
                "QT_MAT_PARDA", "QT_MAT_AMARELA", "QT_MAT_INDIGENA", "QT_MAT_CORND"
            ]
            dados_cursos_ies = pd.read_csv(
                os.path.join(
                    os.path.join(microdados_path, microdados_por_ano,
                    f"MICRODADOS_CADASTRO_CURSOS_{microdados_do_ano[0][len(microdados_do_ano[0])-4:]}.CSV"
                    )
                ),
                sep=";", encoding="latin1",
                usecols=lambda column: column in colunas_usadas_cursos
            )

            for coluna in colunas_usadas_cursos:
                if (coluna not in dados_cursos_ies.columns):
                    dados_cursos_ies[coluna] = None


            colunas_usadas_ies = [
                "CO_IES", "NO_IES", "SG_IES", "DS_ENDERECO_IES", "DS_NUMERO_ENDERECO_IES",
                "DS_COMPLEMENTO_ENDERECO_IES", "NO_BAIRRO_IES", "NU_CEP_IES", "NO_MUNICIPIO_IES",
                "CO_REGIAO_IES", "NO_REGIAO_IES", "CO_UF_IES", "CO_MUNICIPIO_IES", "CO_MESORREGIAO_IES",
                "CO_MICRORREGIAO_IES", "TP_ORGANIZACAO_ACADEMICA", "TP_CATEGORIA_ADMINISTRATIVA",
                "CO_MANTENEDORA", "NO_MANTENEDORA", "NU_ANO_CENSO", "IN_CAPITAL_IES", "IN_ACESSO_PORTAL_CAPES", 
                "IN_ACESSO_OUTRAS_BASES", "IN_ASSINA_OUTRA_BASE", "IN_REPOSITORIO_INSTITUCIONAL", 
                "IN_BUSCA_INTEGRADA", "IN_SERVICO_INTERNET", "IN_PARTICIPA_REDE_SOCIAL", 
                "IN_CATALOGO_ONLINE", "QT_PERIODICO_ELETRONICO", "QT_LIVRO_ELETRONICO", 
                "QT_TEC_TOTAL", "QT_TEC_FUNDAMENTAL_INCOMP_FEM", "QT_TEC_FUNDAMENTAL_INCOMP_MASC", 
                "QT_TEC_FUNDAMENTAL_COMP_FEM", "QT_TEC_FUNDAMENTAL_COMP_MASC", "QT_TEC_MEDIO_FEM", 
                "QT_TEC_MEDIO_MASC", "QT_TEC_SUPERIOR_FEM", "QT_TEC_SUPERIOR_MASC", 
                "QT_TEC_ESPECIALIZACAO_FEM", "QT_TEC_ESPECIALIZACAO_MASC", "QT_TEC_MESTRADO_FEM", 
                "QT_TEC_MESTRADO_MASC", "QT_TEC_DOUTORADO_FEM", "QT_TEC_DOUTORADO_MASC", 
                "QT_DOC_TOTAL", "QT_DOC_EXE", "QT_DOC_EX_FEMI", "QT_DOC_EX_MASC", "QT_DOC_EX_SEM_GRAD", 
                "QT_DOC_EX_GRAD", "QT_DOC_EX_ESP", "QT_DOC_EX_MEST", "QT_DOC_EX_DOUT", "QT_DOC_EX_INT", 
                "QT_DOC_EX_INT_DE", "QT_DOC_EX_INT_SEM_DE", "QT_DOC_EX_PARC", "QT_DOC_EX_HOR", 
                "QT_DOC_EX_0_29", "QT_DOC_EX_30_34", "QT_DOC_EX_35_39", "QT_DOC_EX_40_44", 
                "QT_DOC_EX_45_49", "QT_DOC_EX_50_54", "QT_DOC_EX_55_59", "QT_DOC_EX_60_MAIS", 
                "QT_DOC_EX_BRANCA", "QT_DOC_EX_PRETA", "QT_DOC_EX_PARDA", "QT_DOC_EX_AMARELA", 
                "QT_DOC_EX_INDIGENA", "QT_DOC_EX_COR_ND", "QT_DOC_EX_BRA", "QT_DOC_EX_EST", 
                "QT_DOC_EX_COM_DEFICIENCIA", "NO_UF_IES", "SG_UF_IES", "NO_MESORREGIAO_IES", 
                "NO_MICRORREGIAO_IES"
            ]
            if int(microdados_do_ano[0][len(microdados_do_ano[0])-4:]) < 2022:
                dados_cadastrais_ies = pd.read_csv(
                    os.path.join(
                        os.path.join(microdados_path, microdados_por_ano,
                        f"MICRODADOS_CADASTRO_IES_{microdados_do_ano[0][len(microdados_do_ano[0])-4:]}.CSV"
                        )
                    ),
                    sep=";", encoding="latin1",
                    usecols=lambda column: column in colunas_usadas_ies
                )

            # A partir de 2022 mudou a nomenclatura do arquivo de cadastro de IES
            else:

                dados_cadastrais_ies = pd.read_csv(
                    os.path.join(
                        os.path.join(microdados_path, microdados_por_ano,
                        f"MICRODADOS_ED_SUP_IES_{microdados_do_ano[0][len(microdados_do_ano[0])-4:]}.CSV"
                        )
                    ),
                    sep=";", encoding="latin1",
                    usecols=lambda column: column in colunas_usadas_ies
                )

            for coluna in colunas_usadas_ies:
                if coluna not in dados_cadastrais_ies.columns:
                    dados_cadastrais_ies[coluna] = None

            # https://www.saude.mt.gov.br/unidade/escritorios-regionais-de-saude/696/baixada-cuiabana
            codigos_municipios_baixada_cuiabana = [
                5100102, 5101605, 5103007, 5103403, 5104906, 5106109,
                5106208, 5106455, 5106505, 5107800, 5108402
            ]
            dados_cadastrais_ies = dados_cadastrais_ies[
                (dados_cadastrais_ies["CO_UF_IES"] == 51) &
                (dados_cadastrais_ies["CO_MUNICIPIO_IES"].isin(codigos_municipios_baixada_cuiabana)
                )
            ]

            dados_cadastrais_ies["CO_CAPITAL_IES"] = dados_cadastrais_ies["IN_CAPITAL_IES"]
            dados_cadastrais_ies["IN_CAPITAL_IES"] = dados_cadastrais_ies["IN_CAPITAL_IES"].map(
                {1: "Sim", 0: "Não"}
            )

            dados_cadastrais_ies["CO_CATEGORIA_ADMINISTRATIVA"] = dados_cadastrais_ies["TP_CATEGORIA_ADMINISTRATIVA"]
            dados_cadastrais_ies["TP_CATEGORIA_ADMINISTRATIVA"] = dados_cadastrais_ies["TP_CATEGORIA_ADMINISTRATIVA"].map(
                {
                    1: "Pública Federal",
                    2: "Pública Estadual",
                    3: "Pública Municipal",
                    4: "Privada com fins lucrativos",
                    5: "Privada sem fins lucrativos",
                    6: "Privada - Particular em sentido estrito",
                    7: "Especial",
                    8: "Privada comunitária",
                    9: "Privada confessional"
                }
            )

            dados_cadastrais_ies["CO_ORGANIZACAO_ACADEMICA"] = dados_cadastrais_ies["TP_ORGANIZACAO_ACADEMICA"]
            dados_cadastrais_ies["TP_ORGANIZACAO_ACADEMICA"] = dados_cadastrais_ies["TP_ORGANIZACAO_ACADEMICA"].map(
                {
                    1: "Universidade",
                    2: "Centro Universitário",
                    3: "Faculdade",
                    4: "Instituto Federal de Educação, Ciência e Tecnologia",
                    5: "Centro Federal de Educação Tecnológica"
                }
            )

            dados_cursos_ies = dados_cursos_ies[
                (dados_cursos_ies["CO_UF"] == 51) &
                (dados_cursos_ies["CO_MUNICIPIO"].isin(codigos_municipios_baixada_cuiabana))
            ]

            dados_cursos_ies["CO_GRAU_ACADEMICO"] = dados_cursos_ies["TP_GRAU_ACADEMICO"]
            dados_cursos_ies["TP_GRAU_ACADEMICO"] = dados_cursos_ies["TP_GRAU_ACADEMICO"].map(
                {
                    1: "Bacharelado",
                    2: "Licenciatura",
                    3: "Tecnológico",
                    4: "Bacharelado e Licenciatura",
                }
            )

            dados_cursos_ies_serie_historica = pd.concat(
                [dados_cursos_ies_serie_historica, dados_cursos_ies]
            )
            dados_cadastrais_ies_serie_historica = pd.concat(
                [dados_cadastrais_ies_serie_historica, dados_cadastrais_ies]
            )

        dados_cadastrais_ies_serie_historica.to_csv(
            "/opt/airflow/output/dados_cadastrais_ies_baixada_cuiabana.csv",
            index=False
        )

        dados_cursos_ies_serie_historica.to_csv(
            "/opt/airflow/output/dados_cursos_ies_baixada_cuiabana.csv",
            index=False
        )

    @task()
    def set_dimensions():
        """
        Transformação dos dados, separando em múltiplas dimensões
        de interesse
        """
        dados_cadastrais_ies_serie_historica = pd.read_csv(
            "/opt/airflow/output/dados_cadastrais_ies_baixada_cuiabana.csv",
        )
        dados_cursos_ies_serie_historica = pd.read_csv(
            "/opt/airflow/output/dados_cursos_ies_baixada_cuiabana.csv"
        )

        dimensao_ies = dados_cadastrais_ies_serie_historica[[
            "CO_IES", "NO_IES", "SG_IES", "DS_ENDERECO_IES", "DS_NUMERO_ENDERECO_IES",
            "DS_COMPLEMENTO_ENDERECO_IES", "NO_BAIRRO_IES", "NU_CEP_IES",
            "CO_REGIAO_IES", "CO_UF_IES", "CO_MUNICIPIO_IES", "CO_MESORREGIAO_IES",
            "CO_MICRORREGIAO_IES", "TP_ORGANIZACAO_ACADEMICA", "TP_CATEGORIA_ADMINISTRATIVA",
            "CO_MANTENEDORA", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_municipio = dados_cadastrais_ies_serie_historica[[
            "CO_MUNICIPIO_IES", "NO_MUNICIPIO_IES", "IN_CAPITAL_IES", "CO_CAPITAL_IES", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_categoria_administrativa = dados_cadastrais_ies_serie_historica[[
            "CO_CATEGORIA_ADMINISTRATIVA", "TP_CATEGORIA_ADMINISTRATIVA", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_organizacao_academica = dados_cadastrais_ies_serie_historica[[
            "CO_ORGANIZACAO_ACADEMICA", "TP_ORGANIZACAO_ACADEMICA", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_mantenedora = dados_cadastrais_ies_serie_historica[[
            "CO_MANTENEDORA", "NO_MANTENEDORA", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_infraestrutura = dados_cadastrais_ies_serie_historica[[
            "CO_IES", "IN_ACESSO_PORTAL_CAPES", "IN_ACESSO_OUTRAS_BASES", 
            "IN_ASSINA_OUTRA_BASE", "IN_REPOSITORIO_INSTITUCIONAL", 
            "IN_BUSCA_INTEGRADA", "IN_SERVICO_INTERNET", 
            "IN_PARTICIPA_REDE_SOCIAL", "IN_CATALOGO_ONLINE", 
            "QT_PERIODICO_ELETRONICO", "QT_LIVRO_ELETRONICO", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_tecnico_administrativo = dados_cadastrais_ies_serie_historica[[
            "CO_IES", "QT_TEC_TOTAL", "QT_TEC_FUNDAMENTAL_INCOMP_FEM", 
            "QT_TEC_FUNDAMENTAL_INCOMP_MASC", "QT_TEC_FUNDAMENTAL_COMP_FEM", 
            "QT_TEC_FUNDAMENTAL_COMP_MASC", "QT_TEC_MEDIO_FEM", 
            "QT_TEC_MEDIO_MASC", "QT_TEC_SUPERIOR_FEM", "QT_TEC_SUPERIOR_MASC", 
            "QT_TEC_ESPECIALIZACAO_FEM", "QT_TEC_ESPECIALIZACAO_MASC", 
            "QT_TEC_MESTRADO_FEM", "QT_TEC_MESTRADO_MASC", 
            "QT_TEC_DOUTORADO_FEM", "QT_TEC_DOUTORADO_MASC", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_docente = dados_cadastrais_ies_serie_historica[[
            "CO_IES", "QT_DOC_TOTAL", "QT_DOC_EXE", "QT_DOC_EX_FEMI", 
            "QT_DOC_EX_MASC", "QT_DOC_EX_SEM_GRAD", "QT_DOC_EX_GRAD", 
            "QT_DOC_EX_ESP", "QT_DOC_EX_MEST", "QT_DOC_EX_DOUT", 
            "QT_DOC_EX_INT", "QT_DOC_EX_INT_DE", "QT_DOC_EX_INT_SEM_DE", 
            "QT_DOC_EX_PARC", "QT_DOC_EX_HOR", "QT_DOC_EX_0_29", 
            "QT_DOC_EX_30_34", "QT_DOC_EX_35_39", "QT_DOC_EX_40_44", 
            "QT_DOC_EX_45_49", "QT_DOC_EX_50_54", "QT_DOC_EX_55_59", 
            "QT_DOC_EX_60_MAIS", "QT_DOC_EX_BRANCA", "QT_DOC_EX_PRETA", 
            "QT_DOC_EX_PARDA", "QT_DOC_EX_AMARELA", "QT_DOC_EX_INDIGENA", 
            "QT_DOC_EX_COR_ND", "QT_DOC_EX_BRA", "QT_DOC_EX_EST", 
            "QT_DOC_EX_COM_DEFICIENCIA", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_regiao = dados_cadastrais_ies_serie_historica[[
            "CO_REGIAO_IES", "NO_REGIAO_IES", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_uf = dados_cadastrais_ies_serie_historica[[
            "CO_UF_IES", "NO_UF_IES", "SG_UF_IES", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_mesorregiao = dados_cadastrais_ies_serie_historica[[
            "CO_MESORREGIAO_IES", "NO_MESORREGIAO_IES", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_microrregiao = dados_cadastrais_ies_serie_historica[[
            "CO_MICRORREGIAO_IES", "NO_MICRORREGIAO_IES", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_curso = dados_cursos_ies_serie_historica[[
            "CO_CURSO", "CO_REGIAO", "CO_UF", "CO_MUNICIPIO", "CO_IES", 
            "TP_GRAU_ACADEMICO", "TP_MODALIDADE_ENSINO", "TP_NIVEL_ACADEMICO", 
            "CO_CINE_ROTULO", "NO_CINE_ROTULO", "CO_CINE_AREA_GERAL", 
            "CO_CINE_AREA_ESPECIFICA", "CO_CINE_AREA_DETALHADA", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_cine_area = dados_cursos_ies_serie_historica[[
            "CO_CINE_AREA_GERAL", "NO_CINE_AREA_GERAL", "CO_CINE_AREA_ESPECIFICA", 
            "NO_CINE_AREA_ESPECIFICA", "CO_CINE_AREA_DETALHADA", "NO_CINE_AREA_DETALHADA", 
            "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_grau_academico = dados_cursos_ies_serie_historica[[
            "TP_GRAU_ACADEMICO", "DESCRICAO_GRAU", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_modalidade_ensino = dados_cursos_ies_serie_historica[[
            "TP_MODALIDADE_ENSINO", "DESCRICAO_MODALIDADE", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_nivel_academico = dados_cursos_ies_serie_historica[[
            "TP_NIVEL_ACADEMICO", "DESCRICAO_NIVEL", "NU_ANO_CENSO"
        ]].drop_duplicates()

        dimensao_matricula = dados_cursos_ies_serie_historica[[
            "CO_IES", "CO_CURSO", "NU_ANO_CENSO", "QT_MAT", "QT_MAT_FEM", "QT_MAT_MASC", 
            "QT_MAT_DIURNO", "QT_MAT_NOTURNO", "QT_MAT_0_17", "QT_MAT_18_24", 
            "QT_MAT_25_29", "QT_MAT_30_34", "QT_MAT_35_39", "QT_MAT_40_49", 
            "QT_MAT_50_59", "QT_MAT_60_MAIS", "QT_MAT_BRANCA", "QT_MAT_PRETA", 
            "QT_MAT_PARDA", "QT_MAT_AMARELA", "QT_MAT_INDIGENA", "QT_MAT_CORND"
        ]].drop_duplicates()

        # Salvar as dimensões para fazer a posterior carga com o postgres
        dimensao_ies.to_csv(
            "/opt/airflow/output/dim_ies.csv",
            index=False
        )

        dimensao_municipio.to_csv(
            "/opt/airflow/output/dim_municipio_ies.csv",
            index=False
        )

        dimensao_categoria_administrativa.to_csv(
            "/opt/airflow/output/dim_categoria_administrativa_ies.csv",
            index=False
        )

        dimensao_organizacao_academica.to_csv(
            "/opt/airflow/output/dim_organizacao_academica_ies.csv",
            index=False
        )

        dimensao_mantenedora.to_csv(
            "/opt/airflow/output/dim_mantenedora_ies.csv",
            index=False
        )

        dimensao_infraestrutura.to_csv(
            "/opt/airflow/output/dim_infraestrutura_ies.csv",
            index=False
        )

        dimensao_tecnico_administrativo.to_csv(
            "/opt/airflow/output/fato_tecnico_administrativo_ies.csv",
            index=False
        )

        dimensao_docente.to_csv(
            "/opt/airflow/output/fato_docente_ies.csv",
            index=False
        )

        dimensao_regiao.to_csv(
            "/opt/airflow/output/dim_regiao_ies.csv",
            index=False
        )

        dimensao_uf.to_csv(
            "/opt/airflow/output/dim_uf_ies.csv",
            index=False
        )

        dimensao_mesorregiao.to_csv(
            "/opt/airflow/output/dim_mesorregiao_ies.csv",
            index=False
        )

        dimensao_microrregiao.to_csv(
            "/opt/airflow/output/dim_microrregiao_ies.csv",
            index=False
        )

        dimensao_curso.to_csv(
            "/opt/airflow/output/dim_curso_ies.csv",
            index=False
        )

        dimensao_cine_area.to_csv(
            "/opt/airflow/output/dim_cine_area_curso_ies.csv",
            index=False
        )

        dimensao_grau_academico.to_csv(
            "/opt/airflow/output/dim_grau_academico_curso_ies.csv",
            index=False
        )

        dimensao_modalidade_ensino.to_csv(
            "/opt/airflow/output/dim_modalidade_ensino_curso_ies.csv",
            index=False
        )

        dimensao_nivel_academico.to_csv(
            "/opt/airflow/output/dim_nivel_academico_curso_ies.csv",
            index=False
        )

        dimensao_matricula.to_csv(
            "/opt/airflow/output/fato_matricula_curso_ies.csv",
            index=False
        )

        os.remove("/opt/airflow/output/dados_cadastrais_ies_baixada_cuiabana.csv")
        os.remove("/opt/airflow/output/dados_cursos_ies_baixada_cuiabana.csv")

    @task()
    def load_data():
        """
        Tarefa que faz a carga dos dados no datawarehouse
        """
        load_dotenv(
            os.path.normpath(
                os.path.join(
                    os.path.dirname(__file__),"..", ".env"
                )
            )
        )
        postgres_user = os.getenv("POSTGRES_USER")
        postgres_password = os.getenv("POSTGRES_PASSWORD")
        postgres_host = os.getenv("POSTGRES_HOST")
        postgres_port = os.getenv("POSTGRES_PORT")
        postgres_db = os.getenv("POSTGRES_DB")

        for table in os.listdir("/opt/airflow/output"):
            table_data = pd.read_csv(
                f"/opt/airflow/output/{table}",
                sep=",", encoding="latin1"
            )

            table_data.to_sql(
                table.split('.')[0],
                con=f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}?client_encoding=utf8",
                index=False,
                if_exists="replace",
                schema="inep"
            )


    clean_intermediary_files = BashOperator(
        task_id="clean_intermediary_files",
        bash_command="""
            rm -rf /opt/airflow/output/*
        """,
    )

    chain(extract_baixada_cuiabana_data(), set_dimensions(), load_data(), clean_intermediary_files)

DAG = multidimensional_data_ies_baixada_cuiabana()

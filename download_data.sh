#!/bin/bash

FIRST_YEAR=${1:-2014}
LAST_YEAR=${2:-2023}
ANALYSIS_PERIOD=${3:-1}

echo "Primeiro ano para pegar os dados"
echo "$FIRST_YEAR"

echo "Ultimo ano para pegar os dados"
echo "$LAST_YEAR"

download_path="infra/open_metadata/custom-connector/data"
metadata_path="infra/open_metadata/custom-connector/metadata"

mkdir -p $download_path
mkdir -p $metadata_path

for ((ano_censo=$FIRST_YEAR; ano_censo<=$LAST_YEAR; ano_censo=ano_censo+$ANALYSIS_PERIOD))
do
    curl -o "$download_path/microdados_censo_da_educacao_superior_$ano_censo.zip" \
    "http://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_$ano_censo.zip"
    unzip -o "$download_path/microdados_censo_da_educacao_superior_$ano_censo.zip" -d "$download_path/microdados_censo_da_educacao_superior_$ano_censo"
    rm "$download_path/microdados_censo_da_educacao_superior_$ano_censo.zip"
    mkdir -p $metadata_path/microdados_censo_da_educacao_superior_$ano_censo
    mv $download_path/microdados_censo_da_educacao_superior_$ano_censo/Microdados\ do\ Censo\ da\ EducaЗ╞o\ Superior\ $ano_censo/Anexos/* $metadata_path/microdados_censo_da_educacao_superior_$ano_censo
    mv $download_path/microdados_censo_da_educacao_superior_$ano_censo/Microdados\ do\ Censo\ da\ EducaЗ╞o\ Superior\ $ano_censo/leia-me/* $metadata_path/microdados_censo_da_educacao_superior_$ano_censo
    mv $download_path/microdados_censo_da_educacao_superior_$ano_censo/Microdados\ do\ Censo\ da\ EducaЗ╞o\ Superior\ $ano_censo/dados/* $download_path/microdados_censo_da_educacao_superior_$ano_censo
    rm -r $download_path/microdados_censo_da_educacao_superior_$ano_censo/Microdados\ do\ Censo\ da\ EducaЗ╞o\ Superior\ $ano_censo/
    # 2022
    mv $download_path/microdados_censo_da_educacao_superior_$ano_censo/microdados_educaЗ╞o_superior_$ano_censo/Anexos/* $metadata_path/microdados_censo_da_educacao_superior_$ano_censo
    mv $download_path/microdados_censo_da_educacao_superior_$ano_censo/microdados_educaЗ╞o_superior_$ano_censo/leia-me/* $metadata_path/microdados_censo_da_educacao_superior_$ano_censo
    mv $download_path/microdados_censo_da_educacao_superior_$ano_censo/microdados_educaЗ╞o_superior_$ano_censo/dados/* $download_path/microdados_censo_da_educacao_superior_$ano_censo
    rm -r $download_path/microdados_censo_da_educacao_superior_$ano_censo/microdados_educaЗ╞o_superior_$ano_censo/
    # 2023
    mv $download_path/microdados_censo_da_educacao_superior_$ano_censo/microdados_censo_da_educacao_superior_$ano_censo/Anexos/* $metadata_path/microdados_censo_da_educacao_superior_$ano_censo
    mv $download_path/microdados_censo_da_educacao_superior_$ano_censo/microdados_censo_da_educacao_superior_$ano_censo/leia-me/* $metadata_path/microdados_censo_da_educacao_superior_$ano_censo
    mv $download_path/microdados_censo_da_educacao_superior_$ano_censo/microdados_censo_da_educacao_superior_$ano_censo/dados/* $download_path/microdados_censo_da_educacao_superior_$ano_censo
    rm -r $download_path/microdados_censo_da_educacao_superior_$ano_censo/microdados_censo_da_educacao_superior_$ano_censo/
done

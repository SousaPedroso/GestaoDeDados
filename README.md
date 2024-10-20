## Introdução

Projeto implementado, a partir dos OKR's apresentados abaixo,
com o objetivo de diminuir a evasão escolar das IES na Baixada Cuiabana.

![Key Results do Objetivo de diminuição da evasão escolar das IES na Baixada Cuiabana](/images/KeyResults.png)

Dentre as inúmeras formas de elaboração de arquitetura de dados, definidos os resultados chaves, foi construída a arquitetura de dados abaixo, na qual há a separação de camadas de acordo com o ciclo de vida do dado, com papéis bem definidos para cada etapa (pode haver intersecção entre uma camada ou outra).

![Imagem da arquitetura](/images/Arquitetura%20de%20Dados%20INEP.drawio.png)

Nesse projeto, estamos trabalhando com somente uma fonte de dados, a partir do Censo da Educação Superior do INEP, no qual são pegos os dados dos últimos 10 anos. Na camada de ingestão são definidos glossários e classificações para os dados, bem como dicionários de dados, finalizando com uma etapa de ETL para carga num banco Postgres como datawarehouse separando em algumas dimensões de interesse, bem como fatos (desagregando um pouco o fornecido pelo censo).

Para processamento, a equipe de ciência de dados poderá utilizar pandas ou spark a fim de atingir os KR's 2 e 4. Por fim, poderá ser utilizada uma visualização para extração de insights; com acesso por pessoas do negócio (membros de diferentes setores da educação, de nível federal a municipal), bem como cientistas de dados; pensando nos dados prontos para consumo na camada de armazenamento.

## Execução

Os requisitos básicos para execução do projeto são `docker` e `make`. Você pode considerar usar wsl ou mac, no entanto devido a configurações de volume e rede, não há garantias de plena execução em ambos SO's. Se verificar problemas e quiser contribuir com modificações que possam contribuir para todos SO's, fique a vontade de fazer um fork e abrir um PR.

Para as variáveis de ambiente, em produção considere criar um arquivo *.env* no [openmetadata](/infra/open_metadata/custom-connector/docker/).

Os dados utilizados vêm do [censo escolar das IES de 2014 a 2023](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior). O [script](/download_data.sh) auxilia no download desses dados para você, podendo utilizar da seguinte forma para personalizar sua futura análise:

```sh
bash download_data.sh # baixa todos os dados de 2014 a 2023
bash download_data.sh 2016 # baixa todos os dados a partir de 2016
bash download_data.sh 2014 2021 # baixa todos os dados até 2021
bash download_data.sh 2014 2023 2 # baixa todos bienalmente (a cada 2 anos)
```

Para inicializar o projeto, considere em um novo terminal estar no diretório do [openmetadata](/infra/open_metadata/) e executar `make run`, e a fim de parar a aplicação `make stop`. Vale salientar que sob mudanças que impliquem na imagem, você deve rebuildar a imagem.

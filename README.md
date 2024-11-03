## Introdução

Projeto implementado, a partir dos OKR's apresentados abaixo,
com o objetivo de diminuir a evasão escolar das IES na Baixada Cuiabana.

![Key Results do Objetivo de diminuição da evasão escolar das IES na Baixada Cuiabana](/images/KeyResults.png)

Dentre as inúmeras formas de elaboração de arquitetura de dados, definidos os resultados chaves, foi construída a arquitetura de dados abaixo, na qual há a separação de camadas de acordo com o ciclo de vida do dado, com papéis bem definidos para cada etapa (pode haver intersecção entre uma camada ou outra).

![Imagem da arquitetura](/images/Arquitetura%20de%20Dados%20INEP.drawio.png)

Nesse projeto, estamos trabalhando com somente uma fonte de dados, a partir do Censo da Educação Superior do INEP, no qual são pegos os dados dos últimos 10 anos. Na camada de ingestão são definidos glossários e classificações para os dados, bem como dicionários de dados, finalizando com uma etapa de ETL para carga num banco Postgres como datawarehouse separando em algumas dimensões de interesse, bem como fatos (desagregando um pouco o fornecido pelo censo).

Para processamento, a equipe de ciência de dados poderá utilizar pandas ou spark a fim de atingir os KR's 2 e 4. Por fim, poderá ser utilizada uma visualização para extração de insights; com acesso por pessoas do negócio (membros de diferentes setores da educação, de nível federal a municipal), bem como cientistas de dados; pensando nos dados prontos para consumo na camada de armazenamento.

O datawarehouse foi modelado em multidimensões com 5 tabelas fatos e 18 tabelas dimensões. Vale salientar que aqui foi feita essa simulação da modelagem pensando em um cenário anterior a consolidação dos dados do censo num único arquivo. Para fazer o gerenciamento de metadados do datawarehouse e da fonte de origem dos dados (pense num stage por exemplo) - que nesse caso estão em formato csv e vêm da página do inep.

Para trabalhar com a governança de dados, foram pensados em 3 artefatos: dicionário de dados, glossário de dados e classificação de dados; através do OpenMetadata. O glossário de dados e o catálogo de dados encontram-se na pasta [metadata](/metadata/) do projeto.

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

Para facilitar na criação das DAGs, foi considerado o diretório [airflow](/infra/open_metadata/custom-connector/airflow/), em que estão sendo acessados os pacotes necessários pelo Openmetadata para comunicação entre os serviços. Considere no diretório do airflow, o comando abaixo para criar os arquivos você, para evitar criação como `root` pelo container.

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Além disso, está sendo usado o pyenv + poetry para gerenciar esse isolamento sem afetar outros projetos da máquina. O [setup.sh](/infra/open_metadata/custom-connector/airflow/setup.sh) faz as instalações necessárias. O [init_datawarehouse.sql](/infra/open_metadata/custom-connector/airflow/init_datawarehouse.sql) é quem faz a criação do schema e tabelas utilizados. Em uso de um posgres próprio, considere a utilização do código. Apesar da imagem usar o requirements.txt, não foi ali mapeado para caso queira fazer personalizações ainda com o poetry. Nesse caso, no diretório do airflow, `poetry add pacote`, e quando estiver satisfeito, gere o requirements.txt com `poetry export -o requirements.txt`. Vale salientar também que o datawarehouse está sendo simulado com o [render](https://render.com/), por isso não há especificidade local. Caso queria utilizar também, após a configuração da instância, adicione no arquivo [.env](/infra/open_metadata/custom-connector/docker/.env) suas credenciais.

```sh
POSTGRES_HOST=
POSTGRES_DB=
POSTGRES_USER=
POSTGRES_PORT=
POSTGRES_PASSWORD=
```


Para inicializar o projeto, considere em um novo terminal estar no diretório do [openmetadata](/infra/open_metadata/) e executar `make run_openmetadata`, e a fim de parar a aplicação `make stop_openmetadata`. Vale salientar que sob mudanças que impliquem na imagem, você deve rebuildar a imagem; removendo a mesma antes devido ao cache.

Após a inicialização, é possível trabalhar com a governança de dados. O [glossário](/metadata/glossario_educacao.csv) você pode subir por upload no OpenMetadata e fazer as conexões com os dados diretamente. Ao menos, a versão 1.4.4 aqui utilizada nesse projeto não disponibiliza o upload de [classificação de dados](/metadata/classificacao_educacao.csv), toda a estrutura e tags ali definidas você deve inserir manualmente. Inclusive, é necessário subir antes, para poder fazer o mapeamento do glossário com a classificação. Caso não queira esse mapeamento, remova **os dados** da coluna _tags_ do glossário. As classificações trazidas, vieram em conjunto do trabalho com a Tamara Aguiar Tavares Mascarenhas; com algumas descrições trazidas baseadas na mesma, junto com a [conceituação da unicesumar](https://www.unicesumar.edu.br/blog/diferenca-entre-faculdade-centro-universitario-e-universidade/). Para fins de teste, de qualquer forma, só foram considerados ainda duas tags de classificação no glossário, então é relativamente rápido para adequar a classificação.

Logo abaixo, está um exemplo do funcionamento da governança dos dados. Com relação ao [schema](/infra/airflow/schema_microdados_censo.csv), ali define o schema dos dados e consequentemente
será possível associar um glossário e _classificar_ os dados. O GIF abaixo mostra a criação da conexão ao schema, para que depois seja possível trabalhar
sobre os metadado. O dicionário, é a partir de cada coluna, podendo dar a descrição, bem como associar glossário e classificação. O glossário, é só você
criar um (certifique-se que esteja rodando o serviço do OpenMetadata) na página do [glossário](http://localhost:8585/glossary),
clicar gerenciar glossário e importar o arquivo que está em [metadata](/metadata/glossario_educacao.csv). Lembre-se antes de criar a classificação,
em [tags](http://localhost:8585/tags); e você pode criar somente duas [conforme está mo glossário], inclusive mantenha os mesmos nomes para compatibilidade.
Para criar o connector do postgres, o processo é semelhante ao gif, no entanto, você selecionará o banco posgres, e colocará informações da
conexão, como ocorre no passo a passo ao lado que será mostrado ao selecionar.

![gif schema dos dados csv](/images/insercao_schem_dados_brutos_inep_csv.gif)

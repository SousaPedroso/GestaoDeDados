Projeto implementado, a partir dos OKR's apresentados abaixo,
com o objetivo de diminuir a evasão escolar das IES na Baixada Cuiabana.

![Key Results do Objetivo de diminuição da evasão escolar das IES na Baixada Cuiabana](/images/KeyResults.png)

Dentre as inúmeras formas de elaboração de arquitetura de dados, definidos os resultados chaves, foi construída a arquitetura de dados abaixo, na qual há a separação de camadas de acordo com o ciclo de vida do dado, com papéis bem definidos para cada etapa (pode haver intersecção entre uma camada ou outra).

![Imagem da arquitetura](/images/Arquitetura%20de%20Dados%20INEP.drawio.png)

Nesse projeto, estamos trabalhando com somente uma fonte de dados, a partir do Censo da Educação Superior do INEP, no qual são pegos os dados dos últimos 10 anos. Na camada de ingestão são definidos glossários e classificações para os dados, bem como dicionários de dados, finalizando com uma etapa de ETL para carga num banco Postgres como datawarehouse separando em algumas dimensões de interesse, bem como fatos (desagregando um pouco o fornecido pelo censo).

Para processamento, a equipe de ciência de dados poderá utilizar pandas ou spark a fim de atingir os KR's 2 e 4. Por fim, poderá ser utilizada uma visualização para extração de insights; com acesso por pessoas do negócio (membros de diferentes setores da educação, de nível federal a municipal), bem como cientistas de dados; pensando nos dados prontos para consumo na camada de armazenamento.

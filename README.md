<div id="top"></div>

## Arquitetura streaming usando Kafka,  Spark e Debezium  

**Change Data Capture** (CDC) é um padrão de integração de dados que permite a identificação, captura e transmissão de eventos que ocorreram em uma banco de dados transacional.

Existem algumas abordagens possíveis para o CDC, aqui iremos usar a ferramenta **Debezium**, um projeto open source  que usa a abordagem  Log-Base, ou seja, existe um conector específico para a fontes de dados no qual ele se conecta ao banco de dados para ler as alterações registradas no log.

O Debezium possui conectores para diferentes fontes de dados (MySQL, MongoDB, PostgreSQL, SQL Server, Oracle). Aqui iniciaremos mostrando a integração com o **PostgreSQL**.

Uma vez que uma operação de insert, delete ou update de uma tabela que está sendo monitorado é identificado e capturado, o Debezium pode transmitir essas informações para uma infraestrutura de mensageria. É bastante comum fazer essa integração a um cluster **Apache Kafka** por meio do Apache Kafka Connect. Recentemente, participei de dois projetos em que optaram usar uma plataforma de mensageria de nuvem, Amazon Kinesis e Azure Event Hubs. Também existe conectores para Google Cloud Pub/Sub e outras plataformas open source como Redis e Apache Pulsar.

Nesse projeto usaremos um nó Kafka atuando no formato Kafka Raft (kraft), ou seja, sem depender um cluster Zookeeper e com um nó que é ao mesmo tempo controller e broker.

Para processar os eventos de mudanças que serão inseridos nos tópicos do cluster Kafka, usaremos um nó **Apache Spark** Standalone onde iremos submeter um job escrito em PySpark e usando a abordagem Structured Streaming. Esse job irá ler as mensagens escritas em alguns dos tópicos, processar a informação, realizar algumas transformações e salvar os dados em um banco **Delta Lake**.


### Componentes da Solução

Aqui está a lista dos projetos e bibliotecas que usamos para construir essa solução:

* [PostreSQL](https://www.postgresql.org/)
* [Debezium](https://debezium.io/)
* [Apache Kafka](https://kafka.apache.org/)
* [Apache Spark](https://spark.apache.org/)
* [Delta Lake](https://delta.io/)
* [JupyterLab](https://jupyter.org/)
* [Docker](https://www.docker.com/)

<p align="right">(<a href="#top">voltar ao início</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

Aqui um exemplo de como deixar o ambiente operacional para realizar alguns testes do ambiente de CDC.

### Pré-requisitos

Todas as operações serão realizadas em plataformas instaladas em containeres de Docker. Portanto, precisamos que o docker e o docker compose esteja instalado corretamente.

[Aqui está o link da documentação do Docker](https://docs.docker.com/compose/install/) para realizar a instalação.

Para testar se a instalação está correta, pode ser usado o comando `docker-compose --version`. Você deve ver um resultado similar ao apresentado abaixo.

```sh
   $ docker-compose --version
   docker-compose version 1.29.2, build 1110ad01
   ```


### Instalalação

1. Clone o repositório
   ```sh
   git clone https://github.com/walmeidadf/cdc.git
   ```
2. Construa e execute o aplicativo com o comando do Docker Compose na pasta do projeto.
   ```sh
   cd cdc
   docker-compose up
   ```
3. Opcionalmente, altere os registros para resolução de nomes dos containeres. Se você estiver usando Linux, seria o arquivo `/etc/hosts`.
   ```sh
   172.26.0.2       db_source
   172.26.0.5       kafka-1
   172.26.0.6       jupyter_spark
   ```

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- USAGE EXAMPLES -->
## Uso

Para testar a solução, pode ser usado um notebook através do JupyterLab da máquina `jupyter_spark` usando o link `http://jupyter_spark:8080`.

Será solicitado um token para acessar o ambiente. Use o seguinte comando para identificar o token:
```sh
docker exec cdc_jupyter_spark jupyter server list | grep token
```
Na pasta `work` existem alguns notebook que dão alguns exemplos de como testar a arquiettura.
<p align="right">(<a href="#top">voltar ao início</a>)</p>



<!-- ROADMAP -->
## Roadmap

- [x] Adicionar as referências
- [x] Documentar os notebooks para simplificar a navegação dos primeiros passos
- [x] Configurar o container do Jupyter para adicionar as bibliotecas Python
- [ ] Criar os scritps Python em streaming
- [ ] Fazer a documentação em inglês.
- [ ] Adicionar novas fontes de dados
    - [ ] MySQL
    - [ ] MongoDB
- [ ] Adicionar o lab com Apache Pulsar
- [ ] Adicionar o lab com Redis

<p align="right">(<a href="#top">back to top</a>)</p>


<!-- CONTACT -->
## Contato

Wesley Almeida - [@walmeidadf](https://twitter.com/your_username) - walmeida@gmail.com

Link do Projeto: [https://github.com/walmeidadf/cdc](https://github.com/walmeidadf/cdc)

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Referências

Algumas das páginas de documentação, artigos e posts que me ajudaram a desenvolver esse projeto:

* [Debezium connector for PostgreSQL](https://debezium.io/documentation/reference/1.9/connectors/postgresql.html)
* [Jupyter Notebook Python, Spark Stack](https://hub.docker.com/r/jupyter/pyspark-notebook)

<p align="right">(<a href="#top">back to top</a>)</p>


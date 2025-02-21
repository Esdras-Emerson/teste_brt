# BRT GPS Data Pipeline

Este projeto coleta dados de GPS dos ônibus BRT, salva-os em um arquivo CSV e carrega-os em um banco de dados PostgreSQL.

## Pré-requisitos

- Docker e Docker Compose
- Python 3.8+
- Prefect=0.15.9
- Bibliotecas Python: `requests`, `psycopg2`, `csv`
- dbt (Data Build Tool)

## Configuração

1. Clone o repositório:

    ```sh
    git clone <URL_DO_REPOSITORIO>
    cd <NOME_DO_REPOSITORIO>
    ```

2. Configure e inicie o banco de dados PostgreSQL usando Docker Compose:

    ```sh
    docker-compose up -d
    ```

3. Crie um ambiente virtual:

    ```sh
    python -m venv venv
    source venv/bin/activate  # No Windows use `venv\Scripts\activate`
    ```

4. Instale as dependências Python:

    ```sh
    pip install -r requirements.txt
    ```

5. Instale o dbt:

    ```sh
    pip install dbt-postgres
    ```

## Execução

1. Execute o fluxo Prefect:

    ```sh
    python prefect_flows/brt_flow.py
    ```

2. Execute o dbt para transformar os dados:

    ```sh
    cd /c:/Users/<seu_usuario>/teste_rj/dbt_project
    dbt run
    ```

## Estrutura do Projeto

- `docker-compose.yml`: Configuração do Docker Compose para o banco de dados PostgreSQL.
- `prefect_flows/brt_flow.py`: Script Prefect que coleta dados de GPS, salva em CSV e carrega no PostgreSQL.
- `scripts/init_db.sql`: Script SQL para inicializar o banco de dados.
- `dbt_project/`: Diretório contendo os arquivos de configuração e modelos do dbt.

## Notas

- Certifique-se de que o banco de dados PostgreSQL está em execução antes de iniciar o fluxo Prefect com o comando "docker ps".
- O fluxo Prefect está configurado para ser executado a cada minuto. Você pode ajustar o intervalo no arquivo `brt_flow.py`.
- O dbt é usado para transformar os dados carregados no banco de dados PostgreSQL.

## Contato

Para mais informações, entre em contato com [esdrasemerson@gmail.com].
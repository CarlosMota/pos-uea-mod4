
# Supply Chain Data Lake

Projeto final do Módulo 4 - Pós UEA  
Este projeto organiza e processa dados de supply chain utilizando uma arquitetura de data lake.

## Sumário

- [Supply Chain Data Lake](#supply-chain-data-lake)
  - [Sumário](#sumário)
  - [Pré-requisitos](#pré-requisitos)
  - [Configuração do Ambiente](#configuração-do-ambiente)
  - [Instalação de Dependências](#instalação-de-dependências)
  - [Arquitetura do Projeto](#arquitetura-do-projeto)
  - [Execução](#execução)
  - [Licença](#licença)

## Pré-requisitos

- Python 3.8 ou superior
- [pip](https://pip.pypa.io/en/stable/)
- [virtualenv](https://virtualenv.pypa.io/en/latest/) (opcional, mas recomendado)

## Configuração do Ambiente

Crie um ambiente virtual para isolar as dependências do projeto:

```sh
python -m venv .venv
```

Ative o ambiente virtual:

- **Windows:**

  ```sh
  .venv\Scripts\activate
  ```

- **Linux/Mac:**

  ```sh
  source .venv/bin/activate
  ```

## Instalação de Dependências

Com o ambiente virtual ativado, instale os pacotes necessários:

```sh
pip install -r requirements.txt
```

## Arquitetura do Projeto

```
.
├── mod4atv7.py                # Script principal de processamento
├── requirements.txt           # Dependências do projeto
├── README.md                  # Este arquivo
├── data_lake/
│   ├── 01_raw/                # Dados brutos
│   │   └── supply_chain_data_mao.csv
│   ├── 02_preprocessed/       # Dados pré-processados
│   │   ├── supply_chain_data_mao.csv
│   │   └── supply_chain_data_mao.parquet
│   ├── 03_analytics/          # Dados para análises
│   ├── 04_processed/          # Dados processados finais
│   └── 05_metadados/          # Metadados dos datasets
│       ├── catalogo_datalake.json
│       ├── metadados_supply_chain_data_mao_proc_csv.json
│       ├── metadados_supply_chain_data_mao_proc_parquet.json
│       └── metadados_supply_chain_data_mao.json
```

- **01_raw:** Dados originais, sem tratamento.
- **02_preprocessed:** Dados limpos e convertidos para outros formatos.
- **03_analytics:** Dados prontos para análises.
- **04_processed:** Dados finais prontos para consumo.
- **05_metadados:** Arquivos de metadados e catálogo do data lake.

## Execução

Para rodar o script principal:

```sh
python mod4atv7.py
```

## Licença

Consulte o arquivo [LICENSE](LICENSE) para mais informações.

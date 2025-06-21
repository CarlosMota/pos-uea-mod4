
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
data_lake/
├── 01_raw/            
├── 02_preprocessed/   
├── 03_analytics/      
├── 04_processed/      
├── 05_metadados/

src/
├── __init__.py
├── features_selection.py
├── preprocessed.py
├── script_PCA.py
├── utils.py
```

- **01_raw:** Dados originais, sem tratamento.
- **02_preprocessed:** Dados limpos e convertidos para outros formatos.
- **03_analytics:** Dados prontos para análises.
- **04_processed:** Dados finais prontos para consumo.
- **05_metadados:** Arquivos de metadados e catálogo do data lake.

## Execução

Rodar as células dos arquivos:

1. preprocessed.py
2. script_PCA.py
3. feature_selection

## Licença

Consulte o arquivo [LICENSE](LICENSE) para mais informações.

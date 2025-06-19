# -*- coding: utf-8 -*-
"""
Created on Tue Jun 17 20:00:56 2025

@author: Maria Juliana Monte | mjmm.monte@gmail.com
"""

#%% Importando bibliotecas

import os
import shutil
import json
from pathlib import Path
from datetime import date
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process

print('Bibliotecas importadas com sucesso!')

#%% Diretórios

root_dir = ('./')
data_lake = ('/data_lake')
raw = ('/01_raw')
preprocessed = ('/02_preprocessed')
analytics = ('/03_analytics')
processed = ('/04_processed')
metadados = ('/05_metadados')
dado = ('/supply_chain_data_mao.csv')
dadop = ('/supply_chain_data_mao.parquet')

#%% Criando a estrutura data lake

path_datalake = (root_dir + data_lake)
path_preprocessed = os.path.join(path_datalake + preprocessed)
path_metadados = os.path.join(path_datalake + metadados)
path_raw = os.path.join(path_datalake + raw)
path_analytics = os.path.join(path_datalake + analytics)
path_processed = os.path.join(path_datalake + processed)

print('Caminhos configurados com sucesso!')

#%% Criando os diretórios

os.makedirs(path_preprocessed, exist_ok = True)
os.makedirs(path_datalake, exist_ok = True)
os.makedirs(path_metadados, exist_ok = True)
os.makedirs(path_raw, exist_ok = True)
os.makedirs(path_analytics, exist_ok = True)
os.makedirs(path_processed, exist_ok = True)

print('Diretórios criados com sucesso!')


#%% Abrindo o arquivo bruto

df = pd.read_csv(path_raw + dado, sep = ',', encoding = 'utf8', decimal = ',')

#%% Salvando na pasta raw

df.to_csv(path_raw + dado, index = False)

#%% Criando metadado para arquivo bruto

metadado_arq_bruto = [
    {
    'Arquivo':'supply_chain_data_mao.csv',
    'Descrição':'Processos de produção e entrega de um produtos',
    'Formato':'csv',
    'Delimitador':',',
    'Codificação':'UTF-8',
    'Número de linhas':370,
    'Decimal':',',
    'Colunas':[
        {'nome':' YEAR_MONTH', 'tipo':'object', 'descrição':'Ano e mês'},
        {'nome':'SKU', 'tipo':'object', 'descrição':'Stock Keeping Unit'},
        {'nome':' DESCRIPTION', 'tipo':'object', 'descrição':'Descrição do produto'},
        {'nome':' LEAD_TIMES', 'tipo':'int64', 'descrição':'Tempo de processo'},
        {'nome':' MANUFACTURING_COSTS', 'tipo':'int64', 'descrição':'Preço de produção'},
        {'nome':' CUSTOMER_NAME', 'tipo':'object', 'descrição':'Nome do comprador'},
        {'nome':' CUSTOMER_CITY', 'tipo':'object', 'descrição':'Cidade do comprador'},
        {'nome':' CUSTOMER_COUNTRY', 'tipo':'object', 'descrição':'País do comprador'},
        {'nome':' VIA_TRANSP', 'tipo':'object', 'descrição':'Modo de transporte'},
        {'nome':' SOLD_QUANTITY', 'tipo':'int64', 'descrição':'Quantidade vendida'},
        {'nome':'SOLD_PRICE', 'tipo':'float64', 'descrição':'Preço de venda'},
        {'nome':' FEIHT_ITEM', 'tipo':'float64', 'descrição':'Item fretado'}
        ],  
    'Fonte':'Desconhecida',
    'Data da coleta':'2025-06-18',
    'Responsáveis':'Alexandra Amaro, Carlos Mota, Edney Farias, Maria Juliana Monte, Sidney Melo'
    }
]

#%% Caminho onde o metadado do arquivo bruto será salvo

arq_met_bruto = path_metadados + '/metadados_supply_chain_data_mao.json'
with open(arq_met_bruto, 'w', encoding = 'utf-8') as f:
    json.dump(metadado_arq_bruto, f, ensure_ascii = False, indent = 4)

### Tratamento do dataset  ###

#%% Checando as colunas

print(df.columns)

#%% Renomeando as colunas

df = df.rename(columns = {' YEAR_MONTH':'DATE', ' DESCRIPTION':'DESCRIPTION', ' LEAD_TIMES':'LEAD_TIMES',
       ' MANUFACTURING_COSTS':'MANUFACTURING_COSTS', ' CUSTOMER_NAME':'CUSTOMER_NAME', ' CUSTOMER_CITY':'CUSTOMER_CITY',
       ' CUSTOMER_COUNTRY':'CUSTOMER_COUNTRY', ' VIA_TRANSP':'TRANSP_MODE', ' SOLD_QUANTITY':'QUANTITY', 'SOLD_PRICE':'PRICE',
       ' FEIHT_ITEM':'FREIGHT'})

#%% Checando por colunas vazias e eliminando

print(df.isnull().sum())

for col in df.columns:
    elim = (
        df[col].isna() | 
        (df[col].astype(str).str.strip() == '') | 
        (df[col] == 0)
    ).all()
    
    if elim:
        print(f"A coluna '{col}' possui somente valores nulos, vazios ou iguais a zero.")
        del df[col]

#%% Conferindo se as células possuem valores com erros de digitação

threshold = 100  #sem tolerância de erros, mostra todas as opções

for col in df.columns:
    if pd.api.types.is_numeric_dtype(df[col]):
        print(f"A coluna '{col}' é numérica")
        continue
    
    unicos = []
    for valor in df[col]:
        igual = [c for c in unicos if fuzz.ratio(str(valor), str(c)) >= threshold]
        if not igual:
            unicos.append(valor)
    
    print(f"Valores únicos na coluna '{col}':")
    print(unicos)
    print('--------------------------')

#%% Aplicando a correção

col_corr = ['CUSTOMER_NAME', 'CUSTOMER_CITY', 'CUSTOMER_COUNTRY', 'TRANSP_MODE']

corrigido = {'CUSTOMER_NAME':{'CUSTORMER_03':'CUSTOMER_03', 'CUSTORMER_09':'CUSTOMER_09', 
                              'CUSTORMER_15':'CUSTOMER_15', 'CUSTORMER_07':'CUSTOMER_07', 
                              'CUSTORMER_08':'CUSTOMER_08', 'CUSTORMER_04':'CUSTOMER_04', 
                              'CUSTORMER_10':'CUSTOMER_10', 'CUSTORMER_11':'CUSTOMER_11', 
                              'CUSTORMER_06':'CUSTOMER_06', 'CUSTORMER_16':'CUSTOMER_16', 
                              'CUSTORMER_01':'CUSTOMER_01', 'CUSTORMER_02':'CUSTOMER_02', 
                              'CUSTORMER_13':'CUSTOMER_13'},
           'CUSTOMER_CITY':{'SOROCABA                      ':'Sorocaba', 
                            'PALO ALTO                     ':'Palo Alto', 
                            'BARUERI                       ':'Barueri', 
                            'SERRA                         ':'Serra', 
                            'Sorocaba\t                     ':'Sorocaba', 
                            'Aguadilla                     ':'Aguadilla', 
                            'Janauba                       ':'Janaúba', 
                            'JANAUBA                       ':'Janaúba', 
                            'Varzea da Palma               ':'Várzea da Palma', 
                            'SERRA DO MEL                  ':'Serra do Mel', 
                            'Sao Jose do Belmonte          ':'São José do Belmonte', 
                            'ABAIARA                       ':'Abaiara'},
           'CUSTOMER_COUNTRY':{' BR':'Brasil', 
                               'USA':'Estados Unidos',
                               ' PR':'Porto Rico'},
           'TRANSP_MODE':{'Rodoviario':'Rodoviário', 
                          'Aereo':'Aéreo'}
}

threshold = 100 

for col in col_corr:
    val_corr = []
    correct = corrigido[col]
    
    for valor in df[col]:
        found = False
        for key in correct.keys():
            if fuzz.ratio(str(valor), str(key)) >= threshold:
                val_corr.append(correct[key])
                found = True
                break
        if not found:
            val_corr.append(valor) 

    df[col] = val_corr
    print(f"A coluna '{col}' foi corrigida.")
    print('--------------------------')

#%% Transformando o tipo da coluna DATE e reordenando

df['DATE'] = pd.to_datetime(df['DATE'], format='%Y-%m')

df = df.sort_values('DATE')

df['DATE'] = df['DATE'].dt.strftime('%Y-%m')

#%% Salvando o novo dataframe em csv

df.to_csv(path_preprocessed + dado, index = False)

#%% Criando metadado para arquivo processado salvo em csv

metadado_arq_proc = [
    {
    'Arquivo':'supply_chain_data_mao.csv',
    'Descrição':'Processos de produção e entrega de um produtos',
    'Formato':'csv',
    'Delimitador':',',
    'Codificação':'UTF-8',
    'Número de linhas':370,
    'Decimal':',',
    'Colunas':[
        {'nome':'DATE', 'tipo':'object', 'descrição':'Ano e mês'},
        {'nome':'SKU', 'tipo':'object', 'descrição':'Stock Keeping Unit'},
        {'nome':'DESCRIPTION', 'tipo':'object', 'descrição':'Descrição do produto'},
        {'nome':'CUSTOMER_NAME', 'tipo':'object', 'descrição':'Nome do comprador'},
        {'nome':'CUSTOMER_CITY', 'tipo':'object', 'descrição':'Cidade do comprador'},
        {'nome':'CUSTOMER_COUNTRY', 'tipo':'object', 'descrição':'País do comprador'},
        {'nome':'TRANSP_MODE', 'tipo':'object', 'descrição':'Modo de transporte'},
        {'nome':'QUANTITY', 'tipo':'int64', 'descrição':'Quantidade vendida'},
        {'nome':'PRICE', 'tipo':'float64', 'descrição':'Preço de venda'}
        ],  
    'Fonte':'Desconhecida',
    'Data da coleta':'2025-06-18',
    'Responsáveis':'Alexandra Amaro, Carlos Mota, Edney Farias, Maria Juliana Monte, Sidney Melo'
    }
]

#%% Caminho onde o metadado do arquivo bruto será salvo

arq_met_proc = path_metadados + '\metadados_supply_chain_data_mao_proc_csv.json'
with open(arq_met_proc, 'w', encoding = 'utf-8') as f:
    json.dump(metadado_arq_proc, f, ensure_ascii = False, indent = 4)
    
#%% Salvando o novo dataframe em parquet

df.to_parquet(path_preprocessed + dadop, engine='fastparquet')

#%% Criando metadado para arquivo processado salvo em parquet

metadado_arq_proc = [
    {
    'Arquivo':'supply_chain_data_mao.csv',
    'Descrição':'Processos de produção e entrega de um produtos',
    'Formato':'parquet',
    'Delimitador':',',
    'Codificação':'UTF-8',
    'Número de linhas':370,
    'Decimal':',',
    'Colunas':[
        {'nome':'DATE', 'tipo':'object', 'descrição':'Ano e mês'},
        {'nome':'SKU', 'tipo':'object', 'descrição':'Stock Keeping Unit'},
        {'nome':'DESCRIPTION', 'tipo':'object', 'descrição':'Descrição do produto'},
        {'nome':'CUSTOMER_NAME', 'tipo':'object', 'descrição':'Nome do comprador'},
        {'nome':'CUSTOMER_CITY', 'tipo':'object', 'descrição':'Cidade do comprador'},
        {'nome':'CUSTOMER_COUNTRY', 'tipo':'object', 'descrição':'País do comprador'},
        {'nome':'TRANSP_MODE', 'tipo':'object', 'descrição':'Modo de transporte'},
        {'nome':'QUANTITY', 'tipo':'int64', 'descrição':'Quantidade vendida'},
        {'nome':'PRICE', 'tipo':'float64', 'descrição':'Preço de venda'}
        ],  
    'Fonte':'Desconhecida',
    'Data da coleta':'2025-06-18',
    'Responsáveis':'Alexandra Amaro, Carlos Mota, Edney Farias, Maria Juliana Monte, Sidney Melo'
    }
]

#%% Caminho onde o metadado do arquivo bruto será salvo

arq_met_proc = path_metadados + '\metadados_supply_chain_data_mao_proc_parquet.json'
with open(arq_met_proc, 'w', encoding = 'utf-8') as f:
    json.dump(metadado_arq_proc, f, ensure_ascii = False, indent = 4)

#%% Definição do catálogo do Data Lake com base nos arquivos mencionados

catalogo_datalake = [
    {'Dataset':'supply_chain_data_mao',
     'Descrição':'Base de dados original com lista de produtos em estoque',
     'Caminho':'data_lake\01_raw\supply_chain_data_mao.csv',
     'Formato':'CSV',
     'Data':str(date.today()),
     'Versão':'1.0'
    },
    {'Dataset':'supply_chain_data_mao',
     'Descrição':'Base de dados processada com lista de produtos em estoque',
     'Caminho':'data_lake\02_preprocessed\supply_chain_data_mao.csv',
     'Formato':'CSV e parquet',
     'Data':str(date.today()),
     'Versão':'1.0'
     }
    ]

#%% Caminho onde o catálogo será salvo

arq_metadados = path_metadados + '\catalogo_datalake.json'
with open(arq_metadados, 'w', encoding = 'utf-8') as f:
    json.dump(catalogo_datalake, f, ensure_ascii = False, indent = 4)
    


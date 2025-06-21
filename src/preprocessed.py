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
from category_encoders import TargetEncoder

print('Bibliotecas importadas com sucesso!')

#%% Diretórios

root_dir = ('../')
data_lake = ('data_lake')
raw = ('/01_raw')
preprocessed = ('/02_preprocessed')
analytics = ('/03_analytics')
processed = ('/04_processed')
metadados = ('/05_metadados')
images = ('/images')
dado = ('/supply_chain_data_mao.csv')
dadop = ('/supply_chain_data_mao.parquet')
catalogo = ('/catalogo_datalake.json')

#%% Criando a estrutura data lake

path_datalake = (root_dir + data_lake)
path_preprocessed = os.path.join(path_datalake + preprocessed)
path_metadados = os.path.join(path_datalake + metadados)
path_raw = os.path.join(path_datalake + raw)
path_analytics = os.path.join(path_datalake + analytics)
path_processed = os.path.join(path_datalake + processed)
path_analytics_images = os.path.join(path_analytics + images)
path_metadados_catalogo = os.path.join(path_metadados + catalogo)

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

df = pd.read_csv(root_dir + dado, sep = ',', encoding = 'utf8', decimal = ',')

print(df.columns)

print('Arquivo lido com sucesso!')

#%% Salvando na pasta raw

df.to_csv(path_raw + dado, index = False)

#%% Informações do arquivo bruto

df.info()

#%% Criando metadado para arquivo bruto

metadado_arq_bruto = [
    {
'Arquivo':'supply_chain_data_mao.csv',
    'Descrição':'Processos de produção e entrega de produtos',
    'Formato':'csv',
    'Delimitador':',',
    'Codificação':'UTF-8',
    'Número de linhas':df.shape[0],
    'Decimal':',',
    'Colunas':[
        {'nome':' YEAR_MONTH', 'tipo':'object', 'descrição':'Data de venda do produto'},
        {'nome':'SKU', 'tipo':'object', 'descrição':'Stocking Keeping Unit: Identificador único do produto'},
        {'nome':' DESCRIPTION', 'tipo':'object', 'descrição':'Descrição do produto'},
        {'nome':' LEAD_TIMES', 'tipo':'int64', 'descrição':'Tempo de reposição'},
        {'nome':' MANUFACTURING_COSTS', 'tipo':'float64', 'descrição':'Preço de produção'},
        {'nome':' CUSTOMER_NAME', 'tipo':'object', 'descrição':'Nome do comprador'},
        {'nome':' CUSTOMER_CITY', 'tipo':'object', 'descrição':'Cidade do comprador'},
        {'nome':' CUSTOMER_COUNTRY', 'tipo':'object', 'descrição':'País do comprador'},
        {'nome':' VIA_TRANSP', 'tipo':'object', 'descrição':'Modo de transporte'},
        {'nome':' SOLD_QTY', 'tipo':'float64', 'descrição':'Quantidade vendida'},
        {'nome':' PRICE', 'tipo':'float64', 'descrição':'Preço de venda'},
        {'nome':' REVENUE_GENERATED', 'tipo':'float64', 'descrição':'Receida gerada'},
        {'nome':' FEIHT_ITEM', 'tipo':'float64', 'descrição':'Item fretado'},
        {'nome':' STOK_QTY', 'tipo':'float64', 'descrição':'Quantidade em estoque'},
        {'nome':' LEAD_TIMES_REPOSITION', 'tipo':'int64', 'descrição':'Reposição'}
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

print(df.columns)

df = df.rename(columns = {' YEAR_MONTH':'DATE', ' DESCRIPTION':'DESCRIPTION', ' LEAD_TIMES':'LEAD_TIMES',
       ' MANUFACTURING_COSTS':'MANUFACTURING_COSTS', ' CUSTOMER_NAME':'CUSTOMER_NAME', ' CUSTOMER_CITY':'CUSTOMER_CITY',
       ' CUSTOMER_COUNTRY':'CUSTOMER_COUNTRY', ' VIA_TRANSP':'TRANSP_MODE', ' SOLD_QTY':'SOLD_QTY', ' PRICE':'PRICE', 
       ' REVENUE_GENERATED':'REVENUE', ' FEIHT_ITEM':'FREIGHT', ' STOK_QTY':'STOCK_QTY', ' LEAD_TIMES_REPOSITION':'REPOSITION'})
print(df.columns)

#%% Checando por colunas vazias e eliminando

for col in df.columns:
    elim = (
        df[col].isna() | 
        (df[col].astype(str).str.strip() == '') | 
        (df[col] == 0)
    ).all()
    
    if elim:
        print(f"A coluna '{col}' possui somente valores vazios ou iguais a zero.")
        del df[col]
    else:
        print(f"A coluna '{col}' não possui somente valores vazios ou iguais a zero.")

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
                              'CUSTORMER_14':'CUSTOMER_14', 'CUSTORMER_07':'CUSTOMER_07', 
                              'CUSTORMER_08':'CUSTOMER_08', 'CUSTORMER_04':'CUSTOMER_04', 
                              'CUSTORMER_10':'CUSTOMER_10', 'CUSTORMER_11':'CUSTOMER_11', 
                              'CUSTORMER_06':'CUSTOMER_06', 'CUSTORMER_12':'CUSTOMER_12', 
                              'CUSTORMER_01':'CUSTOMER_01', 'CUSTORMER_02':'CUSTOMER_02', 
                              'CUSTORMER_13':'CUSTOMER_13', 'CUSTORMER_05':'CUSTOMER_05'},
             'CUSTOMER_CITY':{'JUNDIAI                       ':'Jundiaí', 
                              'Salto                         ':'Salto', 
                              'PALO ALTO                     ':'Palo Alto', 
                              'SAO PAULO                     ':'São Paulo', 
                              'Sorocaba\t                     ':'Sorocaba', 
                              'GENEVA                        ':'Genebra', 
                              'BARUERI                       ':'Barueri', 
                              'Guarulhos                     ':'Guarulhos', 
                              'Santana de Parnaiba\t          ':'Santana de Parnaíba', 
                              'ITAJUBA                       ':'Itajubá', 
                              'Hortolandia \t                 ':'Hortolândia', 
                              'SERRA                         ':'Serra', 
                              'Jaguariuna                    ':'Jaguariúna', 
                              'BOITUVA                       ':'Boituva'},
           'CUSTOMER_COUNTRY':{' BR':'Brasil', 
                               'USA':'Estados Unidos',
                               ' CH':'Suíça'},
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

#%% Convertendo valores numéricos para o tipo correto

df['MANUFACTURING_COSTS'] = pd.to_numeric(df['MANUFACTURING_COSTS'], errors='coerce')
df['SOLD_QTY'] = pd.to_numeric(df['SOLD_QTY'], errors='coerce')
df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
df['REVENUE'] = pd.to_numeric(df['REVENUE'], errors='coerce')
df['FREIGHT'] = pd.to_numeric(df['FREIGHT'], errors='coerce')
df['STOCK_QTY'] = pd.to_numeric(df['STOCK_QTY'], errors='coerce')

df.info()

#%% Estatísticas das colunas numéricas

for col in df.columns:
    if pd.api.types.is_numeric_dtype(df[col]):
        print(f"'{col}'")
        print(df[col].describe())
        print('--------------------------')
        continue

#%% Deletando colunas que não serão usadas

del df['DESCRIPTION']
print('Coluna deletada!')

#%% Transformando o tipo da coluna DATE e reordenando

df['DATE'] = pd.to_datetime(df['DATE'], format='%Y/%m')
df = df.sort_values('DATE')


# %% Fazendo feature engineering com a coluna DATE
df['MONTH'] = df['DATE'].dt.month
df['YEAR'] = df['DATE'].dt.year

df.drop(columns=['DATE'], inplace=True)

# %% Separando as colunas numéricas e categóricas
num_cols = df.select_dtypes(include=np.number).columns


# %% Aplicando Target Encoding nas colunas categóricas

cat_cols = df.select_dtypes(include='object').columns
for col in cat_cols:
    df[f"{col}_LABEL"] = df[col]
    df[f"{col}_LABEL"] = df[f"{col}_LABEL"].astype('category')


encoder = TargetEncoder(cols=cat_cols, handle_unknown='value', handle_missing='value')
df[cat_cols] = encoder.fit_transform(df[cat_cols], df['STOCK_QTY'])

#%% Verificando o resultado do Target Encoding

df.info()



#%% Salvando o novo dataframe em csv

df.to_csv(path_preprocessed + dado, index = False)

#%% Criando metadado para arquivo processado salvo em csv

metadado_arq_proc = [
    {
    'Arquivo':'supply_chain_data_mao.csv',
    'Descrição':'Processos de produção e entrega de produtos',
    'Formato':'csv',
    'Delimitador':',',
    'Codificação':'UTF-8',
    'Número de linhas':429,
    'Decimal':',',
    'Colunas':[
        {'nome':'YEAR', 'tipo':'int32', 'descrição':'Ano de venda do produto'},
        {'nome':'MONTH', 'tipo':'int32', 'descrição':'Mês de venda do produto'},
        {'nome':'SKU', 'tipo':'float64', 'descrição':'Stocking Keeping Unit: Identificador único do produto'},
        {'nome':'LEAD_TIMES', 'tipo':'int64', 'descrição':'Tempo de reposição'},
        {'nome':'MANUFACTURING_COSTS', 'tipo':'float64', 'descrição':'Preço de produção'},
        {'nome':'CUSTOMER_NAME', 'tipo':'float64', 'descrição':'Nome do comprador'},
        {'nome':'CUSTOMER_CITY', 'tipo':'float64', 'descrição':'Cidade do comprador'},
        {'nome':'CUSTOMER_COUNTRY', 'tipo':'float64', 'descrição':'País do comprador'},
        {'nome':'TRANSP_MODE', 'tipo':'float64', 'descrição':'Modo de transporte'},
        {'nome':'SOLD_QTY', 'tipo':'float64', 'descrição':'Quantidade vendida'},
        {'nome':'PRICE', 'tipo':'float64', 'descrição':'Preço de venda'},
        {'nome':'REVENUE', 'tipo':'float64', 'descrição':'Receita gerada'},
        {'nome':'FREIGHT', 'tipo':'float64', 'descrição':'Item fretado'},
        {'nome':'STOCK_QTY', 'tipo':'float64', 'descrição':'Quantidade em estoque'},
        {'nome':'REPOSITION', 'tipo':'int64', 'descrição':'Reposição'},
        {'nome':'SKU', 'tipo':'category', 'descrição':'Código do SKU no formato original (string categórica)'},
        {'nome':'CUSTOMER_NAME', 'tipo':'category', 'descrição':'Nome do comprador no formato original (string categórica)'},
        {'nome':'CUSTOMER_CITY_LABEL', 'tipo':'int64', 'descrição':'Cidade do comprador no formato original (string categórica)'},
        {'nome':'CUSTOMER_COUNTRY', 'tipo':'int64', 'descrição':'País do comprador no formato original (string categórica)'},
        {'nome':'TRANSP_MODE', 'tipo':'int64', 'descrição':'Modo de transporte no formato original (string categórica)'}
        ],  
    'Fonte':'Desconhecida',
    'Data do preprocessamento':'2025-06-19',
    'Responsáveis':'Alexandra Amaro, Carlos Mota, Edney Farias, Maria Juliana Monte, Sidney Melo'
    }
]

#%% Caminho onde o metadado do arquivo bruto será salvo

arq_met_proc = path_metadados + '/metadados_supply_chain_data_mao_prepro_csv.json'
with open(arq_met_proc, 'w', encoding = 'utf-8') as f:
    json.dump(metadado_arq_proc, f, ensure_ascii = False, indent = 4)
    
#%% Salvando o novo dataframe em parquet

df.to_parquet(path_preprocessed + dadop, engine='fastparquet')

#%% Criando metadado para arquivo processado salvo em parquet

metadado_arq_proc = [
    {
    'Arquivo':'supply_chain_data_mao.parquet',
    'Descrição':'Processos de produção e entrega de produtos',
    'Formato':'parquet',
    'Delimitador':',',
    'Codificação':'UTF-8',
    'Número de linhas':429,
    'Decimal':',',
    'Colunas':[
        {'nome':'YEAR', 'tipo':'int32', 'descrição':'Ano de venda do produto'},
        {'nome':'MONTH', 'tipo':'int32', 'descrição':'Mês de venda do produto'},
        {'nome':'SKU', 'tipo':'object', 'descrição':'Stocking Keeping Unit: Identificador único do produto'},
        {'nome':'LEAD_TIMES', 'tipo':'int64', 'descrição':'Tempo de reposição'},
        {'nome':'MANUFACTURING_COSTS', 'tipo':'float64', 'descrição':'Preço de produção'},
        {'nome':'CUSTOMER_NAME', 'tipo':'object', 'descrição':'Nome do comprador'},
        {'nome':'CUSTOMER_CITY', 'tipo':'object', 'descrição':'Cidade do comprador'},
        {'nome':'CUSTOMER_COUNTRY', 'tipo':'object', 'descrição':'País do comprador'},
        {'nome':'TRANSP_MODE', 'tipo':'object', 'descrição':'Modo de transporte'},
        {'nome':'SOLD_QTY', 'tipo':'float64', 'descrição':'Quantidade vendida'},
        {'nome':'PRICE', 'tipo':'float64', 'descrição':'Preço de venda'},
        {'nome':'REVENUE', 'tipo':'float64', 'descrição':'Receita gerada'},
        {'nome':'FREIGHT', 'tipo':'float64', 'descrição':'Item fretado'},
        {'nome':'STOCK_QTY', 'tipo':'float64', 'descrição':'Quantidade em estoque'},
        {'nome':'REPOSITION', 'tipo':'int64', 'descrição':'Reposição'},
        {'nome':'SKU', 'tipo':'category', 'descrição':'Código do SKU no formato original (string categórica)'},
        {'nome':'CUSTOMER_NAME', 'tipo':'category', 'descrição':'Nome do comprador no formato original (string categórica)'},
        {'nome':'CUSTOMER_CITY_LABEL', 'tipo':'int64', 'descrição':'Cidade do comprador no formato original (string categórica)'},
        {'nome':'CUSTOMER_COUNTRY', 'tipo':'int64', 'descrição':'País do comprador no formato original (string categórica)'},
        {'nome':'TRANSP_MODE', 'tipo':'int64', 'descrição':'Modo de transporte no formato original (string categórica)'}
        ],  
    'Fonte':'Desconhecida',
    'Data do preprocessamento':'2025-06-19',
    'Responsáveis':'Alexandra Amaro, Carlos Mota, Edney Farias, Maria Juliana Monte, Sidney Melo'
    }
]

#%% Caminho onde o metadado do arquivo bruto será salvo

arq_met_proc = path_metadados + '/metadados_supply_chain_data_mao_prepro_parquet.json'
with open(arq_met_proc, 'w', encoding = 'utf-8') as f:
    json.dump(metadado_arq_proc, f, ensure_ascii = False, indent = 4)

#%% Definição do catálogo do Data Lake com base nos arquivos mencionados

catalogo_datalake = [
    {'Dataset':'supply_chain_data_mao',
     'Descrição':'Base de dados original com lista de produtos em estoque',
     'Caminho':'data_lake\raw\supply_chain_data_mao.csv',
     'Formato':'CSV',
     'Data':str(date.today()),
     'Versão':'1.0'
    },
    {'Dataset':'supply_chain_data_mao',
     'Descrição':'Base de dados processada com lista de produtos em estoque',
     'Caminho':'data_lake\processed\supply_chain_data_mao.csv',
     'Formato':'CSV e parquet',
     'Data':str(date.today()),
     'Versão':'1.0'
     }
    ]

#%% Caminho onde o catálogo será salvo

arq_metadados = path_metadados + '/catalogo_datalake.json'
with open(arq_metadados, 'w', encoding = 'utf-8') as f:
    json.dump(catalogo_datalake, f, ensure_ascii = False, indent = 4)



# %%

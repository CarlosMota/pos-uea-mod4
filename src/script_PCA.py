# -*- coding: utf-8 -*-
"""Untitled23.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1vxaHMQWAC1nsQQFioy0xDdBfaTKCFX3b
"""
#%% Imports principais 

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from preprocessed import path_preprocessed,dadop,path_analytics_images,path_metadados_catalogo,path_metadados,path_processed
import seaborn as sns

from utils import add_image_to_catalog_list,add_dataset_to_catalog_list

# Configurar visualização
sns.set(style="whitegrid")
plt.rcParams['figure.figsize'] = (10, 6)

#%% Ler dados

df = pd.read_parquet(path_preprocessed+dadop)

print(df.columns)

num_cols = ['LEAD_TIMES','MANUFACTURING_COSTS','SOLD_QTY','PRICE','FREIGHT','STOCK_QTY','REPOSITION']


#%% Preencher valores ausentes e normalizar
df[num_cols] = df[num_cols].fillna(df[num_cols].mean())

scaler = StandardScaler()
df_scaled = df.copy()
df_scaled[num_cols] = scaler.fit_transform(df[num_cols])

#%% Criando PCA
# PCA
pca = PCA(n_components=0.95)
df_pca = pca.fit_transform(df_scaled[num_cols])

# Converter PCA para DataFrame
pca_cols = [f'PC{i+1}' for i in range(df_pca.shape[1])]
df_pca_df = pd.DataFrame(df_pca, columns=pca_cols)


#%% Gráficos

# Histograma de uma variável numérica normalizada
sns.histplot(df_scaled['SOLD_QTY'], kde=True, bins=30)
plt.title("Distribuição da variável SOLD_QTY (normalizada)")
plt.xlabel("SOLD_QTY (scaled)")
plt.ylabel("Frequência")

hist_sold_image = path_analytics_images + "/hist_sold_qty.png"
plt.savefig(hist_sold_image, dpi=300, bbox_inches='tight')
plt.show()
add_image_to_catalog_list(hist_sold_image,path_metadados,path_metadados_catalogo)

# # # Matriz de correlação
corr = df[num_cols].corr()
sns.heatmap(corr, annot=True, fmt=".2f", cmap='coolwarm')
plt.title("Matriz de Correlação - Variáveis Numéricas")
corr_image = path_analytics_images + "/correlation.png"
plt.savefig(corr_image, dpi=300, bbox_inches='tight')
plt.show()
add_image_to_catalog_list(corr_image,path_metadados,path_metadados_catalogo)

# # # Variância explicada pelo PCA
plt.plot(np.cumsum(pca.explained_variance_ratio_)*100, marker='o')
plt.title('Variância Acumulada - PCA')
plt.xlabel('Número de Componentes')
plt.ylabel('Variância Explicada (%)')
plt.grid()
plt.tight_layout()
var_image = path_analytics_images + "/variance_PCA.png"
plt.savefig(var_image, dpi=300, bbox_inches='tight')
plt.show()
add_image_to_catalog_list(var_image,path_metadados,path_metadados_catalogo)

# # # Visualização no espaço PCA (PC1 x PC2)
if 'PC1' in df_pca_df.columns and 'PC2' in df_pca_df.columns:
    sns.scatterplot(x='PC1', y='PC2', data=df_pca_df, alpha=0.7)
    plt.title('Pontos no Espaço PCA (PC1 vs PC2)')
    plt.xlabel('Componente Principal 1')
    plt.ylabel('Componente Principal 2')
    plt.grid(True)
    plt.tight_layout()
    compare_pca = path_analytics_images + "/compare_pca.png"
    plt.savefig(compare_pca, dpi=300, bbox_inches='tight')
    plt.show()
    add_image_to_catalog_list(compare_pca,path_metadados,path_metadados_catalogo)

# # # Matriz de cargas (influência das variáveis nos componentes)
loading_matrix = pd.DataFrame(
    pca.components_.T,
    columns=pca_cols,
    index=num_cols
)

# # print("\n🔎 Cargas das variáveis nos Componentes Principais (loading matrix):")
print(loading_matrix)

# #SALVAR SAÍDAS

# # CSV com dados normalizados
normalized_csv_path = path_processed + "/supply_chain_data_normalized.parquet"
df_scaled.to_parquet(normalized_csv_path, index=False)
add_dataset_to_catalog_list(normalized_csv_path, path_metadados, path_metadados_catalogo)

# # CSV com dados reduzidos via PCA
pca_csv_path = path_processed + "/supply_chain_data_pca.parquet"
df_pca_df.to_parquet(pca_csv_path, index=False)
add_dataset_to_catalog_list(pca_csv_path, path_metadados, path_metadados_catalogo)



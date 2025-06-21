# %% Import necessary libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.discriminant_analysis import StandardScaler
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LinearRegression, LogisticRegression
from preprocessed import path_preprocessed,dadop,path_analytics_images,path_metadados_catalogo,path_metadados,path_processed
from sklearn.feature_selection import SelectKBest, f_regression

from utils import add_image_to_catalog_list

print("Importing libraries...")

# %% read file to dataframe
df = pd.read_parquet(path_preprocessed+dadop)

df.columns



# %% Separando as variáveis independentes e dependentes
cols = [col for col in df.columns if col.endswith('_LABEL')]
cols.append('STOCK_QTY')
X = df.drop(columns=cols)
y = df['STOCK_QTY']

print(X.head())



# %% Dividindo o dataset em treino e teste
x_train,x_test,y_train,Y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# %% Normalizando os dados


scaler = StandardScaler()
# Deve-se usar somente os dados de treinamento para ajustar o scaler
# Assim evita o vazamento de dados do teste
X_train_scaled = scaler.fit_transform(x_train)
X_test_scaled = scaler.transform(x_test)



# %% Feature Selection using SelectKBest with f_regression

n_features = X_train_scaled.shape[1]
k_values = range(1, n_features + 1)
r2_scores = []

for k in k_values:

    selector = SelectKBest(score_func=f_regression, k=k)
    x_train_k = selector.fit_transform(x_train, y_train)
    x_test_k = selector.transform(x_test)

    model = LinearRegression()
    model.fit(x_train_k, y_train)
    r2 = model.score(x_test_k, Y_test)
    r2_scores.append(f"{(round(r2)*100)}%")



plt.figure(figsize=(10, 6))
plt.plot(k_values,r2_scores, marker='o')
plt.xlabel('Número de Features Selecionadas (k)')
plt.ylabel('R²')
plt.title('Gráfico para escolha do melhor k')
k_image = path_analytics_images + "/features_quantities.png"
plt.savefig(k_image, dpi=300, bbox_inches='tight')
plt.show()
add_image_to_catalog_list(k_image,path_metadados,path_metadados_catalogo)


# %%
# ----------------------------------
# Gráfico dos Scores Individuais
# ----------------------------------
# Ajusta SelectKBest com k='all' para pegar scores de todas as features
selector = SelectKBest(score_func=f_regression, k='all')
selector.fit(x_train, y_train)

feature_scores = selector.scores_
feature_names = x_train.columns

scores_series = pd.Series(feature_scores, index=feature_names)

scores_sorted = scores_series.sort_values()

plt.figure(figsize=(10, 6))
scores_sorted.plot(kind='barh')
plt.xlabel("Score f_regression")
plt.title("Scores das Features - SelectKBest (Regressão) - Ordenados")
score_image = path_analytics_images + "/score_features.png"
plt.savefig(score_image, dpi=300, bbox_inches='tight')
plt.show()
add_image_to_catalog_list(score_image,path_metadados,path_metadados_catalogo)



# %%

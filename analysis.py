import pandas as pd 

#Leitura do arquivo principal em um DataFrame
df = pd.read_csv('adult.csv', encoding="utf-8")

#Inspeção inicial com dimensão, tipos de dados e primeiras linhas
print(df.shape)
print(df.dtypes)
print(df.head)

#Identificar quais colunas são numéricas e quais são categóricas
df_numerico = df.select_dtypes(include=['number'])
df_categorico = df.select_dtypes(include=['str'])

print("Numéricas:\n", df_numerico)
print("\nCategóricas:\n", df_categorico)

#Substituir ? por valores nulos reais
df = df.replace('?', pd.NA)
print(df)

#Verificar a quantidade de nulos por coluna
print(df.isnull().sum())

#Estratégia de tratamento para valores ausentes

#Padronização de textos removendo espaços extras e uniformizando a escrita das categorias
df_texto = df['workclass', 'education', 'marital-status', 'occupation', 'relationship', 'race', 'gender', 'native-country', 'income']
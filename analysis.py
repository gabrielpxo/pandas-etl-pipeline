import pandas as pd
import numpy as np
import json

# Extração

def load_data(filepath):
    # Carregar o dataset
    df = pd.read_csv(filepath, encoding="utf-8")
    return df

def inspect_data(df):
    # Exibir informações iniciais do dataframe
    print("Shape:", df.shape)
    print("Tipos de dados:\n", df.dtypes)
    print("Primeiras linhas:\n", df.head())
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    print("Colunas numéricas:", numeric_cols)
    print("Colunas categóricas:", categorical_cols)
    return numeric_cols, categorical_cols

# Limpeza

def clean_data(df):
    # Substituir '?' por NaN
    df = df.replace('?', pd.NA)

    # Garantir tipo de colunas numéricas
    numeric_cols = ['age', 'fnlwgt', 'educational-num', 'capital-gain', 'capital-loss', 'hours-per-week']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Padronizar textos: remover espaços extras e converter para minúsculas
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    for col in categorical_cols:
        df[col] = df[col].str.strip().str.lower()

    # Tratar valores ausentes: categóricas com moda, numéricas com mediana
    for col in categorical_cols:
        mode_val = df[col].mode()[0] if not df[col].mode().empty else 'desconhecido'
        df[col] = df[col].fillna(mode_val)
    for col in numeric_cols:
        median_val = df[col].median()
        df[col] = df[col].fillna(median_val)

    # Renomear colunas para padrão consistente (minúsculas e underscores)
    df.columns = [col.replace('-', '_').lower() for col in df.columns]
    return df

# Transformações

def transform_data(df):
    # Criar novas variáveis derivadas
    # 1. Faixa etária (não usa apply, mas é uma das três novas)
    df['age_group'] = pd.cut(df['age'], bins=[0, 25, 35, 45, 55, 100],
                             labels=['<25', '25-34', '35-44', '45-54', '55+'])

    # 2. Carga horária semanal (não usa apply)
    df['hours_group'] = pd.cut(df['hours_per_week'], bins=[0, 20, 40, 60, 100],
                               labels=['<20', '20-39', '40-59', '60+'])

    # 3. Indicador binário de renda (usa apply)
    df['income_binary'] = df['income'].apply(lambda x: 1 if x == '>50k' else 0)

    # 4. razão capital-gain / (capital-loss+1) (usa apply)
    df['capital_ratio'] = df.apply(
        lambda row: row['capital_gain'] / (row['capital_loss'] + 1) if row['capital_loss'] > 0 else row['capital_gain'],
        axis=1
    )
    return df

# 4. Integração com merge (tabela auxiliar)

def create_aux_table():
    # Cria tabela auxiliar com categorias de escolaridade
    edu_mapping = {
        1: 'preschool', 2: '1st-4th', 3: '5th-6th', 4: '7th-8th', 5: '9th',
        6: '10th', 7: '11th', 8: '12th', 9: 'hs-grad', 10: 'some-college',
        11: 'assoc-voc', 12: 'assoc-acdm', 13: 'bachelors', 14: 'masters',
        15: 'prof-school', 16: 'doctorate'
    }
    aux_df = pd.DataFrame(list(edu_mapping.items()),
                          columns=['educational_num', 'education_category'])
    return aux_df

def merge_aux(df, aux_df):
    # Junta a tabela principal com a auxiliar pela escolaridade numérica
    df = df.merge(aux_df, on='educational_num', how='left')
    return df

# 5. Análises com groupby

def groupby_analyses(df):
    # Executa pelo menos 4 análises agregadas, uma delas com agg()
    analyses = {}

    # 1. Média de idade por renda e classe de trabalho
    age_by_income_workclass = df.groupby(['income', 'workclass'])['age'].mean().reset_index()
    analyses['age_by_income_workclass'] = age_by_income_workclass

    # 2. Contagem por categoria educacional e renda
    count_by_edu_income = df.groupby(['education_category', 'income']).size().reset_index(name='count')
    analyses['count_by_edu_income'] = count_by_edu_income

    # 3. Média e contagem de horas por estado civil e renda (usa agg())
    hours_by_marital_income = df.groupby(['marital_status', 'income'])['hours_per_week'].agg(['mean', 'count']).reset_index()
    analyses['hours_by_marital_income'] = hours_by_marital_income

    # 4. Mediana de ganho de capital por ocupação e renda
    capital_by_occ_income = df.groupby(['occupation', 'income'])['capital_gain'].median().reset_index()
    analyses['capital_by_occ_income'] = capital_by_occ_income

    return analyses

# 6. Tabelas dinâmicas

def pivot_analysis(df):
    # Cria duas tabelas dinâmicas com pivot_table
    # Pivot 1: renda nas linhas, categoria educacional nas colunas, contagem de indivíduos
    pivot1 = pd.pivot_table(df, values='age', index='income', columns='education_category',
                            aggfunc='count', fill_value=0)

    # Pivot 2: classe de trabalho nas linhas, renda nas colunas, média de horas semanais
    pivot2 = pd.pivot_table(df, values='hours_per_week', index='workclass', columns='income',
                            aggfunc='mean')

    return pivot1, pivot2

# 7. Exportação

def export_data(df, analyses, pivots, output_prefix):
    # Exporta os resultados para CSV e JSON
    # Base final tratada
    df.to_csv(f"{output_prefix}_cleaned.csv", index=False)

    # Resumos de analise em CSV
    analyses['age_by_income_workclass'].to_csv(f"{output_prefix}_age_by_income_workclass.csv", index=False)
    analyses['count_by_edu_income'].to_csv(f"{output_prefix}_count_by_edu_income.csv", index=False)
    analyses['hours_by_marital_income'].to_csv(f"{output_prefix}_hours_by_marital_income.csv", index=False)
    analyses['capital_by_occ_income'].to_csv(f"{output_prefix}_capital_by_occ_income.csv", index=False)

    # Exportar as tabelas dinâmicas em JSON
    pivot1, pivot2 = pivots
    pivot1.to_json(f"{output_prefix}_pivot1.json")
    pivot2.to_json(f"{output_prefix}_pivot2.json")

    # Exportar um resumo em JSON (primeira análise)
    with open(f"{output_prefix}_summary.json", 'w') as f:
        json.dump(analyses['age_by_income_workclass'].to_dict(orient='records'), f)

# 8. Função principal

def main():
    # Extração
    print("=== EXTRAÇÃO ===")
    df = load_data('adult.csv')
    numeric_cols, categorical_cols = inspect_data(df)

    # Transformação
    print("\n=== LIMPEZA ===")
    df = clean_data(df)
    print("Valores nulos após limpeza:\n", df.isnull().sum())
    print("Tipos após limpeza:\n", df.dtypes)

    print("\n=== TRANSFORMAÇÕES ===")
    df = transform_data(df)
    print("Novas colunas adicionadas:")
    new_cols = [c for c in df.columns if c not in ['age', 'workclass', 'fnlwgt', 'educational_num',
                                                    'education', 'marital_status', 'occupation',
                                                    'relationship', 'race', 'gender', 'capital_gain',
                                                    'capital_loss', 'hours_per_week', 'native_country',
                                                    'income']]
    print(new_cols)

    print("\n=== INTEGRAÇÃO COM MERGE ===")
    aux_df = create_aux_table()
    df = merge_aux(df, aux_df)
    print("Exemplo do merge:\n", df[['educational_num', 'education_category']].head())

    # Análises
    print("\n=== ANÁLISES COM GROUPBY ===")
    analyses = groupby_analyses(df)
    for name, result in analyses.items():
        print(f"{name}:\n", result.head())

    print("\n=== TABELAS DINÂMICAS ===")
    pivot1, pivot2 = pivot_analysis(df)
    print("Pivot 1 (contagem por renda e categoria educacional):\n", pivot1)
    print("Pivot 2 (média de horas por classe de trabalho e renda):\n", pivot2)

    # Carga
    print("\n=== EXPORTAÇÃO ===")
    export_data(df, analyses, (pivot1, pivot2), 'adult_income_output')
    print("Arquivos gerados: adult_income_output_cleaned.csv, summaries CSVs, pivot JSONs, summary.json")

    print("\nETL concluído com sucesso!")

if __name__ == "__main__":
    main()
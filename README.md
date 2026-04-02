# ETL Pipeline with Pandas - Adult Income Dataset

This project implements a complete ETL (Extract, Transform, Load) pipeline using the Pandas library on the **Adult Income** dataset (also known as the "Census Income" dataset). The goal is to clean, transform, enrich, and analyze the data, generating reports and files ready for use in other tools.

## 📚 Project Description

The Adult Income dataset contains demographic, educational, occupational, and income attributes of 48,842 individuals. The original dataset has inconsistencies such as missing values represented by `?`, extra spaces in categorical fields, and case variations.

The developed pipeline:

- Extracts data from a CSV file
- Cleans and standardizes the data (handling nulls, stripping spaces, converting to lowercase)
- Creates new derived variables (age group, hours group, binary income indicator, and capital ratio)
- Enriches the base with an auxiliary education category table via `merge`
- Performs aggregated analyses with `groupby` and `agg`
- Generates pivot tables with `pivot_table`
- Exports results in CSV and JSON formats

## 📁 Repository Structure

.
├── adult.csv                     # Original dataset (must be provided by the user)
├── analysis.py                   # Main script with all ETL pipeline functions
├── README.md                     # This file
└── outputs/                      # Folder where output files will be saved (created automatically)
    ├── adult_income_output_cleaned.csv
    ├── adult_income_output_age_by_income_workclass.csv
    ├── adult_income_output_count_by_edu_income.csv
    ├── adult_income_output_hours_by_marital_income.csv
    ├── adult_income_output_capital_by_occ_income.csv
    ├── adult_income_output_pivot1.json
    ├── adult_income_output_pivot2.json
    └── adult_income_output_summary.json

## 🔧 Requirements

- Python 3.7+
- Libraries:
  - pandas
  - numpy

Install dependencies with:

```bash
pip install pandas numpy
```

## 🚀 How to Run

1. Place the `adult.csv` file in the same directory as the `analysis.py` script.
2. Run the script:

```bash
python analysis.py
```

3. After execution, all output files will be generated in the current directory (you can modify the `export_data` function to save them in a subfolder).

## 📊 ETL Pipeline Steps

### 1. Extraction

The `load_data()` function loads the CSV file using `pd.read_csv()`. The initial inspection (`inspect_data()`) shows:

- Dimensions: 48,842 rows and 15 columns
- Data types: 6 numeric columns (`int64`) and the rest as `object` (categorical)
- Presence of `?` values in categorical columns

### 2. Cleaning and Preparation

The `clean_data()` function performs:

- Replacement of `?` with `pd.NA`
- Conversion of numeric columns to numeric type (coercing errors)
- Removal of extra spaces and conversion to lowercase for all categorical columns
- Imputation of null values:
  - **Categorical columns**: mode (most frequent value)
  - **Numeric columns**: median
- Renaming columns: hyphens replaced by underscores and all lowercase (e.g., `educational-num` → `educational_num`)

After this step, there are no null values in the DataFrame.

### 3. Transformations

The `transform_data()` function adds four new variables:

| Variable          | Description                                                           | Created with    |
|-------------------|-----------------------------------------------------------------------|-----------------|
| `age_group`       | Age range: <25, 25-34, 35-44, 45-54, 55+                              | `pd.cut`        |
| `hours_group`     | Weekly hours range: <20, 20-39, 40-59, 60+                            | `pd.cut`        |
| `income_binary`   | Binary income indicator: 1 if >50K, 0 otherwise                       | `apply()`       |
| `capital_ratio`   | Ratio `capital_gain / (capital_loss + 1)` (avoids division by zero)   | `apply()` on rows |

### 4. Integration with Merge

An auxiliary table (`create_aux_table()`) maps the numeric education level (`educational_num`) to a descriptive text category (e.g., 13 → "bachelors", 16 → "doctorate"). The `merge_aux()` function performs a `left join` between the main DataFrame and this auxiliary table, enriching the base with the `education_category` column.

### 5. GroupBy Analyses

The `groupby_analyses()` function produces four aggregations, one of which uses the `agg()` method for multiple metrics:

1. **Mean age by income and work class**  
   `groupby(['income', 'workclass'])['age'].mean()`

2. **Count of individuals by education category and income**  
   `groupby(['education_category', 'income']).size()`

3. **Mean and count of weekly hours by marital status and income** (using `agg`)  
   `groupby(['marital_status', 'income'])['hours_per_week'].agg(['mean', 'count'])`

4. **Median capital gain by occupation and income**  
   `groupby(['occupation', 'income'])['capital_gain'].median()`

### 6. Pivot Tables

The `pivot_analysis()` function generates two pivot tables:

- **Pivot 1**: Count of individuals by income (`index`) and education category (`columns`).
- **Pivot 2**: Mean weekly hours by work class (`index`) and income (`columns`).

### 7. Export (Load)

The `export_data()` function saves the following files:

| File                                                  | Format | Content                                        |
|-------------------------------------------------------|--------|------------------------------------------------|
| `adult_income_output_cleaned.csv`                    | CSV    | Final cleaned and transformed DataFrame       |
| `adult_income_output_age_by_income_workclass.csv`    | CSV    | Mean age by income and work class             |
| `adult_income_output_count_by_edu_income.csv`        | CSV    | Count by education and income                 |
| `adult_income_output_hours_by_marital_income.csv`    | CSV    | Mean and count of hours by marital status     |
| `adult_income_output_capital_by_occ_income.csv`      | CSV    | Median capital gain by occupation             |
| `adult_income_output_pivot1.json`                    | JSON   | Pivot table 1 (count)                         |
| `adult_income_output_pivot2.json`                    | JSON   | Pivot table 2 (mean hours)                    |
| `adult_income_output_summary.json`                   | JSON   | Summary of the first analysis (age)           |

## 📈 Example Results

### Pivot 1 – Count by income and education category (partial)

| income | bachelors | masters | prof-school |
|--------|-----------|---------|--------------|
| <=50K  | 4273      | 1198    | 217          |
| >50K   | 2503      | 1459    | 617          |

### Pivot 2 – Mean weekly hours by work class and income (partial)

| workclass      | <=50K | >50K |
|----------------|-------|------|
| federal-gov    | 40.21 | 43.54|
| local-gov      | 39.46 | 44.16|
| private        | 38.20 | 45.04|
| self-emp-inc   | 46.55 | 50.24|

## 🤖 Automation

The entire pipeline is encapsulated in well-defined functions, and the `main()` function coordinates the complete flow:

- `load_data()` – extraction
- `inspect_data()` – inspection
- `clean_data()` – cleaning
- `transform_data()` – transformations
- `create_aux_table()` / `merge_aux()` – merge
- `groupby_analyses()` – aggregations
- `pivot_analysis()` – pivot tables
- `export_data()` – export

This structure makes the code modular, easy to maintain, and reusable for other datasets.

## 📝 Conclusion

The project fully meets the requirements of an ETL pipeline with Pandas. The analyses revealed that:

- Individuals with higher education (bachelors, masters, prof-school) are more likely to have income >50K.
- Self-employed incorporated individuals (self-emp-inc) work more hours and have the highest mean age among those earning above 50K.
- Weekly working hours are consistently higher for people with income >50K, regardless of work class.

All output files are generated automatically, ensuring reproducibility and ease of use in other visualization or modeling tools.

## 👤 Author

Gabriel Paixão de Oliveira – Final Project for the **Programming Systems** course.

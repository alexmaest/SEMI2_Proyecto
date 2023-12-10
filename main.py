from io import StringIO
import pandas as pd
import requests

def load_towns(file):
    df = None
    try:
        df = pd.read_csv(file)
        df.columns = df.columns.str.lower()
        print(df)
       
        print('\n')
        print('Information: All town data loaded')
        print('\n')
    except Exception as e:
        print(f'Error: Failed to load towns data: {str(e)}')
    
    return df

def load_countries(url):
    df = None
    try:
        response = requests.get(url)
        if response.status_code == 200:
            csv_content = response.text
            csv = csv_content.replace('ï»¿', '')
            df = pd.read_csv(StringIO(csv))
            # df = pd.read_csv('./data/global.csv')
            df.columns = df.columns.str.lower()
            print(df)

            print('\n')
            print('Information: All countries data loaded')
            print('\n')
        else:
            print(f"Error: Failed to fetch data. Status code: {response.status_code}")
    except Exception as e:
        print(f'Error: Failed to load countries data: {str(e)}')

    return df

def clean_towns(df):
    df_processed = df
    try:
        # Delete duplicated rows
        df_processed = df.drop_duplicates(subset=['departamento', 'municipio'])
        print(df_processed)

        # Delete unnecessary columns
        df_processed = df_processed.drop(['codigo_departamento', 'codigo_municipio'], axis=1)
        print(df_processed)

        # Only 2021 data
        df_processed = df_processed.filter(regex=r'^(.*2021.*)$|^(departamento|municipio|poblacion)$', axis=1)
        print(df_processed)

        # Replace null values and write invalid values to non determining columns
        modify = [col for col in df_processed.columns if col not in ['departamento', 'municipio']]
        for col in modify:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0).astype('Int64')
        print(df_processed)

        # Delete rows with null values to determining columns
        df_processed = df_processed.dropna(subset=['departamento', 'municipio'])
        print(df_processed)

        # Delete rows with incorrect type values to determining columns
        df_processed = df_processed[~df_processed['departamento'].astype(str).str.isnumeric() & ~df_processed['municipio'].astype(str).str.isnumeric()]
        print(df_processed)

        print('\n')
        print('Information: All towns data cleaned')
        print('\n')
    except Exception as e:
        print(f'Error: Failed to clean towns data: {str(e)}')
    
    return df_processed

def clean_countries(df, country):
    df_processed = df
    try:
        # Only Guatemalan data
        df_processed = df[df['country'].str.lower() == country.lower()]
        print(df_processed)

        # Delete duplicated rows
        df_processed = df_processed.drop_duplicates(subset=['date_reported'])
        print(df_processed)

        # Delete unnecessary columns
        df_processed = df_processed.drop(['country_code', 'who_region', 'new_cases', 'cumulative_cases'], axis=1)
        print(df_processed)

        # Replace null values and write invalid values to non determining columns
        modify = [col for col in df_processed.columns if col not in ['date_reported', 'country']]
        for col in modify:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0).astype('Int64')
        print(df_processed)

        # Replace null values and write invalid values to determining columns
        df_processed = df_processed.dropna(subset=['date_reported', 'country'])
        for col in ['date_reported', 'country']:
            df_processed = df_processed[df_processed[col].apply(lambda x: isinstance(x, str))]
        print(df_processed)

        # Convert date_reported to datetime type
        df_processed['date_reported'] = pd.to_datetime(df_processed['date_reported'], errors='coerce')
        df_processed = df_processed.dropna(subset=['date_reported'])
        print(df_processed)

        # Only 2021 data
        df_processed = df_processed[df_processed['date_reported'].dt.year == 2021]
        print(df_processed)

        print('\n')
        print('Information: All countries data cleaned')
        print('\n')
    except Exception as e:
        print(f'Error: Failed to clean countries data: {str(e)}')
    
    return df_processed

# Sources
source_towns = './data/municipio.csv'
source_countries = 'https://seminario2.blob.core.windows.net/fase1/global.csv?sp=r&st=2023-12-06T03:45:26Z&se=2024-01-04T11:45:26Z&sv=2022-11-02&sr=b&sig=xdx7LdUOekGyBvGL%2FNE55ZZj9SBvCC%2FWegxtpSsKjJg%3D'

# Load data
towns_df = load_towns(source_towns)
countries_df = load_countries(source_countries)

# Clean data
towns_tr = clean_towns(towns_df)
countries_tr = clean_countries(countries_df, 'Guatemala')


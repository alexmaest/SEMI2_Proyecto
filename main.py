from database import execute_queries, execute_country, execute_recover, inserted, failed
from itertools import batched
from io import StringIO
import pandas as pd
import requests
import sys

towns = []
departments = []
deaths = []

def arguments():
    try:
        if len(sys.argv) < 2:
            print("Information: Please type the batch size as argument")
            sys.exit(1)
        return int(sys.argv[1])
    except Exception as e:
        print(f'Error: Failed to set batch size: {str(e)}')

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

        # Only 2020 data
        df_processed = df_processed.filter(regex=r'^(.*2020.*)$|^(departamento|municipio|poblacion)$', axis=1)
        print(df_processed)

        # Replace null values and write invalid values to non determining columns
        modify = [col for col in df_processed.columns if col not in ['departamento', 'municipio']]
        for col in modify:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0).astype('Int64')
            df_processed[col] = df_processed[col].apply(lambda x: max(x, 0))
        print(df_processed)

        # Delete rows with null values to determining columns
        df_processed = df_processed.dropna(subset=['departamento', 'municipio'])
        print(df_processed)

        # Clear to only right values on determining columns
        credibilidad_mask = df_processed[['departamento','municipio']].map(lambda x: any(char.isdigit() for char in str(x)))
        df_processed = df_processed[~credibilidad_mask.any(axis=1)]
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
            df_processed[col] = df_processed[col].apply(lambda x: max(x, 0))
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

        # Only 2020 data
        df_processed = df_processed[df_processed['date_reported'].dt.year == 2020]
        print(df_processed)

        print('\n')
        print('Information: All countries data cleaned')
        print('\n')
    except Exception as e:
        print(f'Error: Failed to clean countries data: {str(e)}')
    
    return df_processed

def transform_data(towns, countries):
    df_final = None
    try:
        # Transform towns dataframe
        df_towns = pd.melt(towns, id_vars=['departamento', 'municipio', 'poblacion'], var_name='Fecha', value_name='MuertesFuente1')

        # Transform 'Fecha' column to datetime
        df_towns['Fecha'] = pd.to_datetime(df_towns['Fecha'], errors='coerce')

        # Merge both dataframs by date column
        df_final = pd.merge(df_towns, countries[['date_reported', 'new_deaths', 'cumulative_deaths']], left_on='Fecha', right_on='date_reported', how='left')

        # Format column
        df_final = df_final.rename(columns={'new_deaths': 'MuertesFuente2', 'cumulative_deaths': 'MuertesAcumulativas'})
        print(df_final)

        print('\n')
        print('Information: All countries data cleaned')
        print('\n')
    except Exception as e:
        print(f'Error: Failed to clean countries data: {str(e)}')
        
    return df_final

def create_blocks(data, batch_size):
    blocks = list(batched(data, batch_size))
    final_blocks = []
    execute_country()
    print('\n')
    print('Information: Creating',len(blocks),'blocks...')
    print('\n')
    for block in blocks:
        queries = {}
        queries.update({'departments': create_departments_query(block)})
        queries.update({'towns': create_towns_query(block)})
        queries.update({'deathsource1': create_deathsource1_query(block)})
        queries.update({'deathsource2': create_deathsource2_query(block)})
        final_blocks.append(queries)

    return final_blocks

def create_departments_query(data):
    departments_local = []
    query = 'INSERT INTO department (Name, CountryId) VALUES\n'
    for i, row in enumerate(data):
        if any(department['Name'] == row.departamento for department in departments): continue
        
        departments.append({'Name': row.departamento})
        departments_local.append({'Name': row.departamento})
        query += f'(\'{row.departamento}\', (SELECT Id FROM country WHERE Name = \'Guatemala\')),\n'
    if len(departments_local) == 0: return ''
    else: return query[:-2] + ';\n'

def create_towns_query(data):
    towns_local = []
    query = 'INSERT INTO town (Name, Poblation, DepartmentId) VALUES\n'
    for i, row in enumerate(data):
        if any(town['Department'] == row.departamento and town['Town'] == row.municipio for town in towns): continue

        towns.append({'Department': row.departamento, 'Town': row.municipio})
        towns_local.append({'Department': row.departamento, 'Town': row.municipio})
        query += f'(\'{row.municipio}\', {row.poblacion}, (SELECT Id FROM department WHERE Name = \'{row.departamento}\')),\n'
    if len(towns_local) == 0: return ''
    else: return query[:-2] + ';\n'

def create_deathsource1_query(data):
    query = 'INSERT INTO deathsource1 (Date, Number, TownId) VALUES\n'
    for i, row in enumerate(data):
        query += f'(\'{row.Fecha}\', {row.MuertesFuente1}, (SELECT Id FROM town WHERE Name = \'{row.municipio}\' AND DepartmentId = (SELECT Id FROM department WHERE Name = \'{row.departamento}\'))),\n'

    return query[:-2] + ';\n'

def create_deathsource2_query(data):
    deaths_local = []
    query = 'INSERT INTO deathsource2 (Date, Number, Acumulative, CountryId) VALUES\n'
    for i, row in enumerate(data):
        if any(death['date_reported'] == row.date_reported for death in deaths): continue

        deaths.append({'date_reported': row.date_reported})
        deaths_local.append({'date_reported': row.date_reported})
        query += f'(\'{row.date_reported}\', {row.MuertesFuente2}, {row.MuertesAcumulativas}, (SELECT Id FROM country WHERE Name = \'Guatemala\')),\n'
    if len(deaths_local) == 0: return ''
    else: return query[:-2] + ';\n'

def insert_data(blocks):
    execute_queries(blocks)
    execute_recover()

# Arguments
batch_size = arguments()

if (batch_size):
    # Sources
    source_towns = './data/municipio.csv'
    source_countries = 'https://seminario2.blob.core.windows.net/fase1/global.csv?sp=r&st=2023-12-06T03:45:26Z&se=2024-01-04T11:45:26Z&sv=2022-11-02&sr=b&sig=xdx7LdUOekGyBvGL%2FNE55ZZj9SBvCC%2FWegxtpSsKjJg%3D'

    # Load data
    towns_df = load_towns(source_towns)
    countries_df = load_countries(source_countries)

    # Clean data
    towns_tr = clean_towns(towns_df)
    countries_tr = clean_countries(countries_df, 'Guatemala')

    # Transform data
    all_data = transform_data(towns_tr, countries_tr)

    # Insert data
    blocks = create_blocks(list(all_data.itertuples()), batch_size)
    insert_data(blocks)

from database import connection
from io import StringIO
import pandas as pd
import requests
import pymysql

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

def insert_data(data):
    try:
        with connection.cursor() as cursor:
            # Unique country insertion
            query_country = "INSERT INTO country (Name) VALUES ('Guatemala')"
            cursor.execute(query_country)

            # Obtain Guatemalan id
            query_country_id = "SELECT Id FROM country WHERE Name = 'Guatemala'"
            cursor.execute(query_country_id)
            result = cursor.fetchone()

            if result:
                country_id = result['Id']
            else:
                print("No se encontró el país 'Guatemala' en la tabla country")
                return

            # Insertion of Department, Town, DeathSource1 y DeathSource2 in blocks
            block_size = 50
            data_rows = []
            inserted_blocks = 0
            failed_blocks = 0
            print('\n')
            print('Information: Inserting blocks...')
            print('\n')
            for index, row in data.iterrows():
                try:
                    # Check if department already exists
                    query_check_department = f"SELECT Id FROM department WHERE Name = '{row['departamento']}' AND CountryId = {country_id}"
                    cursor.execute(query_check_department)
                    result_department = cursor.fetchone()

                    if result_department:
                        department_id = result_department['Id']
                    else:
                        query_department = f"INSERT INTO department (Name, CountryId) VALUES ('{row['departamento']}', {country_id}) ON DUPLICATE KEY UPDATE Id=LAST_INSERT_ID(Id)"
                        cursor.execute(query_department)

                        query_department_id = "SELECT LAST_INSERT_ID()"
                        cursor.execute(query_department_id)
                        result_department_id = cursor.fetchone()

                        if result_department_id:
                            department_id = result_department_id['LAST_INSERT_ID()']
                        else:
                            print('Error: Not inserted value',result_department_id)
                            failed_blocks += 1
                            connection.rollback()
                            data_rows = []
                            continue

                    # Check if town already exists
                    query_check_town = f"SELECT Id FROM town WHERE Name = '{row['municipio']}' AND DepartmentId = {department_id}"
                    cursor.execute(query_check_town)
                    result_town = cursor.fetchone()

                    if result_town:
                        town_id = result_town['Id']
                    else:
                        query_town = f"INSERT INTO town (Name, Poblation, DepartmentId) VALUES ('{row['municipio']}', {row['poblacion']}, {department_id}) ON DUPLICATE KEY UPDATE Id=LAST_INSERT_ID(Id)"
                        cursor.execute(query_town)

                        query_town_id = "SELECT LAST_INSERT_ID()"
                        cursor.execute(query_town_id)
                        result_town_id = cursor.fetchone()

                        if result_town_id:
                            town_id = result_town_id['LAST_INSERT_ID()']
                        else:
                            print('Error: Not inserted value',result_town_id)
                            failed_blocks += 1
                            connection.rollback()
                            data_rows = []
                            continue

                    query_deathsource1 = f"INSERT INTO DeathSource1 (Date, Number, TownId) VALUES ('{row['Fecha']}', {row['MuertesFuente1']}, {town_id})"
                    cursor.execute(query_deathsource1)

                    query_check_deathsource2 = f"SELECT Id FROM DeathSource2 WHERE Date = '{row['date_reported']}' AND CountryId = {country_id}"
                    cursor.execute(query_check_deathsource2)
                    result_deathsource2 = cursor.fetchone()

                    if not result_deathsource2:
                        query_deathsource2 = f"INSERT INTO DeathSource2 (Date, Number, Acumulative, CountryId) VALUES ('{row['date_reported']}', {row['MuertesFuente2']}, {row['MuertesAcumulativas']}, {country_id})"
                        cursor.execute(query_deathsource2)

                    # Add row to actual block
                    data_rows.append(row)

                    # Block insertion and reset
                    if len(data_rows) == block_size:
                        connection.commit()
                        inserted_blocks += 1
                        data_rows = []
                except Exception as e:
                    print(f'Error: Not inserted value by the following error: {str(e)}')
                    connection.rollback()

            # Final commit
            connection.commit()
            print(f"\nBloques insertados: {inserted_blocks}", f"Bloques con errores: {failed_blocks}")

    except pymysql.Error as err:
        print(f"Error: {err}")
        connection.rollback()

    finally:
        connection.close()

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
insert_data(all_data)

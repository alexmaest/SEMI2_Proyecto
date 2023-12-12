![Net Image](./assets/banner.jpg "Banner | Proyecto 1")

## Manual técnico | Proyecto 1 <img src="https://media.tenor.com/dHk-LfzHrtwAAAAi/linux-computer.gif" alt="drawing" width="30"/>

### _Descripción_

Se ha realizado un estudio sobre los datos recopilados por el Ministerio de Salud durante la pandemia del Covid-19. Estos datos fueron obtenidos por medio de tabulación de estos durante todos los días del año 2020. 
Como científico de datos, se realizaron las tareas asignadas de recolección y limpieza de los datos, para posteriormente guardarlos en una base de datos. Por medio de la recolección se buscó obtener todos los datos de 2 fuentes principales, una como un archivo local y otra de un url proporcionado.

### _Proceso_
#### Extracción

`load_towns(file)`

Propósito: Esta función está diseñada para cargar datos de muertes especificadas por municipio desde un archivo CSV a un DataFrame de la librería Pandas.

Parámetros:
- file: La ruta del archivo CSV que contiene los datos de los pueblos.

Comportamiento: 
- Intenta leer el archivo CSV especificado por el parámetro file en un DataFrame de Pandas (df).
- Convierte los nombres de las columnas a minúsculas para mantener consistencia.
- Imprime el DataFrame cargado (df) si el archivo se lee con éxito.
- Imprime un mensaje de éxito si los datos se cargan correctamente.
- Imprime un mensaje de error si ocurre alguna excepción durante el proceso de carga.

Retorna:
Retorna el DataFrame de Pandas cargado (df) si tiene éxito; de lo contrario, retorna None.

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

`load_countries(url)`

Propósito: Esta función está diseñada para cargar datos de países desde un archivo CSV accesible mediante una URL a un DataFrame de la librería Pandas.

Parámetros: url: La URL que apunta al archivo CSV que contiene los datos de los países.

Comportamiento:
- Envía una solicitud HTTP a la URL especificada para obtener el contenido CSV.
- Si la solicitud tiene éxito (código de estado 200), se procesa el contenido CSV y se crea un DataFrame de Pandas (df).
- Convierte los nombres de las columnas a minúsculas para mantener consistencia.
- Imprime el DataFrame cargado (df) si los datos se cargan con éxito.
- Imprime un mensaje de éxito si los datos se cargan correctamente.
- Imprime un mensaje de error si la solicitud HTTP falla o si ocurre alguna excepción durante el proceso de carga.

Retorna: Retorna el DataFrame de Pandas cargado (df) si tiene éxito; de lo contrario, retorna None.
Estas funciones siguen una estructura similar, utilizando un bloque try-except para manejar posibles errores durante el proceso de carga de datos. Las funciones imprimen mensajes informativos para indicar el éxito o el fracaso de la operación de carga.

    def load_countries(url):
        df = None
        try:
            response = requests.get(url)
            if response.status_code == 200:
                csv_content = response.text
                csv = csv_content.replace('ï»¿', '')
                df = pd.read_csv(StringIO(csv))
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

`clean_towns(df)`

Propósito: Esta función está diseñada para realizar una serie de operaciones de limpieza en un DataFrame de municipios.

Parámetros:
- df: DataFrame de Pandas que contiene datos de municipios.

Comportamiento:
- Elimina las filas duplicadas basadas en las columnas 'departamento' y 'municipio'.
- Elimina columnas innecesarias ('codigo_departamento' y 'codigo_municipio').
- Conserva solo las columnas relacionadas con el año 2021, 'departamento', 'municipio' y 'poblacion'.
- Reemplaza los valores nulos y escribe los valores por defecto en columnas no determinantes.
- Elimina las filas con valores nulos en las columnas determinantes ('departamento' y 'municipio').
- Elimina las filas con valores incorrectos en las columnas determinantes ('departamento' y 'municipio').

Retorna: Retorna un nuevo DataFrame de Pandas (df_processed) después de aplicar las operaciones de limpieza.

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

`clean_countries(df, country)`

Propósito: Esta función está diseñada para realizar una serie de operaciones de limpieza en un DataFrame de países, enfocándose en datos específicos de Guatemala.

Parámetros:
- df: DataFrame de Pandas que contiene datos de países.
- country: País al que se restringirán los datos (en este caso, Guatemala).

Comportamiento:
- Filtra el DataFrame para incluir solo datos relacionados con el país especificado (Guatemala).
- Elimina las filas duplicadas basadas en la columna 'date_reported'.
- Elimina columnas innecesarias ('country_code', 'who_region', 'new_cases' y 'cumulative_cases').
- Reemplaza los valores nulos y escribe los valores por defecto en columnas no determinantes.
- Elimina las filas con valores nulos en las columnas determinantes ('date_reported' y 'country').
- Elimina las filas con valores no válidos en las columnas determinantes ('date_reported' y 'country').
- Convierte la columna 'date_reported' al tipo de dato datetime.
- Filtra los datos para incluir solo aquellos del año 2021.

Retorna: Retorna un nuevo DataFrame de Pandas (df_processed) después de aplicar las operaciones de limpieza.

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

#### Transformación

`transform_data(towns, countries)`

Propósito: Esta función está diseñada para realizar transformaciones en los datos de municipios y países, para al final fusionarlos en un único DataFrame.

Parámetros:
- towns: DataFrame de Pandas que contiene datos de municipios.
- countries: DataFrame de Pandas que contiene datos de países.

Comportamiento:
- Utiliza la función pd.melt para transformar el DataFrame de municipios (towns) de un formato ancho a un formato largo, manteniendo las columnas 'departamento', 'municipio', 'poblacion' y creando nuevas columnas 'Fecha' y 'MuertesFuente1'.
- Convierte la columna 'Fecha' al tipo de dato datetime.
- Fusiona los DataFrames de municipios y países (countries) utilizando la columna de fecha ('Fecha' y 'date_reported', respectivamente) mediante la función pd.merge.
- Renombra las columnas 'new_deaths' y 'cumulative_deaths' a 'MuertesFuente2' y 'MuertesAcumulativas', respectivamente.
- Imprime el DataFrame resultante (df_final).

Retorna: Retorna un nuevo DataFrame de Pandas (df_final) que resulta de la fusión y transformación de los datos de municipios y países.

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

#### Carga

`insert_data(data)`

Propósito: Esta función está diseñada para insertar datos en una base de datos MySQL por bloques de 50 datos, específicamente en tablas relacionadas con información de países, departamentos, municipios y fuentes de datos sobre muertes.

Parámetros:
- data: DataFrame de Pandas que contiene los datos a ser insertados en la base de datos.

Comportamiento:
- Inserta de manera única el país 'Guatemala' en la tabla country.
- Obtiene el identificador (Id) del país 'Guatemala' recién insertado.
- Inserta datos en bloques (por lotes) en las tablas department, town, DeathSource1 y DeathSource2.
- Verifica la existencia de departamentos y municipios antes de insertar nuevos registros, utilizando consultas SELECT.
- Utiliza operaciones ON DUPLICATE KEY UPDATE para manejar conflictos de clave única y obtener el último ID insertado.
- Realiza comprobaciones para asegurar la integridad referencial y evita la duplicación de registros en las tablas department y town.
- Realiza inserciones en las tablas DeathSource1 y DeathSource2 con información relacionada con fechas y fuentes de muerte.
- Realiza inserciones en bloques de tamaño específico (definido por block_size) y realiza commit después de cada bloque.
- Imprime información sobre la cantidad de bloques insertados con éxito y la cantidad de bloques con errores.
- Maneja posibles errores de MySQL utilizando bloques try-except y realiza commit final antes de cerrar la conexión.

Retorna: No retorna ningún valor. La función realiza operaciones de inserción directamente en la base de datos.

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

### _Modelo relacional_

Para el desarrollo de la carga de datos, se procedió a utilizar el modelo presentado a continuación, donde se tiene como country tabla principal, y las demás tablas se desarrollan para almacenar los datos de las dos fuentes proporcionadas, donde la primera requiere de almacenamiento de localizaciones más específicas como departamento y municipio, mientras que la segunda fuente no poseía esa información, por lo que únicamente se tiene la tabla deathsource2 que almacena todos los datos proporcionados, como fecha, número de muertes, número de muertes acumulativas, y en su contraparte de la primera fuente llamada deathsource1, en la cual se tienen datos similares únicamente cambia a una relación con su respectivo municipio, lo que no pasa con deathsource2 que es directamente con la tabla country.

![relational](./assets/relational.png)

###### _2023 - Laboratorio de Seminario de Sistemas 2_

---

import pandas as pd
import pymysql

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='root',
    db='semi2',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

if connection:
    print('Information: Database connection succeeded')
else:
    raise ConnectionError('Error: Database connection failed')

def obtain_dataset():
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT
                    DS1.Date AS Date,
                    D.Name AS Department,
                    T.Name AS Town,
                    DS1.Number AS Number_DeathSource1,
                    DS2.Number AS Number_DeathSource2,
                    SUM(DS1.Number) OVER (ORDER BY DS1.Date) AS Acumulative_DeathSource1,
                    DS2.Acumulative AS Acumulative_DeathSource2,
                    T.Poblation AS Population
                FROM
                    deathsource1 DS1
                    INNER JOIN deathsource2 DS2 ON DS1.Date = DS2.Date
                    INNER JOIN town T ON DS1.TownId = T.Id
                    INNER JOIN department D ON T.DepartmentId = D.Id
            """
            cursor.execute(query)
            result = pd.DataFrame(cursor.fetchall())
            return result
    finally:
        connection.close()
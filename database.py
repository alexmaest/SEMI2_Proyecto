import pymysql

connection = pymysql.connect(
    host='localhost',
    user='root',
    db='semi2',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

if connection:
    print('Information: Database connection succeeded')
else:
    raise ConnectionError('Database connection failed')
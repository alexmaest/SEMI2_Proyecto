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

inserted = 0
failed = 0
blocks_failed = []

def execute_queries(queries):
    block_number = 1
    print('\n')
    print('Information: Inserting blocks...')
    print('\n')
    for query in queries:
        execute(query, block_number)
        block_number += 1
    print("\n")
    print(f"Information: Inserted blocks: {inserted}", f" blocks not inserted: {failed}")
    print("\n")

def execute(query, block_number):
    try:
        global inserted
        cursor = connection.cursor()
        if query['departments'] != '': cursor.execute(query['departments'])
        if query['towns'] != '': cursor.execute(query['towns'])
        if query['deathsource1'] != '': cursor.execute(query['deathsource1'])
        if query['deathsource2'] != '': cursor.execute(query['deathsource2'])
        # print('Information: Block number',block_number,'inserted')
        inserted += 1
        connection.commit()
    except Exception as e:
        global failed
        failed += 1
        blocks_failed.append(query)
        print('Information: Block number',block_number,' failed to insert')
        print(f"Error: Failed to execute query: {e}")
        connection.rollback()
    else:
        cursor.close()

def execute_country():
    try:
        cursor = connection.cursor()
        cursor.execute('INSERT INTO country (Name) VALUES (\'Guatemala\')')
        connection.commit()
    except Exception as e:
        print(f"Error: Failed to execute query: {e}")
        connection.rollback()
    else:
        cursor.close()

def execute_recover():
    if len(blocks_failed) == 0:
        print("\n")
        print('Information: All blocks inserted succesfully, no recovery needed')
        print("\n")
        return
    
    block_number = 1
    for query in blocks_failed:
        recover(query, block_number)
        block_number += 1
    print("\n")
    print(f"Information: Inserted blocks: {inserted}", f" blocks not inserted: {failed}")
    print("\n")

def recover(query, block_number):
    try:
        global inserted
        cursor = connection.cursor()
        if query['departments'] != '': cursor.execute(query['departments'])
        if query['towns'] != '': cursor.execute(query['towns'])
        if query['deathsource1'] != '': cursor.execute(query['deathsource1'])
        if query['deathsource2'] != '': cursor.execute(query['deathsource2'])
        inserted += 1
        print('Information: Recovery block number',block_number,'inserted')
        connection.commit()
    except Exception as e:
        print('Error: Recovery block number',block_number,'failed to insert')
        print(f"Error: Failed to execute query: {e}")
        connection.rollback()
    else:
        cursor.close()
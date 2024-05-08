import sqlite3
import sql_queries as sq

db_conn = sqlite3.connect(r'.\data\database\db_test.db')

# create tables in create_table_list
for table in sq.create_table_list:
    cursor = db_conn.cursor()
    
    try:
        cursor.execute(table)
        db_conn.commit()
        cursor.close()
    except Exception as e:
        print(e)
        db_conn.rollback()
        cursor.close()
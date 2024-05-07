import os
import logging
import sys
sys.path.append('./src/lib/etl')
import sql_queries as sq


class ETL():
    
    def __init__(self, credentials_file_path, local_database) -> None:
        self.credentials_file_path = credentials_file_path 
        self.local_database = local_database 

    def extract_bigquery_data(self, start_date, table_name):
        from google.cloud import bigquery
        from google.oauth2 import service_account

        credentials = service_account.Credentials.from_service_account_file(self.credentials_file_path)
        client = bigquery.Client(credentials= credentials, project= credentials.project_id)

        with open(rf'./src/lib/etl/{table_name}.sql','r', encoding= 'utf-8') as f:
            query_string = f.read()
            query_string = query_string.format(start_date= '"' +str(start_date)+'"')

        # Make an API request.
        query_job = client.query(query_string)
        df = query_job.to_dataframe()
        
        return df
    
    def extract_local_data(self, table_name):
        import duckdb
        import polars as pl

        conn = duckdb.connect()
        conn.execute('LOAD sqlite;')
        db_conn =  duckdb.connect(self.local_database)

        
        for query_table in sq.query_table_list:
            if table_name in query_table:
                df = pl.read_database(f'select * from {table_name}',connection= db_conn).to_pandas()
                logging.info(f'Succesfully extracted data from table: {table_name}')
                db_conn.close()
                return df
    
    def ingest_data_to_local_db(self, data_ingest, table_name : str) -> None:
        '''
        Insert data into local db.

        param data_ingest: must be a type of dataframe
        '''
        from datetime import datetime
        import polars as pl
        import duckdb

        conn = duckdb.connect()
        conn.execute('LOAD sqlite;')
        db_conn = duckdb.connect(self.local_database)
        
        for table in sq.create_table_list:
            if table_name in table:
                cursor = db_conn.cursor()
                
                try:
                    cursor.execute(table)
                    db_conn.commit()
                    cursor.close()
                except:
                    db_conn.rollback()
                    cursor.close()
        
        for table in sq.insert_table_list:
            date_perform = datetime.now().date().strftime('%Y-%m-%d')
            if table_name in table:
                try:
                    _df = pl.from_dataframe(data_ingest)
                    _df.write_database(table_name=table_name,connection=db_conn,if_table_exists='append')
                    db_conn.commit()
                    logging.info('Update data sucessfully !!!')      
                
                except Exception as e:
                    logging.info('Update data failed. Nothing is updated !!!')
                    logging.info('Error: %s' %e)          

    def auto_update_data(self, table_name: str, update_type):
        import duckdb

        conn = duckdb.connect()
        conn.execute('LOAD sqlite;')
        db_conn = duckdb.connect(self.local_database)

        if update_type == 'update_as_new':
            db_conn.execute("delete from {}".format(table_name))
        

        with db_conn:
            db_conn.execute("select coalesce(date_add(max(complete_date), interval 1 day),'2023-01-01') from {}".format(table_name))
            last_date= db_conn.fetchone()[0]
            db_conn.close()
        
        #call API to get new data
        new_data = self.extract_bigquery_data(start_date= last_date, table_name= table_name)

        # insert into local db
        self.ingest_data_to_local_db(data_ingest=new_data, table_name= table_name)
        

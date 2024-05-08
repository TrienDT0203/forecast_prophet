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
        from datetime import datetime

        credentials = service_account.Credentials.from_service_account_file(self.credentials_file_path)
        client = bigquery.Client(credentials= credentials, project= credentials.project_id)

        with open(rf'./src/lib/etl/{table_name}.sql','r', encoding= 'utf-8') as f:
            query_string = f.read()
            query_string = query_string.format(start_date= '"' +str(start_date)+'"')

        # Make an API request.
        print(f'Start insert data from bigquery at : {str(datetime.now())} \n')
        query_job = client.query(query_string)
        df = query_job.to_dataframe()
        print(f'Finished insert data from bigquery at: {str(datetime.now())} \n')
        
        return df
    
    def extract_local_data(self, table_name):
        import duckdb
        import polars as pl

        conn = duckdb.connect()
        conn.execute("INSTALL sqlite")
        conn.execute('LOAD sqlite;')
    
        try:
            db_conn =  duckdb.connect(self.local_database)
        except Exception as e:
            print('Error: %s' % e)
            return {'status':'unsuccess', 'message': e} 

        try:
            for query_table in sq.query_table_list:
                if table_name in query_table:
                    df = pl.read_database(query= f'select * from {table_name}', connection= db_conn).to_pandas()
                    logging.info(f'Succesfully extracted data from table: {table_name}')
                    db_conn.close()
                    return df
                else:
                    while True:
                        init_table = input('Do you want to create table? (Y/n)')
                        if init_table.lower() == 'y':
                            for table in sq.create_table_list:
                                if table_name in table:
                                    try:
                                        db_conn.query(table)
                                        db_conn.close()
                                        print('Create table successfully !!!')
                                        return {'status':'success', 'message': 'Create table successfully !!!'} 
                                    except Exception as e :
                                        print('Can not create table !!!')
                                        print('Error: %s' % e)
                                        db_conn.close()
                                        return {'status':'unsuccess', 'message': e}
                        elif init_table.lower() == 'n':
                            return 
                        else:
                            print('Only accept values: Y/n. Try again!')
                            pass
                        
        except Exception as e:
            logging.info(f'Error: {e}')
    
        


    def ingest_data_to_local_db(self, data_ingest, table_name : str) -> None:
        '''
        Insert data into local db.

        param data_ingest: must be a type of DataFrame
        '''
        from datetime import datetime
        import pandas as pd
        import duckdb

        conn = duckdb.connect()
        conn.execute('LOAD sqlite;')
        db_conn = duckdb.connect(self.local_database)
        
        for table in sq.create_table_list:
            if table_name in table:
                cursor = db_conn.cursor()
                
                try:
                    cursor.execute(table)
                    cursor.close()
                except:
                    cursor.close()
        
        for table in sq.insert_table_list:
            date_perform = datetime.now()
            data_ingest['update_time'] = date_perform
            data_ingest['complete_date'] = pd.to_datetime(data_ingest['complete_date'], format= '%Y-%m-%d')
            
            if table_name in table:
                try:
                    db_conn.register('df',data_ingest)
                    db_conn.sql(f"INSERT INTO {table_name} SELECT * FROM df")
                    return { 'status':'unsuccess', 'error': 'Update data sucessfully !!!'}
                
                except Exception as e:
                    print('Update data failed. Nothing is updated !!!')
                    return { 'status':'unsuccess', 'error': f'{e}'}          

    def auto_update_data(self, table_name: str, update_type):
        from datetime import datetime
        import duckdb

        conn = duckdb.connect()
        conn.execute('LOAD sqlite;')
        db_conn = duckdb.connect(self.local_database)

        if update_type == 'update_as_new':
            db_conn.execute("delete from {}".format(table_name))
        

        with db_conn:
            db_conn.execute("select coalesce(date_add(cast(max(complete_date) as date), interval 1 day),cast('2023-01-01' as date)) from {}".format(table_name))
            last_date= db_conn.fetchone()[0]
            db_conn.close()
        
        #call API to get new data
        print(f'Start fetching data from bigquery at : {str(datetime.now())} \n')
        print(f'Finished fetching data from bigquery at: {str(datetime.now())} \n')
        new_data = self.extract_bigquery_data(start_date= last_date, table_name= table_name)
        print(f'Start ingest data into table: {table_name}')
        
        # insert into local db
        update_status = self.ingest_data_to_local_db(data_ingest=new_data, table_name= table_name)
        if update_status['status'] == 'unsuccess':
            print(update_status['error'])
        else:
            print(update_status['status'])
        

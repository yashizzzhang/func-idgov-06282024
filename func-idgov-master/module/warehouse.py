import logging, struct, urllib
from sqlalchemy import create_engine, MetaData, Table, func, text
from math import floor 
import pandas as pd

class Warehouse:

    db_engine = None

    def __init__(self, server, database, credential) -> None:
        logging.info('Warehouse: initializing ...')
        token = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        token_struct = struct.pack(f'<I{len(token)}s', len(token), token)
        driver="{ODBC Driver 18 for SQL Server}"
        connection_string = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database
        params = urllib.parse.quote(connection_string)
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        self.db_engine = create_engine("mssql+pyodbc:///?odbc_connect={0}".format(params), connect_args={'attrs_before': {SQL_COPT_SS_ACCESS_TOKEN:token_struct}})

    ## delete all rows in the table
    def erase(self, table_name) -> None:
        logging.info(f'Warehouse: erase() --> table: {table_name}')
        try:
            with self.db_engine.connect() as conn:
                conn.execute(text(f"DELETE FROM {table_name}"))
                conn.commit()
        ## if Table not exist, this avoid error interupt
        except:
            pass

    ## return max value of a column
    def get_max(self, table_name, column_name):
        logging.info(f'Warehouse: get_max() --> table: {table_name}, column: {column_name}')
        ## connection is automatically closed at the end of with-block
        with self.db_engine.connect() as conn:
            result = conn.execute(text(f"SELECT MAX({column_name}) FROM {table_name}"))
            row = result.fetchone()
            return row[0]

    ## append dataframe to existing table
    def append(self, table_name, df) -> None: 
        logging.info(f'Warehouse: append() - dataframe rows: {df.shape[0]}')
        chunksize = floor(2100/df.shape[1]) -1
        with self.db_engine.connect() as conn:
            df.to_sql(table_name, con=conn, index=False, if_exists='append', method='multi', chunksize=chunksize)

    ## retrieve all rows from a table
    def get_table(self, table_name) -> pd.DataFrame:
        df = pd.read_sql(table_name, con = self.db_engine)
        logging.info(f'Warehouse: get_table() - {table_name} : {df.shape[0]}')
        return df

    ## remove rows with criteria
    def delete_rows(self, table_name, column_name, value):
        logging.info(f'Warehouse: delete_rows() --> table: {table_name}')
        try:
            with self.db_engine.connect() as conn:
                conn.execute(text(f"DELETE FROM {table_name} WHERE {column_name}='{value}'"))
                conn.commit()
        ## for no existig table, this avoid error interupt
        except:
            pass   

    ## refresh rows by first delete rows and then append
    def refresh_table_rows(self, table_name, df, column_name, value):
        logging.info(f'Warehouse: refresh_table_rows() --> table: {table_name}')
        ## First Delete the 
        self.delete_rows(table_name, column_name, value)
        self.append(table_name, df)
import sqlite3
import pandas as pd
import logging as logger
import sys
import platform

class rappi_db:
   
    logger.warning("Warning message: Install necessary dependencies - run 'pip install -r requirements.txt' command")
    
    @staticmethod
    def connect(db = 'rappi_data'):
        """ Generation connection to db

        Considerations
        ---------------------------------------------------------
        When you call this method, this automatically create a connection to default schema

        Returns
        ---------------------------------------------------------

        sqlite3.connect object

        """
        try:
            conn = sqlite3.connect(db) 
            print("Connection Established to rappi_db")
        except:
            logger.error("Connection Error",exc_info=True)

        return conn

    @staticmethod 
    def upload_table(df = str,table= 'credit_card_data'):
        """ Create Table on rappi_db.
         
        Parameters
        -----------------------------------------------
         
        df ->  str file name
        table -> str Table name
         
        Returns
        ----------------------------------------------
         
        Confirm Message.
         
        """
        conn = rappi_db.connect()
        data = pd.read_csv(df + '.csv',index_col=False)
        
        try:
            data.to_sql(name=table, con=conn)
        except:
            pass
        
        logger.info('Table uploaded successfully')
    
    @staticmethod
    def get_query_table(query):
        
        """Retrieve table from a specific query
        
        Parameters
        -------------------------------------------------
        
        query -> personalized query (str)
        
        WARNING: Be sure to always specified table schema
        
        Returns
        --------------------------------------------------
        
        pd.DataFrame object
        
        """
        df = pd.read_sql(query,rappi_db.connect())
                            
        return df
    
    
    @staticmethod
    def show_tables(schema = 'rappi_data'):
        """ Show tables available in specified schema connection
         
        Parameters
        -----------------------------------------------
         
        schema -> name of the database (str) default schema: rappi_data
         
        Returns
        ----------------------------------------------
         
        pd.DataFrame object
         
        """
        conn = rappi_db.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                            
        return cursor.fetchall()
     